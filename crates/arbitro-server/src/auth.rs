//! Connection authentication — cold path only.
//!
//! A connection authenticates **once**, during the handshake, right after the
//! v2 HELLO frame. Nothing on the delivery path ever re-checks: the TCP stream
//! is the security boundary, so once the peer is established every subsequent
//! frame on that socket is from the same peer. Re-comparing a token per frame
//! would cost a compare on the hot path and buy nothing — anyone able to
//! inject frames into the stream can replay the token too.
//!
//! # Adding a layer
//!
//! [`Authenticator`] is an enum, not a trait, and that is deliberate: there is
//! exactly one call site (the handshake in `server.rs`) and every mechanism is
//! known at compile time. A new layer — mTLS common-name, JWT, an external
//! auth service — is a new variant plus a new match arm in
//! [`Authenticator::authenticate`]. Nothing else moves. A trait would buy
//! `dyn` dispatch nobody needs, and would fight back the day a variant wants
//! to be `async`.
//!
//! To carry new evidence, add a field to [`Credentials`]; to carry new
//! identity, add one to [`Identity`]. Both are additive.

use arbitro_proto::error::ErrorCode;

/// A permission grantable to an authenticated connection.
///
/// Scaffold: no dispatch call site enforces these yet. When one does, the
/// choke point is the `match action` in `transport::dispatch_v2`, which
/// already receives `conn_id` and the connection registry — so authorization
/// needs no new plumbing, only a lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Publish,
    Consume,
    Admin,
}

/// One configured user, parsed from `ARBITRO_AUTH_USERS`.
///
/// This is a **config record** and holds the bearer token. It is never
/// attached to a connection — see [`Identity`].
#[derive(Debug, Clone)]
pub struct AuthUser {
    pub username: String,
    pub token: String,
    pub permissions: Vec<Permission>,
}

/// What a connection presents at handshake.
///
/// One field per mechanism. `token` is `None` when the client sent no `Auth`
/// frame at all, which is distinct from sending an empty one.
#[derive(Debug, Default)]
pub struct Credentials<'a> {
    pub token: Option<&'a [u8]>,
}

/// Who the connection *is*, once authenticated.
///
/// Deliberately carries **no secret**. The token that proved this identity
/// stays in the [`Authenticator`]; copying it onto every connection would put
/// a live credential in per-connection state for no reason, and a future
/// mTLS or JWT identity has no static token to put there anyway.
#[derive(Debug, Clone, Default)]
pub struct Identity {
    /// `None` in shared-token mode — authenticated, but anonymous.
    pub username: Option<String>,
    pub permissions: Vec<Permission>,
}

impl Identity {
    /// `Admin` implies every other permission.
    ///
    /// An identity with no permissions at all is the shared-token case:
    /// authentication succeeded and no per-user policy exists, so nothing is
    /// denied. Returning `false` there would lock out every shared-token
    /// deployment the moment the first authorization check lands.
    pub fn has_permission(&self, perm: Permission) -> bool {
        self.permissions.is_empty()
            || self.permissions.contains(&perm)
            || self.permissions.contains(&Permission::Admin)
    }
}

/// The verdict. `Deny` carries the code the client is owed — `AuthRequired`
/// when nothing was presented, `AuthFailed` when what was presented is wrong.
/// The distinction matters to clients: one means "send a token", the other
/// means "stop retrying, this token will never work".
pub enum AuthOutcome {
    Allow(Identity),
    Deny(ErrorCode),
}

/// How this broker authenticates connections. Selected once at startup.
#[derive(Debug, Clone)]
pub enum Authenticator {
    /// No credentials required. The handshake skips the `Auth` frame entirely.
    Disabled,
    /// Shared bearer token, per-user bearer tokens, or both.
    Token {
        shared: Option<String>,
        users: Vec<AuthUser>,
    },
}

impl Authenticator {
    /// Build from the configured shared token plus `ARBITRO_AUTH_USERS`.
    /// With neither set, authentication is off and behavior is unchanged.
    pub fn from_config(shared: Option<String>) -> Self {
        let users = parse_users_env();
        if shared.is_none() && users.is_empty() {
            return Self::Disabled;
        }
        Self::Token { shared, users }
    }

    /// Whether the handshake must wait for an `Auth` frame before dispatching.
    pub fn requires_credentials(&self) -> bool {
        !matches!(self, Self::Disabled)
    }

    /// Number of configured per-user identities — for startup logging.
    pub fn user_count(&self) -> usize {
        match self {
            Self::Disabled => 0,
            Self::Token { users, .. } => users.len(),
        }
    }

    /// Resolve credentials to an identity. The whole security decision lives
    /// here; a new layer is a new arm.
    pub fn authenticate(&self, creds: &Credentials<'_>) -> AuthOutcome {
        match self {
            Self::Disabled => AuthOutcome::Allow(Identity::default()),

            Self::Token { shared, users } => {
                let Some(presented) = creds.token else {
                    return AuthOutcome::Deny(ErrorCode::AuthRequired);
                };

                if shared
                    .as_deref()
                    .is_some_and(|expected| constant_time_eq(presented, expected.as_bytes()))
                {
                    // Shared-token mode carries no per-user policy.
                    return AuthOutcome::Allow(Identity::default());
                }

                // Scan every user even after a match: returning early would
                // leak, through timing, how far down the list a token sits.
                let mut matched: Option<&AuthUser> = None;
                for user in users {
                    if constant_time_eq(presented, user.token.as_bytes()) {
                        matched = Some(user);
                    }
                }

                match matched {
                    Some(user) => AuthOutcome::Allow(Identity {
                        username: Some(user.username.clone()),
                        permissions: user.permissions.clone(),
                    }),
                    None => AuthOutcome::Deny(ErrorCode::AuthFailed),
                }
            }
        }
    }
}

/// Compare two secrets without an early exit.
///
/// A plain `==` on byte slices stops at the first differing byte, which turns
/// response latency into an oracle: an attacker recovers the token one byte at
/// a time. The length check is fine to short-circuit — a single failed attempt
/// already reveals the expected length.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Parse `ARBITRO_AUTH_USERS`: `user1:token1:publish,consume;user2:token2:admin`.
///
/// An entry without both colons is dropped. That silence is dangerous on a
/// *security* setting — `ARBITRO_AUTH_USERS="alicetok"` (missing colons) would
/// otherwise boot a broker with authentication quietly off — so a set-but-
/// unusable value screams instead.
fn parse_users_env() -> Vec<AuthUser> {
    let raw = match std::env::var("ARBITRO_AUTH_USERS") {
        Ok(v) if !v.is_empty() => v,
        _ => return Vec::new(),
    };
    let users = parse_users(&raw);
    if users.is_empty() {
        tracing::error!(
            "ARBITRO_AUTH_USERS is set but no entry parsed — expected \
             `user:token:perms;...`. Authentication is NOT enabled by it."
        );
    }
    users
}

fn parse_users(raw: &str) -> Vec<AuthUser> {
    raw.split(';')
        .filter_map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return None;
            }
            let mut parts = entry.splitn(3, ':');
            let username = parts.next()?.to_string();
            let token = parts.next()?.to_string();
            let permissions = parts
                .next()
                .unwrap_or("")
                .split(',')
                .filter_map(|p| match p.trim() {
                    "publish" => Some(Permission::Publish),
                    "consume" => Some(Permission::Consume),
                    "admin" => Some(Permission::Admin),
                    "" => None,
                    other => {
                        tracing::warn!(
                            permission = other,
                            "ARBITRO_AUTH_USERS: unknown permission, skipping"
                        );
                        None
                    }
                })
                .collect();
            Some(AuthUser {
                username,
                token,
                permissions,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn users() -> Vec<AuthUser> {
        vec![
            AuthUser {
                username: "alice".into(),
                token: "alice-tok".into(),
                permissions: vec![Permission::Publish],
            },
            AuthUser {
                username: "root".into(),
                token: "root-tok".into(),
                permissions: vec![Permission::Admin],
            },
        ]
    }

    #[test]
    fn disabled_allows_without_credentials() {
        let a = Authenticator::Disabled;
        assert!(!a.requires_credentials());
        assert!(matches!(
            a.authenticate(&Credentials::default()),
            AuthOutcome::Allow(_)
        ));
    }

    #[test]
    fn missing_token_is_auth_required_not_failed() {
        // The two codes drive different client behavior: retry vs give up.
        let a = Authenticator::Token {
            shared: Some("s3cret".into()),
            users: Vec::new(),
        };
        match a.authenticate(&Credentials::default()) {
            AuthOutcome::Deny(ErrorCode::AuthRequired) => {}
            _ => panic!("missing credentials must deny with AuthRequired"),
        }
    }

    #[test]
    fn shared_token_roundtrip() {
        let a = Authenticator::Token {
            shared: Some("s3cret".into()),
            users: Vec::new(),
        };
        assert!(matches!(
            a.authenticate(&Credentials {
                token: Some(b"s3cret")
            }),
            AuthOutcome::Allow(_)
        ));
        match a.authenticate(&Credentials {
            token: Some(b"wrong"),
        }) {
            AuthOutcome::Deny(ErrorCode::AuthFailed) => {}
            _ => panic!("wrong token must deny with AuthFailed"),
        }
    }

    #[test]
    fn shared_token_rejects_prefix_and_extension() {
        let a = Authenticator::Token {
            shared: Some("s3cret".into()),
            users: Vec::new(),
        };
        for bad in [&b"s3cre"[..], &b"s3cretX"[..], &b""[..]] {
            assert!(
                matches!(
                    a.authenticate(&Credentials { token: Some(bad) }),
                    AuthOutcome::Deny(_)
                ),
                "accepted {bad:?}"
            );
        }
    }

    #[test]
    fn per_user_token_yields_identity_without_the_secret() {
        let a = Authenticator::Token {
            shared: None,
            users: users(),
        };
        match a.authenticate(&Credentials {
            token: Some(b"alice-tok"),
        }) {
            AuthOutcome::Allow(id) => {
                assert_eq!(id.username.as_deref(), Some("alice"));
                assert!(id.has_permission(Permission::Publish));
                assert!(!id.has_permission(Permission::Admin));
            }
            _ => panic!("valid per-user token must be allowed"),
        }
    }

    #[test]
    fn admin_implies_every_permission() {
        let a = Authenticator::Token {
            shared: None,
            users: users(),
        };
        match a.authenticate(&Credentials {
            token: Some(b"root-tok"),
        }) {
            AuthOutcome::Allow(id) => {
                assert!(id.has_permission(Permission::Publish));
                assert!(id.has_permission(Permission::Consume));
            }
            _ => panic!("admin token must be allowed"),
        }
    }

    #[test]
    fn shared_mode_identity_is_not_denied_by_default() {
        // Shared-token identities carry no permission list. If that read as
        // "denied", the first authorization check would lock out every
        // shared-token deployment.
        let id = Identity::default();
        assert!(id.has_permission(Permission::Publish));
        assert!(id.has_permission(Permission::Admin));
    }

    #[test]
    fn shared_and_per_user_coexist() {
        let a = Authenticator::Token {
            shared: Some("shared-tok".into()),
            users: users(),
        };
        assert!(matches!(
            a.authenticate(&Credentials {
                token: Some(b"shared-tok")
            }),
            AuthOutcome::Allow(_)
        ));
        assert!(matches!(
            a.authenticate(&Credentials {
                token: Some(b"root-tok")
            }),
            AuthOutcome::Allow(_)
        ));
    }

    #[test]
    fn constant_time_eq_matches_semantics_of_eq() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }
}
