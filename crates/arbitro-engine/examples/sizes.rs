//! What a MatchEntry actually costs, field by field.
use arbitro_engine::catalog::match_table::MatchEntry;
use arbitro_engine::types::*;

fn main() {
    println!("MatchEntry        {:>3} B (align {})",
        std::mem::size_of::<MatchEntry>(), std::mem::align_of::<MatchEntry>());
    println!();
    let f: [(&str, usize); 5] = [
        ("consumer_id",     std::mem::size_of::<ConsumerId>()),
        ("queue_id",        std::mem::size_of::<QueueId>()),
        ("subscription_id", std::mem::size_of::<SubscriptionId>()),
        ("connection_id",   std::mem::size_of::<ConnectionId>()),
        ("binding_idx",     std::mem::size_of::<u32>()),
    ];
    let sum: usize = f.iter().map(|(_, s)| s).sum();
    for (n, s) in f { println!("  {n:<16} {s:>2} B"); }
    println!("  {:<16} {:>2} B  (suma de campos)", "", sum);
    println!("  {:<16} {:>2} B  (relleno por alineación)", "",
        std::mem::size_of::<MatchEntry>() - sum);
    println!();
    let e = std::mem::size_of::<MatchEntry>();
    for n in [8usize, 240, 1000] {
        println!("{n:>5} entradas = {:>7} B = {:>5.1} lineas de cache",
            n * e, (n * e) as f64 / 64.0);
    }
}
