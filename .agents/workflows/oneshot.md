---
description: High-scale editing with pre-think consensus loop
---

# Oneshot Workflow (ONESHOT)

Philosophy: `Cost(Think) << Cost(Refactor)`

## Execution Formula
`Efficiency = (Thought^n / Σ(Edits)) * Accuracy`
Where `n` represents the number of reasoning cycles required to reach a logical consensus before a single line of code is committed.

## Agent Guidelines
1. **Pre-Compute Consensus**: Your internal reasoning must reach a deterministic and stable state. DO NOT execute edits until the plan is airtight.
2. **Atomic Massive Scaling**: Target high-leverage refactors. Implement sweeping changes in a single operation to maintain architectural integrity. Avoid fragmented "micro-edits".
3. **Strict Context Alignment**: Zero tolerance for API hallucinations. If the codebase state is uncertain, perform a full re-scan before proposing changes.
4. **Logic over Velocity**: It is faster to think for 60 seconds and edit once than to edit for 5 minutes and debug for an hour.

## Operational Loop
1. **Analyze**: Map the dependency graph and identify side effects.
2. **Synthesize**: Construct the optimal solution in cognitive memory.
3. **Atomic Commit**: Apply the change using the most efficient tool path (e.g., `multi_replace_file_content` with unique anchors).
