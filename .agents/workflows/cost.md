---
description: Workflow for reducing token cost and compacting agent communication
arguments: [HIGH, MEDIUM, LOW]
---

# Token Optimization Workflow (COST)

This workflow defines guidelines to minimize token usage during agent interactions while maintaining execution accuracy.

## Optimization Levels

- **HIGH**: Absolute minimum verbosity. No conversational filler. Use technical shorthand. Minimal tool output snippets.
- **MEDIUM (Default)**: Balanced verbosity. Concise explanations. Summary-first tool outputs.
- **LOW**: Standard verbosity. Detailed reasoning. Full tool outputs where context is complex.

## Rules for Compact Execution

1.  **Compact Reasoning**: Avoid multi-paragraph thoughts. Use bullet points or single-sentence logic.
2.  **Tool Output Filtering**: When viewing files or running commands, only show/request the lines relevant to the current instruction.
3.  **Abbreviated Responses**: Summarize results in 1-2 sentences. Avoid repeating the USER's request or providing redundant "next steps" unless necessary.
4.  **Batch Operations**: Combine multiple small edits into a single `multi_replace_file_content` call to reduce round-trips.
5.  **No Placeholders**: Do not include template text or boilerplate in the responses.

## Mode Activation

Activate by specifying the level (e.g., `/cost HIGH`). The agent will immediately switch to the corresponding verbosity profile for the duration of the task.
