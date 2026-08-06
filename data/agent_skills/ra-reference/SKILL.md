---
name: ra-reference
description: Reference tables for ResearchAssistant error codes - what each fetch failure, operation blocker and integrity check means, and whether it is worth retrying. Use when a command reports a code you do not recognise, or when you need to explain a failure to the user precisely.
license: MIT
---

# Error code reference

Every code the app can report, with what it means and what to do.

Read [references/codes.md](references/codes.md).

That file is generated from the code, so it cannot drift out of date. If you
meet a code that is not in it, say so to the user rather than guessing — an
unrecognised code means something changed and the tables need regenerating.

In normal use you should not need this: `.agents/bin/ra triage` already groups
failures, explains each one, and gives you the exact command to fix it.
