# CLAUDE.md — Claude Code entry point

This file is intentionally thin. The canonical repository engineering contract is [AGENTS.md](AGENTS.md). Claude Code must read and follow it before acting.

This complete `/Users/beckliu/Documents/0agentproject2026/boazapp/` project owns the dedicated mobile health collector and Tokyo relay: `boazapp.xcodeproj`, Swift `boazapp/`, Rust `Server/`, `scripts/` and `Tests/`. Never treat the Swift child as its root. The Desktop SSOT is `/Users/beckliu/Documents/0agentproject2026/TomeofSouls/`; entity-governance principles derive from its [Boaz project boundaries](https://github.com/qi9zg202-source/TomeofSouls/blob/main/docs/BOAZ_PROJECT_BOUNDARIES.md), while health schemas and native Swift responsibilities remain owned here. SQLite authorities remain separate and VictoriaMetrics is derived. This adds no desktop health synchronization; follow the detailed relationship in `AGENTS.md`.

Read in this order:

1. [AGENTS.md](AGENTS.md) — authority, safety, scope, verification and release rules.
2. [design.md](design.md) — current architecture and evidence boundaries.
3. [memory.md](memory.md) — dated project decisions and open gates, not personal or health data.
4. [docs/prd.html](docs/prd.html) — the sole product requirements and acceptance contract.
5. The current source, schemas, tests and dated evidence relevant to the task.

Claude-specific reminders:

- The current worktree and running-system evidence outrank every document.
- Preserve unrelated work and inspect overlapping diffs before editing.
- Do not deploy, enable real upload, modify Tokyo access, erase real data, commit or push without separate explicit authorization.
- Use only synthetic health records for development and automated verification.
- A local build, simulator run, synthetic receiver check or dated preflight is not physical iPhone/Watch or Tokyo production acceptance.
- End substantive reviews with `AI-NATIVE GATE: PASS` or `AI-NATIVE GATE: INTERCEPT`, scoped to what was actually verified.

Do not duplicate the rules from `AGENTS.md` here. If the two files appear to conflict, `AGENTS.md` is authoritative and this entry point must be corrected.
