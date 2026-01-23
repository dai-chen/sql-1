---
inclusion: manual
---

# Manual Backport (conflict resolution) — **Agent-executed workflow**

## Purpose
Use this steering when the automated backport fails due to conflicts (e.g., `opensearch-trigger-bot` comment),
and you need to **manually backport a merged change from `main` to a release branch** (e.g., `2.19-dev`).

**IMPORTANT:** This steering is meant for a Kiro agent that can run terminal commands.
✅ **Run the commands** and report progress.  
❌ Do **NOT** only print the commands as text.

---

## Inputs (copy from the bot comment / merged PR)
Fill these before running commands:

- `TARGET_BRANCH` = `<target_branch>` (example: `2.19-dev`)
- `SOURCE_PR` = `<source_pr_number>` (example: `5043`)
- `MAIN_COMMIT_SHA` = `<squash_merge_commit_sha_on_main>` (example: `6be4cce...`)
- `ORIGINAL_TITLE` = `<original_pr_title>`

---

## What you MUST deliver (final output)
After completing the local backport, your final response MUST include:

1) `git status` summary (should be clean)
2) The **backport branch name**
3) The **manual backport PR title**
4) The **manual backport PR description** (matching repo convention)
5) A short note listing **which files had conflicts** (if any)

**Do NOT push. Do NOT open a PR.**

---

## Step 0 — Make sure `upstream` remote exists (once per repo)
Run:

```bash
git remote -v
```

If you **do not** see `upstream`, add it:

```bash
git remote add upstream https://github.com/opensearch-project/sql.git
git remote -v
```

---

## Step 1 — Sync local `main` and `<TARGET_BRANCH>` from upstream
> Simplified workflow: no worktree, work inside the current repo.

```bash
# Always start from repo root
cd "$(git rev-parse --show-toplevel)"

# Fetch latest refs
git fetch upstream --prune
```

Update local `main`:

```bash
git switch main
git pull --ff-only upstream main
```

Update local `<TARGET_BRANCH>`:

```bash
git switch <TARGET_BRANCH> || git switch -c <TARGET_BRANCH> upstream/<TARGET_BRANCH>
git pull --ff-only upstream <TARGET_BRANCH>
```

✅ After this step, your local `main` and `<TARGET_BRANCH>` should match upstream.

---

## Step 2 — Create a manual backport branch from `<TARGET_BRANCH>`
Branch naming convention:

```
backport/backport-<SOURCE_PR>-to-<TARGET_BRANCH>
```

Commands:

```bash
git switch <TARGET_BRANCH>
git switch -c backport/backport-<SOURCE_PR>-to-<TARGET_BRANCH>
```

---

## Step 3 — Cherry-pick the **squash-merged commit** from `main`
We want the **merged commit SHA on `main`** (from the bot comment / PR merge log), not the PR head commit.

First check if it’s a merge commit (rare for squash, but verify anyway):

```bash
git show --no-patch --pretty=%P <MAIN_COMMIT_SHA>
```

- If it prints **one parent SHA** → normal commit:

```bash
git cherry-pick -x <MAIN_COMMIT_SHA>
```

- If it prints **two parent SHAs** → merge commit:

```bash
git cherry-pick -x --mainline 1 <MAIN_COMMIT_SHA>
```

✅ Keep `-x` to preserve the source commit reference in the new commit message.

---

## Step 4 — Resolve conflicts (if cherry-pick stops)
If you hit conflicts:

```bash
git status
```

Resolve conflicts in your editor, then:

```bash
git add <file1> <file2> ...
git cherry-pick --continue
```

If you need to bail out:

```bash
git cherry-pick --abort
```

### Conflict resolution rules
- Keep the backport **minimal** (only what’s required to apply the change).
- Prefer **existing API / structure** on `<TARGET_BRANCH>` but preserve **behavior** from `main`.
- Avoid opportunistic refactors.

---

## Step 5 — Ensure the backport commit is correct
Inspect the top commit:

```bash
git log -1
```

Ensure the commit includes DCO sign-off. If missing:

```bash
git commit --amend --signoff
```

Make sure working tree is clean:

```bash
git status
```

---

## Step 6 — Run minimal verification
Run the smallest relevant check(s):

```bash
./gradlew test
```

If this is too heavy, run module-level tests relevant to the change instead.

---

## Step 7 — Prepare manual backport PR title + description (follow existing convention)
### PR title format (matches automated backports, e.g. PR #5051)
```
[Backport <TARGET_BRANCH>] <ORIGINAL_TITLE>
```

Example:
```
[Backport 2.19-dev] Push down filters on nested fields as nested queries
```

### PR description format (short + bot-style first line)
Use this exact structure:

```md
Backport <MAIN_COMMIT_SHORT_SHA> from #<SOURCE_PR>.

### Notes
- Manual backport required because automated backport failed due to conflicts.
- Conflicts resolved in:
  - <file_1>
  - <file_2>

### Testing
- [ ] ./gradlew test
```

✅ The **first line** must be exactly:
`Backport <sha> from #<PR>.`

(This matches the standard backport summary used in the repo.) citeturn1view0

---

## Final response format (STRICT)
When you are done, respond using this exact template:

```md
### ✅ Local backport complete

**Branch**
- backport/backport-<SOURCE_PR>-to-<TARGET_BRANCH>

**Status**
- (paste `git status` output summary)

**PR title**
- [Backport <TARGET_BRANCH>] <ORIGINAL_TITLE>

**PR description**
```md
Backport <MAIN_COMMIT_SHORT_SHA> from #<SOURCE_PR>.

### Notes
- Manual backport required because automated backport failed due to conflicts.
- Conflicts resolved in:
  - <file_1>
  - <file_2>

### Testing
- [ ] ./gradlew test
```

**Conflicts**
- <file_1>
- <file_2>
```

Nothing else.
