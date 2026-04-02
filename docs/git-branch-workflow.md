# Git Branch Workflow

## Purpose

This repo now uses a simple two-branch workflow:

- `main`: stable, deployable, release-ready code
- `dev`: daily development branch

Use `dev` for normal work. Only merge into `main` after validation.

## Current Milestone

The current important milestone has been tagged as:

- `milestone-2026-04-02`

This tag points to the repository state before switching to the long-term `dev -> main` workflow.

## Daily Development Flow

Always start development on `dev`:

```bash
cd /root/codex-console
git checkout dev
git pull userrepo dev
```

After making changes:

```bash
git add .
git commit -m "feat: your change"
git push userrepo dev
```

## Release Flow

When `dev` has been tested and is ready to release:

```bash
cd /root/codex-console
git checkout main
git pull userrepo main
git merge --no-ff dev
git push userrepo main
```

Recommended:

- Keep `main` always deployable
- Do not develop directly on `main`
- Use `--no-ff` so each merge back to `main` stays visible in history

## Milestone Tagging

Before a major refactor or risky change, create a tag on the current stable state:

```bash
git checkout main
git pull userrepo main
git tag -a milestone-YYYY-MM-DD -m "milestone note"
git push userrepo milestone-YYYY-MM-DD
```

## Rollback

If a release on `main` is bad, first locate the last good point:

```bash
git log --oneline --decorate --graph -n 30
git tag --list "milestone-*"
```

Then either:

- revert the bad merge commit, or
- redeploy a known-good tagged commit

Avoid `git reset --hard` on shared branches unless you fully understand the impact.

## Remote Setup

Current remotes:

- `userrepo`: your GitHub repo
- `upstream`: original upstream repo

Typical sync pattern:

```bash
git checkout main
git fetch upstream
git fetch userrepo
```

If you want to absorb upstream updates later, do that deliberately on `dev` first, not directly on `main`.

## GitHub Authentication

This server currently has no working GitHub HTTPS credentials, so `git push` fails.

The simplest setup is a GitHub Personal Access Token with repo permission.

### Option 1: HTTPS + PAT

1. Create a GitHub Personal Access Token.
2. Recommended minimum scope:
   - Fine-grained token: access only the target repo, with `Contents: Read and write`
   - Classic token: `repo`
3. Configure credential storage on this machine:

```bash
git config --global credential.helper store
```

4. On the first push, Git will ask:
   - Username: your GitHub username
   - Password: paste the PAT, not your GitHub login password

5. Git will save the credential to:

```bash
/root/.git-credentials
```

After that, pushes should work normally.

### Option 2: Preload Credential Non-Interactively

If you want to write the credential once:

```bash
printf 'https://YOUR_GITHUB_USERNAME:YOUR_GITHUB_PAT@github.com\n' > /root/.git-credentials
chmod 600 /root/.git-credentials
git config --global credential.helper store
```

Replace:

- `YOUR_GITHUB_USERNAME`
- `YOUR_GITHUB_PAT`

### Option 3: SSH

If you prefer SSH, switch the remote URL and configure a deploy key or personal SSH key:

```bash
git remote set-url userrepo git@github.com:yangzcy/codex-console.git
```

Then ensure the machine key is added to your GitHub account.

## Recommended Branch Protection

On GitHub, protect `main`:

- disable direct pushes
- require PR or controlled merge process
- keep `dev` as the branch for ongoing work

For a small private workflow, even without PRs, protecting `main` is still useful.
