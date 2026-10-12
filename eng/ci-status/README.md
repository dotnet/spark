# CI status badges

The [publisher workflow](../../.github/workflows/ci-status.yml) reads public GitHub Checks from the Azure Pipelines app (`9426`) for pipeline `51729` in ManagedOSS. It does not call Azure DevOps or expose build logs. It uses the workflow's built-in `GITHUB_TOKEN`; no PAT, Azure credential, GitHub Pages site, or anonymous Azure badge setting is required.

## Operation

- After this change reaches `main`, a main push, scheduled run, or manual workflow run creates the machine-owned `ci-status` branch. README badge URLs are unavailable until that first successful publication.
- The workflow requests `checks: read` and `contents: write` only for its publishing job. Repository/organization policy must allow those permissions and creation/update of `ci-status`. It does not bypass branch rules. If publication fails, inspect the workflow run before changing any permissions.
- Publication always executes code from a resolved `main` SHA. Pull requests only run read-only tests; forks do not publish. Main is rechecked before updating the status branch, and ref updates are non-forced.
- Main pushes and eligible Azure check-suite completion events request a refresh. A schedule at minutes 7, 22, 37, and 52 provides a fallback because completion events are not guaranteed. GitHub schedules and image caches can delay updates; these are snapshots, not real-time status.
- Only changes to commit/build/check results create commits. Generated data includes source timestamps and the source SHA, not a new wall-clock timestamp on every poll. Consult publisher runs for the last refresh attempt and `ci-status/status.json` for the captured source data.
- `ci-status` contains SVGs, a linked result table, and JSON metadata only. The publisher creates an orphan branch on first use and refuses an existing branch without its ownership marker. It does not write generated files to `main` or delete files from the status branch.

## Result selection

The collector paginates all Azure check suites for the resolved main commit and reads only suites reporting `head_branch=main`. It verifies each run's app, head SHA, and exact `external_id` (`51729|<build ID>|1d09b833-a6f2-4fab-8ec0-b2bf075dcd70`). This reports GitHub Checks associated with main; it is not an independent query of Azure's source-branch metadata.

The greatest numeric build ID selects the build. Within that build, the greatest Check Run ID for each exact job name selects its report: Azure can create new completion records while older queued records remain. A late completion from an older build cannot replace a newer build's results. Missing jobs in a newer build never inherit an older passing result.

Only an explicit `completed/success` report is green. Running, pending, cancelled, skipped, missing, and unknown results remain distinct. API/permission failures fail the refresh without overwriting the last snapshot; compare its revision badge with main and inspect the publisher if it appears stale. There is a small unavoidable window between the final main-ref check and publication; the next main-triggered or scheduled refresh reconciles it.

The expected matrix is read from `azure-pipelines-pr.yml`. Keep the homepage table's rows and image references aligned when changing that matrix; the tests enforce this mapping. Old generated images are harmless and are not deleted automatically.

## Local validation

No npm installation is required. Use Node.js 20 or newer:

```sh
node --test eng/ci-status/index.test.cjs
```

Tests mock the GitHub API, including publication. They never create remote commits or refs. Live workflow execution and branch-rule compatibility must be verified after merge; passing local tests alone does not prove deployment.
