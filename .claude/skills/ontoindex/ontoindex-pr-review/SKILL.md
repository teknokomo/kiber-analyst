---
name: ontoindex-pr-review
description: 'Review a pull request or diff for correctness, merge risk, and missing test coverage.'
---

Read ontoindex-navigation first (freshness labels and query failure guidance).

## Establish the review scope

For a local change that has no GitHub PR yet, select the comparison matching
the work in progress:

- `git diff` for unstaged changes;
- `git diff --cached` for staged changes;
- `git diff <base>...HEAD` for a branch compared with its merge base.

Use that same base and changed-file list for OntoIndex review and test-gap
checks. Do not invent a PR number or claim remote CI evidence for a local diff.

Run `gh auth status` first, then `gh pr diff <pr> --repo <owner/repository>`.
Resolve the PR's base and head commits and ensure the local comparison contains those commits.
Use that exact range for graph-assisted review:

```text
audit({action: "review_diff", repo: "<repo>", scope: "commit-range", commitRange: "<base>...<head>"})
impact({action: "diff_impact", repo: "<repo>", scope: "commit-range", commitRange: "<base>...<head>"})
audit({action: "test_gap", repo: "<repo>", scope: "compare", diffRef: "<base>", changedFiles: ["<changed-file>"], executedTests: ["<executed-test>"]})
```

Run coverage comparison in the checkout of the reviewed head; supply only tests actually executed.
Read the diff and current source for changed contracts, callers, error paths, and regression evidence.
Graph coverage matches may be heuristic; inspect the tests before treating them as coverage.
Use the tool's risk classification; warn on HIGH or CRITICAL before any follow-up edit.

Report actionable findings with file/line evidence, a concrete trigger, and the consequence.
Separate confirmed defects from evidence gaps; state when no defects were found and what remains unverified.
Use `gh pr checks <pr> --repo <owner/repository>` before claiming CI is green.
