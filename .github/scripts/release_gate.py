#!/usr/bin/env python
"""Refuse to release a commit whose `ci.yml` run did not finish green.

`release.yml` does not run the test suite. It never has, on purpose: the
tag is meant to point at a commit `ci.yml` already proved, and paying for
the suite twice per tag is how cutting a release becomes something people
avoid. The header said "the tag is expected to point at a commit whose
ci.yml run was green", and nothing checked. Then 0.4.0 was published six
minutes before its own ci run finished, on a commit whose parent had been
cancelled mid-run and never got a verdict at all (timelakedb#168). Tributary's release.yml
had the same header and the same hole; this file is a copy of the one
there, and the two are meant to stay the same file.

So this asks. For the tagged commit, the newest `ci.yml` run:

    completed + success     release
    anything still running  refuse, print the run, say "re-run me later"
    cancelled / failure     refuse, print the run

A commit with no run at all is either untested or docs-only. `ci.yml`
does not run for a change that touches only markdown or LICENSE, and its
header says why that is the intended trade: the last green run still
describes the last commit that could have changed behaviour. This gate
encodes the same rule rather than contradicting it. A docs-only commit
defers to its parent, a few times over if it has to, and a commit that
touches code and has no run is refused as never tested.

Nothing here needs the tag to move. A refused tag is re-run from the
Actions page once ci is green; the event is preserved, so a re-run still
publishes.

    python .github/scripts/release_gate.py            # GITHUB_SHA, GITHUB_REPOSITORY
    python .github/scripts/release_gate.py --sha v0.4.0 --repo timelakelabs/timelakedb

The token comes from GH_TOKEN or GITHUB_TOKEN. Reading runs on a public
repository needs none, but the rate limit without one is 60 an hour.
"""

import argparse
import fnmatch
import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request

WORKFLOW = "ci.yml"

# What ci.yml does not run for. A test checks this against the workflow's
# own `paths-ignore`, so a change to one without the other fails loudly
# instead of letting the gate quietly disagree with the thing it gates on.
CI_IGNORED = ("**.md", "LICENSE")

# How many docs-only commits to walk back through before giving up. Four
# in the last sixty on main; anyone tagging behind twenty in a row has a
# different problem.
MAX_WALK = 20


def ignored(path):
    """Would ci.yml skip a push that touched only this file?

    GitHub's filter patterns are close enough to fnmatch for the shapes in
    use: `**.md` is any markdown at any depth, `LICENSE` is the root file
    and only that. fnmatch's `*` crosses `/`, which is what `**` means here.
    """
    return any(fnmatch.fnmatch(path, pat.replace("**", "*")) for pat in CI_IGNORED)


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def docs_only(sha):
    """True if `sha` changes nothing ci.yml would have run for.

    Compared against the first parent, which for a merge is what the merge
    brought in. An empty commit changes nothing either.
    """
    files = git("diff", "--name-only", f"{sha}^", sha).split("\n")
    return all(ignored(f) for f in files if f)


def parent_of(sha):
    return git("rev-parse", "--verify", f"{sha}^")


def fetch_runs(repo, sha, token):
    """Every `ci.yml` run whose head is exactly `sha`."""
    q = urllib.parse.urlencode({"head_sha": sha, "per_page": 100})
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{WORKFLOW}/runs?{q}"
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30) as r:
        return json.load(r)["workflow_runs"]


def newest(runs):
    """A re-run keeps its run number and reports its latest attempt, so the
    highest run number is the most recent word on this commit."""
    return max(runs, key=lambda r: (r.get("run_number", 0), r.get("id", 0)))


def decide(sha, runs_for, docs_only_at, parent_of_, max_walk=MAX_WALK):
    """(ok, lines). Pure: the three callables are the only I/O.

    `runs_for(sha)` -> list of workflow runs, `docs_only_at(sha)` -> bool,
    `parent_of_(sha)` -> sha.
    """
    lines = []
    cur = sha
    for _ in range(max_walk + 1):
        runs = runs_for(cur)
        if runs:
            run = newest(runs)
            head = (f"{WORKFLOW} run #{run['run_number']} for {cur[:7]}"
                    f" ({run['event']} on {run['head_branch']}): ")
            if run["status"] != "completed":
                return False, lines + [
                    head + f"{run['status']}, no verdict yet",
                    run["html_url"],
                    "Wait for it to finish green, then re-run this workflow from the"
                    " Actions page. The tag does not need to move.",
                ]
            if run["conclusion"] != "success":
                return False, lines + [
                    head + str(run["conclusion"]),
                    run["html_url"],
                    "Not releasing an unproven tree. Get that run green (a re-run, if it"
                    " was cancelled), then re-run this workflow. The tag does not need"
                    " to move.",
                ]
            return True, lines + [head + "success", run["html_url"]]
        if not docs_only_at(cur):
            return False, lines + [
                f"{cur[:7]} has no {WORKFLOW} run and touches more than markdown:"
                " it was never tested.",
                f"{WORKFLOW} runs on pushes to main and on pull requests. Tag a commit"
                " that was one of those, or wait a moment if it was pushed seconds ago.",
            ]
        lines.append(f"{cur[:7]} has no {WORKFLOW} run and touches only files it"
                     " ignores; the verdict is its parent's")
        cur = parent_of_(cur)
    return False, lines + [
        f"gave up after {max_walk} docs-only commits without finding a {WORKFLOW} run",
    ]


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--sha", default=os.environ.get("GITHUB_SHA"),
                    help="commit or tag to release (default: $GITHUB_SHA)")
    ap.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"),
                    help="owner/name (default: $GITHUB_REPOSITORY)")
    args = ap.parse_args()
    if not args.sha or not args.repo:
        sys.exit("need --sha and --repo, or GITHUB_SHA and GITHUB_REPOSITORY")
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")

    # An annotated tag is its own object; the run belongs to the commit.
    sha = git("rev-parse", "--verify", f"{args.sha}^{{commit}}")

    ok, lines = decide(sha, lambda s: fetch_runs(args.repo, s, token), docs_only, parent_of)
    verdict = "passed" if ok else "REFUSED"
    print(f"gate: {verdict}")
    for line in lines:
        print("  " + line)

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(f"### gate: {verdict}\n\n")
            f.write("".join(f"{line}  \n" for line in lines))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
