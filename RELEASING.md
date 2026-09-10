# Releasing Tributary

The list, because every cut so far dropped a different step and never the
same one, so none of them got learned. TimeLakeDB's 0.4.0 shipped no
container image and a Helm chart advertising `appVersion: "main"`
(timelakedb#166); this repository had the same shape and the same gap.
Most of it is automated now, and the point of the document is to say which
parts are, so nobody does them by hand.

## What a tag does on its own

Pushing `vX.Y.Z` starts `.github/workflows/release.yml`, which:

1. **Refuses** unless `ci.yml` has already finished green on that exact
   commit (timelakedb#168). Not "was expected to be green" — it looks.
2. Builds the `.deb` and `.rpm` in containers, installs and smoke-tests
   them on four distros, and attaches them with `SHA256SUMS`.
3. Publishes `ghcr.io/timelakelabs/tributary:X.Y.Z` for `linux/amd64` and
   `linux/arm64`, plus `:latest` for a non-prerelease tag, then inspects
   the manifest list and fails if either platform is missing (#77).
4. Attaches `deploy/k8s/daemonset.yaml` **stamped with that tag**, written
   only after the image job succeeds so it can never name an image nobody
   pushed. That attached copy is the one an operator applies.

No image step and no artifact upload is yours to do.

## Before you tag

**The DaemonSet manifest's image tag IS yours**, and this is the one place
this document differs from TimeLakeDB's. Its chart deliberately tracks
`main` in the tree and is stamped only in the artifact; here the manifest
in the tree pins a real version, because a floating tag meant every node
that rebooted pulled a different agent than its neighbours (#77). A cargo
test holds the pin equal to the crate version, so a release commit that
bumps `Cargo.toml` and forgets the manifest fails CI rather than shipping
a fleet-wide drift:

```
crates/tributary/tests/manifest_pins_the_crate_version.rs
```

The release commit touches exactly five things:

| File | Change |
|---|---|
| `CHANGELOG.md` | promote `## [Unreleased]` to `## [X.Y.Z] — YYYY-MM-DD` and open a fresh `[Unreleased]` above it |
| `Cargo.toml` | the workspace `version` |
| `Cargo.lock` | follows the workspace version — regenerate, do not hand-edit |
| `README.md` | the `VER=` line in the install snippet |
| `deploy/k8s/daemonset.yaml` | the `image:` tag, to the same version |

No local Rust toolchain on the usual machine, so the lock file comes from
a container:

```sh
docker run --rm -v "$PWD:/w" -w /w rust:1-slim cargo update -w
```

Between releases the in-tree manifest names an image that does not exist
yet. That is intended, and `deploy/k8s/README.md` says so: apply the copy
attached to the Release, or override the one line the way
`deploy/k8s/kind-smoke.sh` does.

## The order, and why it is this order

**`main` refuses a direct push** (timelakedb#169: required checks, no
bypass) and the release workflow **refuses a tag whose CI is not green**
(timelakedb#168). Those two together fix the order:

1. Open a pull request with the release commit. `changes` will say
   `code=true` because `Cargo.toml` moved, so the full suite runs —
   including the manifest-pin test.
2. Merge it.
3. **Wait for `ci.yml` to finish green on the merge commit on `main`.**
   Tag before that and the gate refuses the tag; that is the gate working.
4. Tag the merge commit and push:

```sh
git checkout main && git pull --ff-only
git tag -a vX.Y.Z -m "Tributary X.Y.Z — <what a reader would want to know>. .deb + .rpm attached."
../ops/git-push-ssh.sh --tags
```

Tags are signed (`tag.gpgsign` is on) and annotated.

If the gate refuses, nothing is lost and the tag does not move: fix what
is red on `main`, then **re-run the release workflow from the Actions
page**. A re-run keeps the original event, so it still publishes.

## After the tag

- `gh release view vX.Y.Z --json assets` — the `.deb`, the `.rpm`,
  `SHA256SUMS`, and `daemonset.yaml`.
- `docker buildx imagetools inspect ghcr.io/timelakelabs/tributary:X.Y.Z`
  — both platforms.
- The attached `daemonset.yaml` names `:X.Y.Z`, not `:latest` and not the
  previous version.

## The cross-repo half, which is where steps go missing

None of this is in a workflow, and all of it has been forgotten at least
once:

- **Close the milestone, open the next.**
- **Add the row to the umbrella's `ROADMAP.md` §4.** Enforced from the
  other side: the umbrella's `ops/status.py` fails if a released version
  has no row (umbrella#9).
- **Move the board** — org Project #2, shipped items to Done and the
  `Release` field set.
- **Re-read `README.md`'s status paragraph.** It is the document most
  likely to be quietly wrong after a release; it sat at "phases L0–L4"
  for eleven days after L5's DaemonSet shipped (#86).

## What not to do

- **Do not make `release.yml` re-run the test suite.** Its header explains
  the cost, and that cost is why cutting a release stops being something
  people avoid. The gate reads `ci.yml`'s verdict instead.
- **Do not point the manifest back at a floating tag** to make it
  applyable from `main` between releases. That is #77, and the fleet-drift
  it caused is why the pin and its test exist.
- **Do not tag a commit that is not on `main`.**
