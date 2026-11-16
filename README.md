# `deploy`: the deployment's build branch

This branch is not ZeroClaw. It holds one workflow, `.github/workflows/deploy-artifact.yml`,
which builds an exact ZeroClaw commit for `aarch64-unknown-linux-gnu`, with `channel-slack`, and
publishes it as the release `build-<sha>`. [`kmsquire/zeroclaw-oci-terraform`](https://github.com/kmsquire/zeroclaw-oci-terraform)
installs that archive at boot instead of compiling. Its `docs/zeroclaw-artifact.md` covers the
design, and [Build the ZeroClaw binary out of band and ship an artifact](https://github.com/kmsquire/llm-workspace/issues/26)
has the decision.

To build a commit:

```bash
gh workflow run deploy-artifact.yml --repo kmsquire/zeroclaw --ref deploy -f sha=<40-character sha>
```

- Only this branch can publish. The `release` environment admits `deploy` and nothing else.
- Releases are immutable, and the workflow refuses to rebuild a `build-<sha>` that exists.
- The branch is protected against force-push and deletion, so change it with commits on top,
  through a pull request into `deploy`.
- `master` is a pure fast-forward mirror of upstream, with upstream's workflows disabled on
  this fork.
