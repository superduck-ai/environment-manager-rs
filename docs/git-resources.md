# Managed Agents Git resources

Only `startup_context.origin = "managed_agents_api"` uses the resource clone path.
Existing source formats and session modes keep their previous behavior. Legacy
sources must omit `mount_path` and `checkout`; both startup validation and the
legacy Git mutation entrypoints reject these resource-only fields.

```json
{
  "origin": "managed_agents_api",
  "api_base_url": "https://oma.example.test",
  "sources": [{
    "type": "git_repository",
    "git_info": {
      "type": "github",
      "host": "github",
      "repo": "owner/repository",
      "url": "https://github.com/owner/repository"
    },
    "mount_path": "/workspace/repository",
    "checkout": {"type": "branch", "name": "main"}
  }]
}
```

`checkout` may be omitted/null for the default branch, a branch with `name`, or
a commit with `sha`. This path does not accept `git_info.auth` or `git_info.ref`.
The real upstream credential remains in OMA; the existing session ingress token
authenticates the CONNECT relay and is never a repository URL/config value.

The Manager prepares the CA bundle and starts the existing CONNECT-over-WebSocket
relay before environment/source initialization. Source Git commands and Claude
share this relay. Both `NO_PROXY` and `no_proxy` retain only local/private-network
and control-plane exceptions, removing Anthropic, GitHub, and package registry
bypasses. Clients honoring the proxy environment use the relay; network policy
and credential checks still apply. `GIT_SSL_CAINFO` points to the combined CA
bundle. Source Git commands additionally clear both bypass variables. Clone
failure fails environment startup.

Branches use `clone --depth=1 --single-branch --no-tags` and must produce the exact
`refs/heads/<name>` symbolic HEAD; a same-named tag cannot satisfy branch checkout.
Commits use an explicit
depth-one fetch, verify the fetched commit ID, then detached checkout. No failure
falls back to complete history. A short commit prefix is accepted only when it
uniquely identifies an advertised object. For an unadvertised ancestor, pass its
full SHA. Git LFS and recursive submodules are not prepared by this resource path.

Mounts must be normalized, non-overlapping directories below `/workspace`, with
no `.git`, `.claude`, or `.oma` path segment. Descriptor-relative directory traversal
rejects symlinks, and clone runs in a temporary directory below the pinned parent.
Installation uses `renameat2(RENAME_NOREPLACE)`. Failed/cancelled clone removes the
temporary directory and never replaces an existing target. On recovery, a real
repository with the same origin is reused without fetch/reset/checkout, preserving
local branches, uncommitted edits, and untracked files.

Validation includes real local Git repositories for branch/default/commit shallow
clone, cleanup, cancellation, path escape, and recovery, plus source schema and
shared proxy environment tests:

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

Run these on Linux GNU with Rust 1.97. A source change enters Sandbox only after
the Rust artifact is built, its digest/SHA pins are updated in
`managed-agent-sandbox`, and the rebuilt Sandbox passes a real private-repository
Session/Deployment test.
