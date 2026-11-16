# kosa8 backend

Runs each DeerFlow sandbox as a [kosa8](https://kosa8.com) Cloud sandbox: a
microVM with its own kernel, booted from an OCI image, that can sleep with its
memory saved and wake in place. The provider calls kosa8's REST API with the
`httpx` client the harness already depends on, so it needs no extra package.

## Configuration

```yaml
sandbox:
  use: deerflow.community.kosa8:Kosa8SandboxProvider
  kosa8_api_key: $KOSA8_API_KEY    # or KOSA8_API_KEY in the Gateway environment
  # kosa8_base_url: https://api.kosa8.com  # or KOSA8_API_URL; for a self-hosted kosa8
  # template: base                 # a kosa8 template; the org default when omitted
  # image: python:3.12             # an OCI image instead of a template (boots cold)
  # size: 2x4                      # vCPU x GiB: 2x4, 4x8, 8x16 or 16x32
  # region: us                     # us, eu, or a region id; nearest when omitted
  # persistent: false              # every exec in one long-lived container
  # sleep_after_s: 300             # kosa8 sleeps an idle sandbox (memory kept); 0 never
  # ttl_s: 86400                   # kosa8 deletes the sandbox after this; 0 means never
  # ready_timeout: 300             # seconds to wait for a cold boot
  # root_dir: /workspace/user-data # directory behind /mnt/user-data; must be under /workspace
  # replicas: 3                    # active + warm sandboxes per Gateway process
  # idle_timeout: 600              # warm seconds before delete; 0 disables
  # environment:                   # set on the sandbox at create, given to every command
  #   PYTHONUNBUFFERED: "1"
```

Create an API key at <https://app.kosa8.com>. The credential keys are named
`kosa8_api_key` and `kosa8_base_url`, not `api_key` and `base_url`, so a key
left in `config.yaml` for another provider is never sent to kosa8.

`environment` values are kept with the sandbox on its kosa8 host and are never
logged or returned by the API. Per-call variables (request-scoped secrets) ride
in each exec's own `env` field, never in the command string.

## Lifecycle

The provider derives a stable local ID from `(user_id, thread_id)` and names the
kosa8 sandbox `deer-flow-<id>`. A released sandbox goes to an in-process warm
pool, keeps running (kosa8 puts it to sleep after `sleep_after_s` idle seconds,
and the next command wakes it), and only the same scope may reclaim it, after a
liveness probe. A sandbox that kosa8 reports gone (`404 no such sandbox` or
`409 not_running`) is evicted, and the next acquire creates a new one.

Create returns after the sandbox is running and its
`/mnt/user-data/{workspace,uploads,outputs}` directories exist; a failure on the
way deletes the new sandbox. `/mnt/user-data` is a link to `root_dir` where the
image lets the sandbox user create it, so shell commands can use the literal
path; the file APIs map it to `root_dir` either way.

Before a user/thread sandbox is parked, the provider copies regular files from
`/mnt/user-data/outputs` to the owner's local thread outputs directory, so
Gateway previews and downloads work after the turn. It follows the OpenSandbox
rules: non-portable names and names that differ only by case or Unicode
normalization are skipped, linked destinations are never written through, each
file is published with an atomic replace, and a pass stops at 2,000 files,
512 MiB, a 512 KiB listing or 120 seconds. A failure keeps the previous complete
copy, and a file deleted in the sandbox stays on the host.

`reset()` parks active sandboxes for this instance's reaper and shutdown.
`shutdown()` fences admission and deletes every sandbox; a delete that fails, or
an acquire still in flight, makes it raise so the caller can retry. A sandbox
whose create finishes after shutdown is deleted, never published. Sandboxes
left by a crashed Gateway are deleted by kosa8 when `ttl_s` runs out.

## Contract coverage

The full `Sandbox` surface is implemented:

- `execute_command` runs `sh -lc` over kosa8's streamed exec, with per-call
  `env` and `timeout` (sent as `timeout_s`). A timeout keeps the partial output
  and ends with `Error: command timed out` and `Exit Code: 124`. A host without
  a streamed timeout answers before the command starts, and the provider falls
  back to a buffered exec, which kosa8 holds to 90 seconds. Commands are never
  retried, because exec is not idempotent.
- `read_file` / `download_file` stream from kosa8's files API under
  `/workspace`, and through `cat` over the exec stream elsewhere or above the
  files API's 8 MiB request limit. The 100 MB download cap is enforced on bytes
  received. Downloads are restricted to `/mnt/user-data`, and every path rejects
  traversal.
- `write_file` / `update_file` upload in one request when the target is under
  `/workspace` and at most 8 MiB. Larger files, appends and other targets are
  uploaded in parts to `/workspace/.deer-flow-staging` and assembled with one
  `cat`, so an append never rewrites the file.
- `list_dir` / `glob` / `grep` run busybox-portable `find` / `grep` and use the
  shared parsers in `deerflow.sandbox`.

Each exec argument is at most 8 KiB, so a longer command is uploaded as a script
and run from the staging directory.

## Not implemented

- Skills under `/mnt/skills` and Agent skill isolation: the provider uploads no
  skill projection (`supports_agent_skill_isolation` stays `False`), as with
  the Tenki and OpenSandbox providers.
- Cross-process discovery: each Gateway process has its own warm pool, and a
  sandbox left by a crashed process is not adopted.
- Command cancellation: an exec runs until it ends or its timeout.

## Status

The unit tests run the real client against an in-memory kosa8 API that executes
commands with a local `sh`. `test_live_kosa8_sandbox` runs against kosa8 Cloud
when `KOSA8_API_KEY` is set:

```bash
cd backend
KOSA8_API_KEY=... DEER_FLOW_RUN_LIVE_TESTS=1 uv run pytest -m live tests/test_kosa8_provider.py -v -s
```
