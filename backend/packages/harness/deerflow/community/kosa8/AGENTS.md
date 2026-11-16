# kosa8 provider (`deerflow/community/kosa8/`)

General sandbox rules are in the [sandbox guide](../../sandbox/AGENTS.md).

```bash
cd backend && python -m pytest tests -q -k "kosa8"
```

- **The files API reaches `/workspace` only, at most 8 MiB a request.** Keep
  `root_dir` under `/workspace`. Reads elsewhere stream through `cat` over exec;
  writes elsewhere, large writes and appends upload parts to the staging
  directory and assemble them with one `cat`.
- **Each exec argument is at most 8 KiB.** Never splice file content or a long
  script into `cmd`; stage it through the files API. The fake API in the tests
  asserts this limit.
- **A 404 is not always a dead sandbox.** A missing file also answers 404
  `not_found`. Only `Kosa8APIError.sandbox_gone` may evict a sandbox, or a
  missing file would delete the agent's VM.
- **Credential keys keep the `kosa8_` prefix**, so a key configured for another
  provider is never sent to kosa8.
