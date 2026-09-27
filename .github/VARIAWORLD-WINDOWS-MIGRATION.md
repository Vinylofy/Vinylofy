# Variaworld Windows self-hosted runner migration log

This log records the first Vinylofy scraper migration to a persistent Windows GitHub Actions runner. Read it before migrating another scraper, then verify every assumption against that scraper and the target runner.

## Result

- Dedicated workflow: `.github/workflows/usf-variaworld.yml`.
- Runner: `vinylofy-windows-01`, labels `self-hosted`, `Windows`, `X64`, `vinylofy-windows`.
- The dedicated workflow uses `runs-on: [self-hosted, Windows, X64, vinylofy-windows]` and keeps `workflow_dispatch` and its existing schedules/concurrency.
- The original pending job requested `self-hosted`, `linux`, `x64`; no available runner matched those labels. This was a runner-label mismatch, not a concurrency blockage.
- The migration was limited to the dedicated Variaworld workflow. Scraper/data rules and other shop workflows were not migrated.

## Windows runner issues encountered

1. `pwsh` was not installed. Use the Windows PowerShell shell available on the runner (`powershell`), unless PowerShell 7 is explicitly installed and verified.
2. The service account's execution policy blocked generated `.ps1` scripts. Workflow PowerShell steps use process-scoped `-ExecutionPolicy Bypass`; the machine policy was not changed.
3. `actions/setup-python` failed while installing Python under the runner service account. The successful workflow uses pinned `astral-sh/setup-uv` and a Python 3.11 virtual environment.
4. uv's managed Python placement under the runner's `E:` temporary drive failed when it tried to create a Windows junction (`Incorrect function`). Point `UV_PYTHON_INSTALL_DIR` to the service account's `%LOCALAPPDATA%\Vinylofy\variaworld-python`; keep the run-specific virtual environment and uv cache under `$RUNNER_TEMP` and remove them in an `if: always()` cleanup step. This is specific to the observed runner layout; check drive/filesystem behavior on another machine.
5. PowerShell does not use bash syntax. Use PowerShell argument arrays for Python invocations and check `$LASTEXITCODE` after external commands. A bash `fi` left in a workflow step caused a parse failure; use PowerShell block syntax throughout.

The workflow pins uv to `0.12.15` and the setup action to a reviewed commit SHA. Recheck the repository's action-pinning policy before copying these values to another workflow.

## Persistent-runner hygiene

- Check out into a dedicated subdirectory (`variaworld`) so the scraper's working files are easy to identify.
- Disable checkout credential persistence when later steps do not need Git credentials.
- Put ephemeral virtualenvs and caches under `$RUNNER_TEMP`, and remove only those named paths in an unconditional cleanup step. Do not clean the runner account's general directories or anything outside the Actions workspace/temp locations.
- Keep the managed Python installation in the runner service account's local application data so uv can reuse it; it is a runtime installation, not per-run scraper output.
- Inspect the scraper's actual output/download locations before migration. This workflow's temp cleanup does not prove that arbitrary output written elsewhere is removed.
- Check for orphan processes and leftover large files after a run. Successful Actions cleanup reported orphan-process cleanup; independently verify runner availability after future changes.

## Validation and database safety

- Validate runner name in the job before doing work. A green job alone is not proof that it ran on the intended machine.
- First run a small bounded listing dry-run, then a bounded write only when its exact write scope is understood. Do not use a full scheduled scrape as the first Windows test.
- The bounded dry-run ran on `vinylofy-windows-01`, installed Python/dependencies, and processed 128 priced listings across four listing pages with `write=False`.
- A later bounded production run also ran on that runner and did write to the configured database: it reported 128 listing offers, 12 inserts, 116 updates, and 7,480 price rows updated. Its pre-write comparison showed 79 changed rows; 76 differences remained in a read-only post-check with the write transaction timestamp. This discrepancy was reported and not automatically corrected. Treat those numbers as historical evidence, not expected counts for another run.
- The bounded tests skipped detail enrichment. Windows execution of the detail path therefore still requires its own bounded verification before relying on it.
- No earlier successful-run baseline was available for a meaningful catalog-count comparison.

## Reusable checklist for the next scraper

- [ ] Inspect its workflow, imports, wrappers, requirements, env/secrets, data writes, schedules, concurrency, output/download paths, and subprocess usage before editing.
- [ ] Confirm exact runner labels, available shell, Python strategy, service-account environment, and temp/workspace drives.
- [ ] Preserve its existing data rules, database contract, failure behavior, scheduling, and concurrency unless a Windows blocker requires a minimal change.
- [ ] Replace Linux-only shell/path assumptions with native cross-platform Python where practical; use PowerShell only for workflow orchestration on this Windows runner.
- [ ] Make checkout, temp files, caches, output and cleanup behavior explicit without touching user files outside the Actions workspace/temp locations.
- [ ] Keep manual dispatch available and verify the job reports the expected runner name.
- [ ] Run local/configuration checks first; then use a bounded dry-run and verify outputs. Assess and report any write scope before a bounded production write.
- [ ] Verify dependency setup, scraper start, source fetch, database connectivity/writes if applicable, cleanup, and runner availability afterward.
- [ ] Compare counts to a prior successful run only when a reliable baseline exists. Report unexplained differences; do not auto-correct them.

## Scope caveat

The dedicated `usf-variaworld.yml` workflow is the migrated path. At the time of the migration, the broader `vinylofy-automation.yml` workflow also offered a Variaworld selection on a GitHub-hosted runner; it was left unchanged to keep the pilot scoped. Check whether that alternate path is still present before treating the self-hosted workflow as the only way to run Variaworld.
