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

## JPC migration learnings

The JPC migration uses the same single pipeline on two execution routes:

- Normal scheduled execution: `.github/workflows/usf-jpc-windows.yml` on `vinylofy-windows-01`.
- Fallback execution: `.github/workflows/usf-jpc.yml` on `ubuntu-latest`, started with `workflow_dispatch`.

The cloud fallback has no automatic schedule. A failed or unavailable Windows run therefore does not automatically start a second GitHub-hosted run and does not create an unexpected fallback run or associated Actions usage. The fallback can be started manually, and its schedule can be restored deliberately if the Windows machine is unavailable for a longer period.

Both workflows use the concurrency group `usf-jpc-production` with `cancel-in-progress: false`. The validate and run jobs are sequential within one workflow, and the shared group prevents the local and fallback workflows from running the JPC pipeline at the same time. The Windows runner itself executes one job at a time; a queued job means that the runner is busy, offline, or the job is waiting on the concurrency group.

### Windows-specific JPC findings

- The correct runner target is `[self-hosted, Windows, X64, vinylofy-windows]`; the workflow additionally verifies the runner name `vinylofy-windows-01` before doing work.
- `powershell` with process-scoped `ExecutionPolicy Bypass` is required on this runner. `pwsh` was not available.
- Python 3.11 is created through pinned `astral-sh/setup-uv`. The managed Python installation belongs under `%LOCALAPPDATA%\Vinylofy\jpc-python`; the run-specific virtual environment and uv cache belong under `$RUNNER_TEMP` and are removed in an unconditional cleanup step.
- PowerShell external commands require explicit `$LASTEXITCODE` checks. Python arguments are passed as PowerShell arrays so paths and flags do not depend on Bash syntax.
- `DATABASE_URL` is passed from the existing GitHub Secret. No database credential is stored in the workflow or scraper code.

### JPC filter and price-sync findings

The JPC availability controls are custom elements. The working implementation submits the site's POST form field `filter_availability` for the selected facet; a GET query parameter is not equivalent. The filter implementation remains fail-closed when the expected facet cannot be confirmed.

The scheduled Windows listing route is deliberately price-focused. It runs discovery and listing-price sync, then skips detail, staging, promotion and quarantine. In run `36703121038` this path reported:

- 21,465 listing links inspected;
- 21,416 known-EAN links;
- 21,388 price rows refreshed;
- 14 price rows whose value actually changed;
- 14 history rows written.

The `prices_updated` counter is therefore a refreshed price-link count in this pipeline output; `changed_rows` is the count of actual price changes. These are different metrics and should both be recorded when assessing a run.

Do not use the manual `write=true` full pipeline as a substitute for the scheduled price route when only prices are required. A bounded full write in run `36694896294` discovered 40 listings and inserted or updated them, but the cover promotion step hit a PostgreSQL statement timeout in `cover_lookup_queue`. This did not affect the price-only route because that route explicitly skips cover-related stages.

### Database and run interpretation

- A green Windows price route proves that the listing-price sync completed; it does not prove that detail enrichment or cover promotion completed.
- A scheduled detail-burst run can finish successfully without work when its configured date window is closed. Run `36727597470` was such a no-op: it reported success, but performed no price or detail writes because the detail-burst window had ended.
- The observed `cover_lookup_queue` timeout was a shared database/queue-path issue, not evidence of a Windows-specific scraper failure. Read-only inspection showed queue and candidate-table bloat plus long-lived idle-in-transaction sessions, but no blocking lock at the inspection instant. The root cause was therefore not conclusively established and no database change was made as part of the runner migration.
- Ubuntu runs inspected before this migration did not show the same timeout in their logs. The underlying database path is shared, so that comparison does not prove that Ubuntu is immune to the issue.

### Operational checklist learned from JPC

- For price verification, use the scheduled Windows listing route or a manual run with the explicit price-sync flags and all detail/stage/promote/quarantine steps disabled.
- Record both `prices_updated` and `changed_rows` from the pipeline output.
- Treat a full manual write as a separate bounded test with an explicit write scope.
- Check the event and schedule before interpreting a green run; a detail-burst no-op is not a price refresh.
- If the Windows runner fails, inspect the failure and start the cloud fallback manually only when needed. The current configuration intentionally does not auto-fallback.

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

## Platomania migration: persistent runner software

Platomania uses the same Windows runner pattern in `.github/workflows/usf-platomania-windows.yml`:

- The virtual environment is kept in `%LOCALAPPDATA%\Vinylofy\platomania-venv` and the managed Python installation in `%LOCALAPPDATA%\Vinylofy\platomania-python`.
- Before each job, the workflow checks whether the Python environment exists. It creates it only when the Python executable is missing.
- Before each job, each required import is checked (`requests`, `bs4`, `psycopg`, and `dotenv`). Only missing packages are installed.
- The persistent virtual environment and managed Python are not removed after a run. Only the run-specific uv cache is cleaned.
- This check-before-install policy applies to future migrated shops as well: reuse software already present on the local runner, add only missing components, and leave the reusable installation in place after the pipeline finishes.

## FiftiesStore migration

FiftiesStore keeps its existing four-step pipeline on the Windows runner:

- refresh the Clerk listing API;
- sync listing prices to existing live prices;
- stage raw offers;
- promote staged offers.

The cloud workflow remains available through `workflow_dispatch` and no longer has an automatic schedule. The Windows workflow uses the same `usf-fiftiesstore-production` concurrency group with `cancel-in-progress: false`, so local and fallback runs cannot overlap.

The first local manual test should use a small `max_products` value and `write=false`. A bounded write can then enable `write=true` while keeping the product, stage and promote limits explicit. Scheduled execution preserves the existing full-catalog settings.

Because the FiftiesStore refresh normally marks links absent from the fetched catalog as out of stock, bounded runs pass `--skip-delist-missing`. Full scheduled runs keep the existing delist behavior; a partial sample must never be treated as the complete catalog.

## Get Back Music migration

Get Back Music uses a stateful scraper/importer workflow rather than a single USF job. The Windows workflow keeps the existing pipeline and its two schedules:

- daily listing-first price refresh;
- daily listing refresh with a bounded unresolved EAN/detail batch.

The state artifact is restored before every run. The Windows workflow searches its own latest successful artifact first and then the existing Ubuntu workflow artifact, so listing/detail progress survives the runner migration. The old Ubuntu workflow remains a manual fallback with its automatic schedules disabled.

The persistent-runner rule applies here as well: keep the Python environment under `%LOCALAPPDATA%\Vinylofy\getbackmusic-venv`, check imports before installing, and remove only the run-specific uv cache. The CSV state remains run data and is transferred through the existing GitHub Actions artifact mechanism.

## Windows runner schedule audit

The self-hosted runner executes one job at a time, but each shop has its own concurrency group. The previous schedules therefore allowed several long database pipelines to become runnable within the same 30-minute window. A green workflow did not prove that another shop was not waiting in the runner queue.

The schedules were spread across the day while keeping each workflow's frequency and pipeline parameters unchanged:

- iMusic exposure: daily `05 1 * * *`; weekly `05 0 * * 0`.
- Get Back Music: `47 3 * * *` and `17 22 * * *`.
- Variaworld: `20 5 * * *` and `20 6 * * 2,5`.
- Sounds Venlo: `35 7 * * *`.
- Platomania: `35 8 * * *`.
- Platenzaak: `55 9 * * *`.
- Bob's Vinyl: `35 10 * * *`.
- Records on Vinyl: `17 11 * * *`.
- iMusic normal pipeline: `25 12 * * *`.
- FiftiesStore: `35 13 * * *`.
- JPC listing remains twice daily at `17 4,16 * * *`; its detail bursts are `47 2,8,14,20 * * *`.
- CD Hal Ruinen remains daily at `30 0 * * *` in the `Europe/Amsterdam` timezone.

The larger workflows now have roughly an hour between scheduled starts. The shared concurrency groups remain unchanged, so a manual fallback still serializes with its local workflow. This is schedule-level protection; it does not replace inspecting a queued or unusually long run.

## iMusic migration

iMusic has two scheduled USF writers: the normal pipeline and the exposure-priority pipeline. Both were moved to Windows workflows, `usf-imusic-windows.yml` and `usf-imusic-exposures-windows.yml`. Their Ubuntu schedules were disabled while both Ubuntu workflows retain `workflow_dispatch` as manual fallbacks.

All four routes use the shared `usf-imusic-production` concurrency group with `cancel-in-progress: false`. This includes the two retained Ubuntu fallbacks and prevents normal and exposure iMusic database writers from overlapping. The existing schedules and parameters were preserved: exposure daily at `05 2 * * *`, exposure weekly at `45 2 * * 0`, and the normal pipeline at `25 3 * * *`.

The Windows workflows keep the existing pipeline code and command order. The normal route retains genre discovery, price sync, EAN seeding, stale requeue, detail, staging, promotion and quarantine behavior. The exposure route retains exposure discovery, detail, staging, promotion and quarantine. Manual inputs default to `write=false` for bounded tests; scheduled runs use the existing production limits and `write=true`.

Both workflows verify `vinylofy-windows-01`, use Windows PowerShell with process-scoped execution policy bypass, and share the persistent `%LOCALAPPDATA%\Vinylofy\imusic-venv`. Imports are checked first and only missing `requests`, `bs4`, `psycopg`, or `dotenv` packages are installed. The persistent environment is retained and only each run's uv cache is cleaned.

## Platenzaak migration

Platenzaak uses the existing USF listing-first pipeline. The scheduled Ubuntu route in `usf-platenzaak.yml` was moved to `usf-platenzaak-windows.yml`; the Ubuntu workflow keeps `workflow_dispatch` as the manual fallback. Both workflows use the existing concurrency group `vinylofy-platenzaak` with `cancel-in-progress: false`, which also protects against overlap with the retained legacy manual workflows.

The Windows workflow keeps the existing schedule (`55 2 * * *`) and scheduled parameters: full listing discovery, 500 detail records, and real writes. Manual runs retain the existing bounded inputs and default to `write=false`. The pipeline order remains listing refresh, latest-title prioritization, detail EAN enrichment, and a second listing refresh. Legacy stage/promote remain disabled by the existing pipeline contract.

The runner verifies `vinylofy-windows-01` and uses Windows PowerShell with process-scoped execution policy bypass. Python 3.11 is managed by uv under local application data. The persistent `platenzaak-venv` is reused between runs; imports are checked first and only missing `requests`, `bs4`, `psycopg`, or `dotenv` packages are installed. The persistent environment is retained and only the run-specific uv cache is cleaned.

## Bob's Vinyl migration

Bob's Vinyl uses the existing USF listing-first pipeline. The scheduled Ubuntu route in `usf-bobsvinyl.yml` was moved to `usf-bobsvinyl-windows.yml`; the Ubuntu workflow keeps `workflow_dispatch` as the manual fallback. Both workflows use `usf-bobsvinyl-${{ github.ref }}` with `cancel-in-progress: false`, so local and fallback runs cannot execute concurrently.

The Windows workflow keeps the existing schedule (`10 3 * * *`) and scheduled parameters: full listing discovery, 500 detail records and real writes. Manual runs retain the existing bounded inputs and default to `write=false` for a safe pilot. The scraper and database pipeline code were not changed.

The runner uses the known Windows labels and verifies `vinylofy-windows-01`. Python 3.11 is managed by uv under the runner service account's local application data. The persistent `bobsvinyl-venv` is reused between runs; each required import (`requests`, `bs4`, `psycopg`, and `dotenv`) is checked first and only missing packages are installed. The persistent environment and managed Python are not removed; only the run-specific uv cache is cleaned.
