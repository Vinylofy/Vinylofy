#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Iterable

import requests

API_ROOT = "https://api.github.com"


def log(msg: str) -> None:
    print(msg, flush=True)


def github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "vinylofy-recordsonvinyl-state-restore",
    }


def get_json(session: requests.Session, url: str) -> dict:
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def iter_successful_runs(session: requests.Session, repo: str, workflow_name: str, per_page: int = 25) -> Iterable[dict]:
    url = f"{API_ROOT}/repos/{repo}/actions/runs?status=completed&per_page={per_page}"
    payload = get_json(session, url)
    runs = payload.get("workflow_runs", [])
    for run in runs:
        if run.get("name") != workflow_name:
            continue
        if run.get("conclusion") != "success":
            continue
        yield run


def find_artifact(session: requests.Session, repo: str, run_id: int, prefix: str) -> dict | None:
    url = f"{API_ROOT}/repos/{repo}/actions/runs/{run_id}/artifacts"
    payload = get_json(session, url)
    for artifact in payload.get("artifacts", []):
        name = artifact.get("name", "")
        expired = artifact.get("expired", False)
        if name.startswith(prefix) and not expired:
            return artifact
    return None


def download_and_extract(session: requests.Session, artifact_url: str, target_dir: Path) -> int:
    with tempfile.TemporaryDirectory(prefix="recordsonvinyl_restore_") as tmpdir:
        archive_path = Path(tmpdir) / "artifact.zip"
        with session.get(artifact_url, stream=True, timeout=120) as response:
            response.raise_for_status()
            with archive_path.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        extract_dir = Path(tmpdir) / "extract"
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(extract_dir)

        copied = 0
        target_dir.mkdir(parents=True, exist_ok=True)
        for path in extract_dir.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in {".csv", ".json"}:
                continue
            destination = target_dir / path.name
            shutil.copy2(path, destination)
            copied += 1
        return copied


def main() -> int:
    parser = argparse.ArgumentParser(description="Restore latest non-expired workflow artifact with CSV/JSON state")
    parser.add_argument("--repo", required=True, help="owner/repo")
    parser.add_argument("--target-dir", required=True)
    parser.add_argument("--artifact-prefix", required=True)
    parser.add_argument("--workflow-name", action="append", required=True)
    args = parser.parse_args()

    token = os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")
    if not token:
        log("[restore] GH_TOKEN / GITHUB_TOKEN ontbreekt; restore wordt overgeslagen.")
        return 0

    session = requests.Session()
    session.headers.update(github_headers(token))

    target_dir = Path(args.target_dir)
    for workflow_name in args.workflow_name:
        log(f"[restore] Zoek runs voor workflow: {workflow_name}")
        for run in iter_successful_runs(session, args.repo, workflow_name):
            run_id = run.get("id")
            if not run_id:
                continue
            artifact = find_artifact(session, args.repo, int(run_id), args.artifact_prefix)
            if artifact is None:
                continue
            log(f"[restore] Gebruik artifact {artifact.get('name')} uit run {run_id}")
            copied = download_and_extract(session, artifact["archive_download_url"], target_dir)
            log(f"[restore] Herstelde bestanden: {copied}")
            return 0

    log("[restore] Geen bruikbaar artifact gevonden; ga door zonder restore.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
