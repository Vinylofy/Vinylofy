#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Render cover-pipeline JSON-metrics als "
            "GitHub Actions-samenvatting."
        )
    )
    parser.add_argument("--title", required=True)
    parser.add_argument("--status", default="")
    parser.add_argument(
        "--summary-json",
        action="append",
        required=True,
        metavar="LABEL=PATH",
    )
    return parser.parse_args()


def markdown_value(value: Any) -> str:
    if isinstance(value, str):
        rendered = value
    else:
        rendered = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
        )

    return (
        rendered.replace("|", "\\|")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def render_table(
    title: str,
    values: dict[str, Any],
) -> list[str]:
    rows = [
        (str(key), value)
        for key, value in sorted(values.items())
        if not isinstance(value, (dict, list))
    ]

    if not rows:
        return []

    output = [
        f"#### {title}",
        "",
        "| Veld | Waarde |",
        "|---|---:|",
    ]

    output.extend(
        f"| `{key}` | {markdown_value(value)} |"
        for key, value in rows
    )
    output.append("")

    return output


def render_summary(
    label: str,
    path: Path,
) -> list[str]:
    output = [f"### {label}", ""]

    if not path.is_file():
        return output + [
            f"Samenvattingsbestand ontbreekt: `{path}`",
            "",
        ]

    try:
        payload = json.loads(
            path.read_text(encoding="utf-8")
        )
    except Exception as exc:
        return output + [
            (
                "Samenvattingsbestand kon niet worden gelezen: "
                f"`{type(exc).__name__}: {exc}`"
            ),
            "",
        ]

    if not isinstance(payload, dict):
        return output + [
            "Samenvattingsbestand bevat geen JSON-object.",
            "",
        ]

    run_values = {
        key: value
        for key, value in payload.items()
        if key != "metrics"
    }
    output.extend(render_table("Run", run_values))

    metrics = payload.get("metrics")
    if isinstance(metrics, dict):
        output.extend(render_table("Metrics", metrics))

    return output


def parse_specification(
    specification: str,
) -> tuple[str, Path]:
    label, separator, path_text = specification.partition("=")

    if (
        not separator
        or not label.strip()
        or not path_text.strip()
    ):
        raise ValueError(
            "--summary-json verwacht LABEL=PATH"
        )

    return label.strip(), Path(path_text.strip())


def main() -> None:
    args = parse_args()

    output = [
        f"## {args.title}",
        "",
        f"- Workflowstatus: `{markdown_value(args.status)}`",
        "",
    ]

    for specification in args.summary_json:
        label, path = parse_specification(specification)
        output.extend(render_summary(label, path))

    print("\n".join(output).rstrip() + "\n")


if __name__ == "__main__":
    main()
