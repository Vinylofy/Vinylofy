#!/usr/bin/env python3
from __future__ import annotations

import csv
import sys
from pathlib import Path

CSV_PATH = Path(sys.argv[1] if len(sys.argv) > 1 else 'data/raw/shop3345/3345_products.csv')

SUSPECT = {'80', '80,00', '€80', '€80,00'}


def clean_text(value: object) -> str:
    return str(value or '').strip()


def normalize_price(value: str) -> str:
    value = clean_text(value).replace('EUR', '').replace('€', '').strip()
    value = value.replace('.', ',')
    if value.endswith(',0'):
        value += '0'
    return value


if not CSV_PATH.exists():
    raise SystemExit(f'CSV not found: {CSV_PATH}')

with CSV_PATH.open('r', encoding='utf-8-sig', newline='') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames or []
    rows = list(reader)

changed = 0
for row in rows:
    price = normalize_price(row.get('price', ''))
    if price in SUSPECT:
        row['price'] = ''
        status = clean_text(row.get('detail_status'))
        if 'price_repair_needed' not in status:
            row['detail_status'] = f'{status}|price_repair_needed'.strip('|')
        changed += 1

with CSV_PATH.open('w', encoding='utf-8-sig', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f'Updated {changed} suspect 3345 price rows in {CSV_PATH}')
