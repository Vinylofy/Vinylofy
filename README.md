# Vinylofy cover-art MVP bundle

Deze bundle bevat een backend-first MVP voor de cover-art flow.

## Bestandsoverzicht

- `supabase/migrations/20260401143000_add_cover_pipeline_mvp.sql`
- `scripts/maintenance/cover_worker.py`
- `scripts/maintenance/cover_seed_preload.py`
- `scripts/maintenance/cover_export_lists.py`
- `lib/supabase/admin.ts`
- `app/api/covers/queue/route.ts`
- `components/cover-queue-beacon.tsx`
- voorbeeld-integraties:
  - `app/search/page.tsx`
  - `app/product/[id]/page.tsx`
  - `app/top25/page.tsx`
  - `app/nieuwe releases/page.tsx`

## Omgevingsvariabelen

Voeg toe:

- `SUPABASE_SERVICE_ROLE_KEY=...`
- `MUSICBRAINZ_USER_AGENT=Vinylofy/0.1 (covers; your-email-or-contact-url)`

Bestaand nodig:

- `DATABASE_URL=...`
- `NEXT_PUBLIC_SUPABASE_URL=...`

## Python packages

Installeer minimaal:

```bash
pip install "psycopg[binary]" python-dotenv requests supabase
```

## Aanbevolen uitvoeringsvolgorde

1. SQL migratie draaien.
2. Bucket checken in Supabase Storage: `product-covers`.
3. `SUPABASE_SERVICE_ROLE_KEY` en `MUSICBRAINZ_USER_AGENT` toevoegen.
4. Seed batch laden:
   ```bash
   python -u scripts/maintenance/cover_seed_preload.py \
     --batch april-home-seed \
     --input data/cover_seed_home.csv \
     --apply
   ```
5. Worker draaien:
   ```bash
   python -u scripts/maintenance/cover_worker.py --limit 50
   ```
6. Vroege queue-triggers activeren via:
   - search
   - top25
   - nieuwe releases
   - detail fallback
7. Exports draaien:
   ```bash
   python -u scripts/maintenance/cover_export_lists.py --kind missing --out output/cover_missing.csv
   python -u scripts/maintenance/cover_export_lists.py --kind failed_review --out output/cover_failed_review.csv
   python -u scripts/maintenance/cover_export_lists.py --kind priority --out output/cover_priority.csv
   python -u scripts/maintenance/cover_export_lists.py --kind status --out output/cover_status.csv
   ```

## Testcases

### 1. Seed test
- laad 5 bekende EANs in met `--apply`
- controleer dat `product_cover_queue.state = 'pending'`

### 2. Worker test
- draai `cover_worker.py --limit 5`
- controleer:
  - `products.cover_status = 'ready'`
  - `products.cover_url` gevuld
  - `products.cover_storage_path` gevuld
  - bucket bevat file onder `ean/...`

### 3. Frontend trigger test
- open `/search?q=...`
- controleer dat POST naar `/api/covers/queue` gebeurt
- controleer dat dezelfde producten in queue komen

### 4. Placeholder test
- product zonder `cover_url` toont placeholder
- na worker-run toont dezelfde kaart lokale `cover_url`

## Dashboard-fase later

Aanbevolen pagina: `Cover Management`

Blokken:
- status cards: missing / queued / ready / failed / review
- upload area voor CSV
- textarea voor handmatige EAN-paste
- tabel voor failed/review
- export buttons
- button `Start preload batch`
- lijst van priority candidates

## Belangrijke noot

De worker implementeert MusicBrainz + Cover Art Archive volledig.
De Muziekweb fallback is bewust als adapter-hook voorbereid, maar niet hard aangezet voor unattended writes; voeg die pas toe nadat je een betrouwbare barcode/title -> cover retrieval-route hebt gevalideerd.

## Central cover pipeline — operations

### Publicatiecontract

- `scripts/maintenance/cover_worker.py` is de centrale publisher voor productcovers.
- Externe shopafbeeldingen worden niet rechtstreeks naar `products.cover_url` gepubliceerd. Scrapers/promoters leveren deze als cover-candidate metadata aan.
- MusicBrainz en Cover Art Archive leveren cover-candidates; zij publiceren geen productcover rechtstreeks.
- Een bestaande geldige lokale cover wordt door de centrale pipeline gerespecteerd.
- EAN blijft de publieke gatekeeper voor producten.

### Pipeline en workflows

De centrale coverlaag gebruikt drie workflows met gedeelde cover-pipeline concurrency:

- `.github/workflows/cover-candidate-refresh.yml` verzamelt/ververst candidates.
- `.github/workflows/cover-publish.yml` verwerkt de centrale publicatiequeue.
- `.github/workflows/cover-backfill.yml` is de handmatige backfillroute.

Na integratie mogen Candidate Refresh en Cover Publish pas worden geactiveerd nadat de geïntegreerde toestand is gevalideerd. Cover Backfill blijft uit tot de grootschalige backfill in Fase 8.

Een normale globale worker-dry-run moet vóór muterende routes stoppen en resulteert in nul claims, downloads, uploads en publicaties.

### Storagecontract

Nieuwe productcovers gebruiken het canonieke pad:

`ean/<eerste-3-cijfers-van-EAN>/<EAN>.webp`

Het gedefinieerde releasepad is:

`covers/releases/<release_calendar.id>.webp`

Bestaande legacy lokale covers kunnen een ander bestaand afbeeldingsformaat hebben en worden niet uitsluitend vanwege dat formaat vervangen.

Rollbacklogica verwijdert geen Storage-objecten op basis van inferentie.

### Nieuwe releases

Publiek getoonde release-calendar-items gebruiken de cover van het gekoppelde product. Externe Bob's Vinyl-afbeeldingen worden alleen als `image_source_url` bronmetadata opgeslagen en niet rechtstreeks als publieke releasecover gebruikt.

De Bob's Vinyl release-discovery retryt HTTP 429, 502, 503 en 504 en request-fouten maximaal drie pogingen. Een numerieke `Retry-After` wordt gerespecteerd; anders wordt begrensde exponentiële backoff gebruikt.

### Fasegrenzen

- **Fase 7 — Public Vinylofy Image Layer:** browser-zichtbare cover-URL's worden via een Vinylofy-owned publieke URL-laag aangeboden. Directe shop-, MusicBrainz-, Cover Art Archive- of `*.supabase.co` image-URL's horen niet het uiteindelijke publieke browsercontract te zijn.
- **Fase 8 — Large-scale Cover Backfill & Recovery:** pas hier wordt de covervoorraad grootschalig aangevuld. Start met een baseline en kleine gecontroleerde batch, evalueer de resultaten en schaal daarna op met behoud van idempotentie.
