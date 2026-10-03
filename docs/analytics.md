# Vinylofy analytics

Vinylofy gebruikt PostHog Cloud via de officiële `posthog-js` SDK. De browserinitialisatie staat in `instrumentation-client.ts` en wordt alleen uitgevoerd wanneer beide publieke environment variables aanwezig zijn:

```text
NEXT_PUBLIC_POSTHOG_PROJECT_TOKEN
NEXT_PUBLIC_POSTHOG_HOST
```

Er staan geen tokens of hosts hardcoded in de broncode. Ontbrekende variables schakelen alleen analytics uit; de applicatie blijft functioneren.

## SDK-gedrag en privacy

- `defaults: "2026-05-30"` is ingesteld.
- Pageviews volgen PostHog history changes, zodat App Router-navigatie automatisch wordt gemeten zonder een tweede handmatige pageviewlaag.
- Performance/Web Vitals worden via `capture_performance` aangezet.
- `autocapture` staat uit. Daardoor worden geen formulierkliks, toetsaanslagen of ongecontroleerde inputwaarden als autocapture-events verstuurd.
- Session Replay blijft technisch beschikbaar. Inputwaarden worden gemaskeerd via `session_recording.maskAllInputs`.
- Vinylofy roept `identify` niet aan en gebruikt geen eigen distinct ID.
- Analytics-calls zijn fire-and-forget en worden defensief afgevangen; ze blokkeren renderen, zoeken of outbound navigatie niet.
- Zoekstrings worden getrimd, whitespace wordt genormaliseerd, de lengte is maximaal 120 tekens en herkenbare e-mailadressen worden als `[redacted]` verstuurd.
- Er worden geen e-mailadressen, accountnamen, IP-adressen of andere expliciete persoonsgegevens als eventproperty verstuurd.

Iedere custom event krijgt de property `environment`, met `NEXT_PUBLIC_VERCEL_ENV` als die beschikbaar is en anders `NODE_ENV`.

## Naming convention

Gebruik business-events in lowercase snake_case. Eventnamen horen bij een gebruikersactie of funnelstap, niet bij een concreet UI-element of component. Voeg nieuwe events en hun properties eerst hier toe en implementeer daarna een typed helper in `lib/analytics.ts`.

## Eventcatalogus

| Event | Properties | Wanneer |
| --- | --- | --- |
| `search_performed` | `query`, `normalized_query`, `result_count`, `source`, `search_type` | Na een daadwerkelijk uitgevoerde productzoekactie en nadat de resultatenpagina geladen is. |
| `search_result_clicked` | `query`, `normalized_query`, `result_position`, `result_count`, `release_id`, `artist_name`, `ean`, `source` | Bij klik op een zoekresultaat naar een release/productdetail. |
| `autocomplete_selected` | `query`, `selected_value`, `result_position`, `entity_type`, `source` | Alleen bij selectie van een autocomplete-resultaat. |
| `release_viewed` | `release_id`, `ean`, `artist_name`, `release_title`, `offer_count`, `lowest_price`, `source` | Een productdetailpagina is daadwerkelijk geladen. |
| `offers_expanded` | `release_id`, `ean`, `offer_count`, `source` | Helper is voorbereid, maar de huidige UI heeft geen aparte expand-actie. |
| `offer_clicked` | `release_id`, `ean`, `shop_id`, `shop_name`, `price`, `shipping_cost`, `total_price`, `offer_position`, `is_cheapest`, `source` | Helper is voorbereid voor een aparte in-app offer-interactie. De huidige uitgaande shopactie gebruikt uitsluitend `shop_clicked`. |
| `shop_clicked` | `release_id`, `ean`, `artist_name`, `release_title`, `shop_id`, `shop_name`, `price`, `shipping_cost`, `total_price`, `offer_position`, `is_cheapest`, `source` | Bij de daadwerkelijke klik op een externe shop/productpagina. |
| `price_history_viewed` | `release_id`, `ean`, `artist_name`, `release_title`, `source` | Bij zichtbare productdetail-prijshistorie. |
| `ftg_started` | `start_artist_id`, `start_artist_name`, `source` | Bij selectie van de startartiest in FTG. |
| `ftg_relation_clicked` | `start_artist_id`, `start_artist_name`, `from_artist_id`, `from_artist_name`, `to_artist_id`, `to_artist_name`, `relation_type`, `depth` | Bij een bestaande FTG-relatie-link. `relation_type` gebruikt bestaande reason-codes: `membership`, `recording_collaboration`, `factual_and_similarity`, `similar_artist`. |
| `ftg_artist_viewed` | `start_artist_id`, `start_artist_name`, `artist_id`, `artist_name`, `depth` | Bij het laden van een FTG-artiestpagina of het openen van een trail-breadcrumb. |
| `ftg_release_clicked` | `start_artist_id`, `start_artist_name`, `artist_id`, `artist_name`, `release_id`, `release_title`, `ean`, `depth` | Niet gekoppeld: de huidige FTG-UI heeft geen directe release-link. |
| `country_changed` | `from_country`, `to_country` | Bij een daadwerkelijke marktkeuze. |
| `sort_changed` | `page_type`, `sort_from`, `sort_to` | Bij een daadwerkelijke wijziging van zoeksortering. |

`source` gebruikt waar mogelijk bestaande herkomstwaarden: `homepage`, `header`, `search`, `release_page`, `release_detail`, `top45`, `new_releases`, `ftg` en `unknown`.

## Bestaande koppelingen

De actieve journeys zijn gekoppeld in:

- homepage/header/autocomplete en zoekresultaten;
- productdetail, prijshistorie en externe shoplinks;
- Top 45/Top Deals, nieuwe releases en homepage-discoverylinks;
- marktselector en zoeksortering;
- FTG start, trail-artiestpagina’s, breadcrumbs en relatiekeuzes.

De huidige code heeft geen aparte “alle aanbiedingen tonen”-expand-actie en geen directe FTG-releasekaart. Daarom worden `offers_expanded` en `ftg_release_clicked` nog niet geproduceerd. `offer_clicked` blijft gereserveerd voor een toekomstige afzonderlijke in-app offer-interactie om geen dubbel event naast `shop_clicked` te produceren.

## Lokaal testen

1. Stel beide environment variables lokaal in.
2. Start `pnpm dev`.
3. Open de homepage en navigeer client-side naar `/search`; controleer in de browser Network-tab de PostHog requests en controleer dat er per route één `$pageview` is.
4. Test een zoekactie, autocomplete-selectie, zoekresultaat, productdetail, prijshistorie, shoplink, marktwijziging, sortering en FTG-relatie.
5. Gebruik in PostHog Live Events de eventnamen hierboven; custom events bevatten `environment`.

## Wijzigingen aan analytics

Voeg geen losse `posthog.capture()`-calls toe aan UI-componenten. Definieer eventnaam, propertytype en helper in `lib/analytics.ts`, koppel die helper aan de concrete gebruikersactie en werk deze documentatie bij. Houd tracking optioneel, privacy-minimaal en niet-blokkerend.
