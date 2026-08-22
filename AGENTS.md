Kernprincipe

Vinylofy is een live dataproduct. Correctheid, herleidbaarheid, idempotentie en gecontroleerde wijzigingen gaan voor snelheid.

Ga nooit uit van aannames over hoe de repository werkt. Controleer vóór een wijziging eerst de actuele code, relevante tests, databasecontracten en bestaande implementaties.

Repo-evidence is leidend boven geheugen, algemene best practices of naamgeving.

Maak onderscheid tussen:

PROVEN: rechtstreeks bevestigd door code, tests, database-output of command-output.
INFERRED: zeer waarschijnlijk, maar niet direct bewezen.
UNKNOWN: nog niet vastgesteld.

Presenteer een aanname nooit als bewezen feit.

Scope beheersen

Maak de kleinst mogelijke samenhangende wijziging die het gevraagde probleem oplost.

Doe geen:

ongevraagde refactors;
hernoemingen zonder noodzaak;
architectuurwijzigingen alleen omdat iets theoretisch mooier kan;
wijzigingen aan andere shops, pipelines of features zonder directe noodzaak;
fixes van toevallig gevonden problemen buiten de opdracht.

Ontdek je iets anders, rapporteer het apart.

Repository-first werken

Start niet-triviaal werk met een precheck.

Controleer minimaal:

huidige branch;
HEAD;
origin/main indien beschikbaar;
git status;
tracked wijzigingen;
relevante untracked bestanden;
relevante code;
relevante tests.

Ga nooit automatisch ervan uit dat de lokale checkout gelijk is aan remote main.

Bescherm bestaand gebruikerswerk.

Gebruik geen destructieve Git-commando's zoals git reset --hard, git clean -fd of vergelijkbare acties zonder expliciete opdracht.

Werk structureren

Voer grotere opdrachten gefaseerd uit.

Bepaal vóór implementatie:

wat de huidige situatie is;
wat de gewenste situatie is;
welke onderdelen geraakt worden;
welke veiligheidsregels gelden;
hoe de oplossing wordt gevalideerd.

Bij afronding rapporteer je:

wat gewijzigd is;
wat bewust niet gewijzigd is;
welke tests zijn uitgevoerd;
resultaat van de tests;
eventuele databasewrites;
eventuele externe netwerkacties;
resterende risico's;
gewijzigde bestanden.
Local-first uitvoeren

Ontwikkeling en validatie zijn standaard lokaal.

Gebruik GitHub Actions niet als goedkope vervanging voor lokale tests.

GitHub Actions-verbruik en kosten zijn relevant voor Vinylofy.

Gebruik waar mogelijk:

lokale Python-tests;
lokale TypeScript-checks;
lokale builds;
dry-runs;
kleine scraperpilots.

Activeer of wijzig scheduled workflows alleen wanneer dat daadwerkelijk onderdeel van de opdracht is.

Standaard applicatiechecks

Vinylofy gebruikt onder andere Next.js, React, TypeScript en Python.

Relevante checks zijn onder andere:

pnpm lint
pnpm typecheck
pnpm build
pnpm test:cover-pipeline
pnpm verify:cover-pipeline
git diff --check

Draai niet automatisch elke dure test voor iedere kleine wijziging.

Begin gericht en schaal de validatie op wanneer de impact dat vereist.

Claim nooit PASS wanneer een test niet daadwerkelijk succesvol is afgerond.

Maak duidelijk onderscheid tussen nieuwe regressies en reeds bestaande fouten.

Database- en Supabase-veiligheid

Behandel Supabase/Postgres als productie-infrastructuur.

Structurele databasewijzigingen horen thuis in migrations onder supabase/migrations.

Waar zinvol hoort een rollback onder supabase/rollbacks.

Bij muterende databasewijzigingen:

beperk de targetset exact;
bepaal vooraf hoeveel rijen geraakt worden;
gebruik transacties waar toepasselijk;
voorkom brede UPDATE- of DELETE-statements;
maak wijzigingen waar mogelijk idempotent;
valideer na afloop de postconditie.

Gebruik voor onderzoek bij voorkeur read-only SQL.

Dry-run betekent nul databasewrites, tenzij expliciet anders gedocumenteerd.

EAN/GTIN is de productgatekeeper

EAN/GTIN is de primaire identifier voor publieke productmatching.

Maak geen publiek product op basis van alleen:

artiest en titel;
product-URL;
SKU;
afbeelding;
fuzzy matching;
waarschijnlijkheid.

Gebruik de centrale normalisatiehelpers uit de repository.

Een bronregel zonder bruikbare EAN/GTIN mag niet stilzwijgend als nieuw publiek product worden gepubliceerd.

Verzin nooit een EAN.

Scraperarchitectuur eerst onderzoeken

Bekijk bij nieuwe shops eerst:

scripts/scrapers;
scripts/scrapers/usf;
scripts/importers;
scripts/importers/contracts.py;
scripts/importers/template_shop.py;
scripts/importers/registry.py;
relevante tests;
relevante workflows.

Gebruik de technisch meest vergelijkbare werkende shop als voorbeeld.

Niet automatisch de nieuwste scraper.

Contract voor nieuwe shops

Een nieuwe reguliere shop hoort het bestaande onboardingcontract te volgen.

Minimaal:

de scraper levert een stabiele bronfile;
kolomnamen zijn voorspelbaar;
data wordt gemapt naar CanonicalRecord;
required_columns worden gedeclareerd;
optional_columns worden gedeclareerd;
er is een ShopImporterDefinition;
de importer komt in scripts.importers.registry;
pipelineconfiguratie wordt waar mogelijk uit die registry afgeleid;
headervalidatie werkt;
importer dry-run werkt;
pipeline dry-run werkt waar toepasselijk.

Gebruik template_shop.py of een nauw verwante bestaande shop als basis.

Listing-first prijsarchitectuur

Voor normale winkels is de listing leidend voor:

actuele artikelprijs;
saleprijs;
beschikbaarheid indien zichtbaar;
product-URL;
catalog discovery.

Detailpagina's zijn bedoeld voor verrijking.

Detail mag bijvoorbeeld leveren:

EAN;
metadata;
formaat;
second-hand-classificatie;
covercandidate;
verificatie.

Een detailrun mag geen recentere listingprijs overschrijven.

Ook availability blijft listing-first, tenzij de shopimplementatie aantoonbaar een ander contract gebruikt.

Saleprijs boven basisprijs

Wanneer een publieke saleprijs en een reguliere prijs zichtbaar zijn, is de daadwerkelijk te betalen saleprijs leidend.

Gebruik niet als productprijs:

kortingscodebedragen;
loyaliteitskortingen;
verzendkosten;
oude doorgestreepte prijzen;
andere willekeurige bedragen op de pagina.

Gebruik alleen publiek zichtbare prijzen.

Listing refresh los van detail enrichment

Prijsactualiteit en dure detailverrijking moeten zo veel mogelijk los van elkaar staan.

Voorkeur:

listings relatief vaak verversen;
detail minder vaak;
bestaande geldige EAN's hergebruiken;
failures gecontroleerd opnieuw proberen;
geen complete detailcrawl alleen om prijzen te verversen.

Prijsactualiteit mag niet afhankelijk zijn van een succesvolle detailfetch.

Large-catalog policy

Behandel een shop als groot wanneer ongeveer een van deze grenzen wordt bereikt:

5.000 of meer productlinks;
meer dan circa 60 minuten voor een volledige detailrun;
meer dan circa 2.000 openstaande detail/EAN-items.

Dan geldt:

listingrefresh blijft prioriteit;
detail wordt gebatcht;
gebruik cursor/checkpoint/resume;
vermijd resolved items opnieuw opvragen;
verlaag detailfrequentie indien nodig.
Netwerkgedrag scrapers

Scrapers moeten terughoudend omgaan met externe sites.

Gebruik:

timeouts;
beperkte retries;
beperkte concurrency;
retry/backoff bij tijdelijke fouten;
expliciete 429-handling waar nodig;
consistente User-Agent.

Geen:

oneindige retries;
onbeperkte requests;
CAPTCHA-bypass;
authenticatie-omzeiling;
steeds agressievere anti-bot-evasie.

Wanneer een winkel scraping blokkeert, stop gecontroleerd en rapporteer dat.

Pagination moet kunnen stoppen

Iedere scraper moet een aantoonbare stopconditie hebben.

Mogelijke signalen:

lege laatste pagina;
expliciete last-page metadata;
herhaling van dezelfde productset;
geen nieuwe URL's;
einde API-pagination.

Gebruik niet uitsluitend een willekeurig maximum paginanummer als enige logica.

Voorkom infinite-scroll-loops en herhalende pagina's.

Nieuwe scraper: fase A

Begin met read-only reconnaissance.

Onderzoek:

platform/CMS;
catalogroutes;
pagination;
prijsstructuur;
saleprijzen;
voorraadweergave;
product-URL;
EAN;
metadata;
second-hand;
rate limits;
anti-botgedrag.

Doe nog geen volledige crawl.

Nieuwe scraper: fase B

Voer een kleine listingpilot uit.

Gebruik bijvoorbeeld 1 tot 3 pagina's.

Controleer:

productdetectie;
URL-normalisatie;
deduplicatie;
prijs;
saleprijs;
availability;
pagination;
stopconditie.
Nieuwe scraper: fase C

Voer daarna een kleine detailpilot uit.

Gebruik slechts een beperkt aantal producten.

Controleer:

EAN-extractie;
metadata;
failure handling;
retries;
dat detail listingprijs niet overschrijft.
Nieuwe scraper: fase D

Draai vervolgens de importer in dry-run.

Controleer:

headers;
mappings;
EAN-validatie;
duplicates;
rejects;
summary-output.

Geen productiewrites in deze fase.

Nieuwe scraper: fase E

Pas na geslaagde pilots:

shop registreren;
contracttests toevoegen;
pipeline koppelen;
bounded end-to-end test uitvoeren.

Start nog steeds niet direct met een volledige productiecatalogus.

Tweedehands platen

Ontbrekende EAN betekent niet automatisch tweedehands.

Classificeer alleen als second-hand wanneer daar bronbewijs of een bestaande expliciete shopregel voor bestaat.

Verzwak de EAN-gatekeeper niet om tweedehands producten tóch als regulier product te publiceren.

Centrale coverarchitectuur

Productcovers worden centraal beheerd.

scripts/maintenance/cover_worker.py is de centrale publisher.

Shop-scrapers mogen niet rechtstreeks willekeurige externe afbeeldingen als definitieve productcover publiceren.

Shopafbeeldingen gaan als structured candidate of bronmetadata de coverpipeline in.

Covercandidate safety

Correcte covers zijn belangrijker dan een hoge coverage.

Gebruik geen willekeurige img-tags als betrouwbare coverbron.

Voorkeur gaat uit naar gestructureerde productmetadata.

Een rejected candidate blijft rejected, tenzij een expliciete recoveryroute anders bepaalt.

Bekende slechte assets mogen exact op URL en/of hash worden geblokkeerd.

Blokkeer nooit zonder bewijs een hele shop of heel domein wanneer slechts één asset fout is.

Coveralternatieven behouden

Wanneer een slechte cover wordt geblokkeerd:

deselecteer de slechte candidate;
voorkom herpublicatie;
behoud andere geldige candidates;
requeue het product waar toepasselijk;
verwijder geen unrelated candidates.

Verwijder geen fysieke Storage-objecten puur op basis van aannames.

Cover storage

Nieuwe canonieke productcovers gebruiken de bestaande conventie:

ean/<eerste-drie-EAN-cijfers>/<EAN>.webp

Introduceer niet stilzwijgend een andere opslagstructuur.

Bestaande geldige legacycovers hoeven niet te worden vervangen enkel vanwege ander formaat of pad.

De publieke browserlaag hoort Vinylofy-owned URLs te gebruiken en niet rechtstreeks externe shops of raw Supabase-URLs als eindcontract.

Follow the Groove relaties

Follow the Groove mag alleen relaties tonen waarvoor geldige evidence bestaat.

Toegestane categorieën zijn onder andere:

huidige of voormalige bandleden;
supergroups;
solo-artiest ↔ band;
recording performer;
artist credit;
gastbijdragen op recordings;
collaborations;
bands gekoppeld via gedeeld lid;
similarity.

Verzin nooit muzikale relaties.

Follow the Groove uitgesloten relaties

Maak geen FTG-relatie alleen op basis van:

producer;
mixer;
mastering;
engineer;
songwriter;
lyricist;
composer;
arranger;
label;
management;
studio;
zakelijke relatie;
familie;
romantische relatie;
covers;
samples;
tribute;
interpolation;
touring-only;
live-only.

Bij onvoldoende bewijs: geen edge.

Follow the Groove ranking

Behoud bestaande rankingcontracten tenzij de opdracht expliciet iets verandert.

Belangrijk:

Python- en TypeScript-logica blijven gelijk;
product_count mag ranking niet onbedoeld domineren;
bezochte artiesten worden niet opnieuw als ongewenste bridge gebruikt;
artist-family filtering blijft intact;
echte bands en collaborations mogen niet door naamsgelijkenis worden weggefilterd;
output-evidence blijft fail-closed.
Follow the Groove zoekintegratie

Standalone FTG en search hebben verschillende eligibility.

Standalone mag waar toegestaan ook artiesten zonder Vinylofy-aanbod tonen.

Search is strenger.

Search-kandidaten moeten voldoen aan de bestaande eligibilityvoorwaarden, inclusief relevant productaanbod.

Verlaag de evidence-eisen niet alleen om drie resultaten te kunnen vullen.

Follow the Groove UI-contract

Behoud voor V1:

maximaal 5 standalone candidates;
maximaal 3 search-candidates;
maximaal één FTG-searchblok;
bij voldoende zoekresultaten na resultaat 5;
bij minder dan 5 resultaten na het laatste resultaat;
search-candidates moeten search-eligible zijn.

Voeg geen prijzen aan discoverycards toe tenzij de productrequirement expliciet verandert.

Follow the Groove persistence

Collector- en persistencewerk moet:

bounded zijn;
resumable zijn waar geïmplementeerd;
retry-safe zijn;
idempotent zijn waar mogelijk;
veilig omgaan met concurrency;
transactieveilig zijn.

Behoud execution IDs en single-flight bescherming waar aanwezig.

Unknown blijft unknown.

Leid proven_bridge_only nooit af uit het ontbreken van gegevens.

Frontend prijscontract

Zoekresultaten tonen de artikelprijs exclusief verzendkosten.

Sorteer en vergelijk daar eveneens op artikelprijs, tenzij de UI expliciet een totaalprijs toont.

Verzendkosten mogen apart worden getoond.

Verander productdetail- of shippinglogica niet als neveneffect van een searchwijziging.

Frontend discipline

Volg de bestaande Next.js App Router-architectuur.

Hergebruik bestaande:

components;
utilities;
Supabase-patterns;
formatters;
responsive patronen;
visuele tokens.

Maak geen parallelle businesslogic wanneer er al een gedeelde helper bestaat.

Behoud accessibility en semantische HTML.

MusicBrainz en masterdata

MusicBrainz is een primaire masterdata- en evidencebron.

Respecteer:

bestaande User-Agent;
rate limits;
huidige matchinglogica.

Sterkere canonieke metadata mag niet worden overschreven door zwakkere shopmetadata.

Shopmetadata kan fallback of placeholder zijn waar dat bestaande contract dit toestaat.

Forceer nooit een MusicBrainz-match bij ambiguïteit.

Definition of done

Een opdracht is pas klaar wanneer het relevante deel aantoonbaar correct werkt.

Controleer waar toepasselijk:

requested behaviour geïmplementeerd;
scope beperkt gebleven;
relevante tests groen;
typecheck groen;
lint groen;
build groen;
git diff --check groen;
geen onbedoelde productiewrites;
geen unrelated bestanden gewijzigd;
geen secrets toegevoegd;
migrations veilig;
dry-run semantiek intact;
resultaat daadwerkelijk geverifieerd.

Gebruik in de eindrapportage alleen woorden als fixed, live, merged, clean of green wanneer dat daadwerkelijk gecontroleerd is.