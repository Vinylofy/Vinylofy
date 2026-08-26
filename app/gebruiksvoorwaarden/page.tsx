import type { Metadata } from "next";
import Link from "next/link";

import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

const LAST_UPDATED_LABEL = "26 augustus 2026";

export const metadata: Metadata = {
  title: {
    absolute: "Gebruiksvoorwaarden & databescherming | Vinylofy",
  },
  description:
    "Lees hoe je Vinylofy en de door Vinylofy opgebouwde product-, prijs- en muziekinformatie mag gebruiken.",
  alternates: {
    canonical: "https://vinylofy.com/gebruiksvoorwaarden",
  },
};

const allowedUses = [
  "Vinylofy normaal gebruiken om vinyl en aanbiedingen te zoeken.",
  "Individuele product- en prijspagina's bekijken.",
  "Links naar openbare Vinylofy-pagina's delen.",
  "Informatie voor persoonlijk, niet-commercieel gebruik raadplegen.",
  "Vinylofy laten indexeren door toegestane openbare zoekmachines die onze technische instructies respecteren.",
];

const permissionRequiredUses = [
  "Vinylofy automatisch scrapen, crawlen of uitlezen.",
  "Grote hoeveelheden gegevens downloaden of kopiëren.",
  "Steeds kleine hoeveelheden gegevens verzamelen om onze database na te bouwen.",
  "Onze gegevens gebruiken voor een andere prijsvergelijker, catalogus of commerciële datadienst.",
  "Prijshistorie, productkoppelingen of Follow the Groove-relaties opnieuw publiceren.",
  "Onze content of gegevens gebruiken voor commerciële tekst- en datamining of het trainen van AI-modellen.",
  "Technische beveiligingen, toegangsbeperkingen of rate limits omzeilen.",
];

const databaseInvestments = [
  "product- en releasegegevens",
  "winkel- en prijsinformatie",
  "beschikbaarheidsinformatie",
  "prijsontwikkelingen en prijshistorie",
  "productmatches tussen verschillende winkels",
  "artiestrelaties en Follow the Groove-verbindingen",
  "rangschikkingen, selecties en zoekresultaten",
];

const scrapingRestrictions = [
  "de gehele databank of een substantieel gedeelte daarvan op te vragen of te hergebruiken",
  "herhaaldelijk of systematisch kleinere gedeelten op te vragen",
  "gegevens te verzamelen met als doel de database of dienstverlening van Vinylofy geheel of gedeeltelijk te reconstrueren",
  "geautomatiseerde verzoeken te versturen die de werking of beschikbaarheid van Vinylofy kunnen beïnvloeden",
  "toegangsbeperkingen, rate limits of andere technische maatregelen te omzeilen",
  "niet-openbare API's, endpoints of databronnen te benaderen",
];

const commercialUses = [
  "een andere prijsvergelijker",
  "een productzoekmachine",
  "een commerciële productcatalogus",
  "een concurrerende website of applicatie",
  "marketing- of leadbestanden",
  "prijsanalyses of commerciële marktrapportages",
  "een API, datadienst of dataset voor derden",
];

const aiUses = [
  "generatieve AI-modellen",
  "grote taalmodellen",
  "machinelearningmodellen",
  "commerciële aanbevelingssystemen",
  "geautomatiseerde product- of prijsdatabases",
];

const collaborationDetails = [
  "welke gegevens je wilt gebruiken",
  "voor welk doel",
  "hoeveel gegevens je nodig hebt",
  "hoe vaak je deze wilt ophalen",
  "of de gegevens openbaar worden gepubliceerd",
];

const textLinkClassName =
  "font-semibold text-orange-600 underline underline-offset-4 transition hover:text-orange-700 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-orange-600";

const ctaLinkClassName =
  "inline-flex w-fit items-center justify-center rounded-full bg-orange-600 px-5 py-3 text-sm font-semibold !text-white transition hover:bg-orange-700 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-orange-600";

function CheckList({ items }: { items: string[] }) {
  return (
    <ul className="mt-5 space-y-3 text-sm leading-6 text-neutral-700">
      {items.map((item) => (
        <li key={item} className="flex gap-3">
          <span
            aria-hidden="true"
            className="mt-2 h-1.5 w-1.5 shrink-0 rounded-full bg-orange-600"
          />
          <span>{item}</span>
        </li>
      ))}
    </ul>
  );
}

function LegalList({ items }: { items: string[] }) {
  return (
    <ul className="mt-4 list-disc space-y-2 pl-5 text-neutral-700 marker:text-orange-600">
      {items.map((item) => (
        <li key={item}>{item};</li>
      ))}
    </ul>
  );
}

export default function GebruiksvoorwaardenPage() {
  return (
    <div className="min-h-screen bg-white text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-4xl px-6 py-12">
        <section className="border-b border-neutral-200 pb-10">
          <p className="text-sm font-medium uppercase tracking-[0.14em] text-orange-600">
            Gebruiksvoorwaarden
          </p>

          <h1 className="mt-4 text-3xl font-semibold tracking-tight md:text-4xl">
            Gebruiksvoorwaarden & databescherming
          </h1>

          <p className="mt-5 max-w-3xl text-xl font-semibold leading-8 text-neutral-950">
            Muziek ontdekken mag. Onze database kopiëren niet.
          </p>

          <div className="mt-6 space-y-5 text-base leading-7 text-neutral-600">
            <p>
              Vinylofy helpt muziekliefhebbers om vinyl te ontdekken en prijzen
              van verschillende winkels te vergelijken. Daarvoor verzamelen,
              controleren, koppelen en verrijken we grote hoeveelheden informatie.
            </p>

            <p>
              Je mag Vinylofy natuurlijk gebruiken, pagina&apos;s bekijken en links met
              anderen delen. Het geautomatiseerd kopiëren of commercieel
              hergebruiken van onze database is niet toegestaan zonder
              voorafgaande toestemming.
            </p>
          </div>
        </section>

        <section className="grid gap-4 py-10 md:grid-cols-2">
          <article className="rounded-3xl border border-neutral-200 bg-white p-6 shadow-sm">
            <h2 className="text-xl font-semibold tracking-tight text-neutral-950">
              Dit mag wel
            </h2>
            <CheckList items={allowedUses} />
          </article>

          <article className="rounded-3xl border border-neutral-200 bg-neutral-50 p-6 shadow-sm">
            <h2 className="text-xl font-semibold tracking-tight text-neutral-950">
              Hiervoor heb je toestemming nodig
            </h2>
            <CheckList items={permissionRequiredUses} />
          </article>
        </section>

        <p className="border-b border-neutral-200 pb-10 text-base leading-7 text-neutral-700">
          Wil je gegevens van Vinylofy gebruiken voor onderzoek, publicatie of een
          commerciële toepassing? Neem dan eerst{" "}
          <Link
            href="/contact"
            className={textLinkClassName}
          >
            contact met ons op
          </Link>
          . We bekijken graag of een passende samenwerking of licentie mogelijk is.
        </p>

        <div className="space-y-10 py-10 text-base leading-7 text-neutral-700">
          <section>
            <h2 className="text-2xl font-semibold tracking-tight text-neutral-950">
              1. Intellectuele eigendom en databankrechten
            </h2>

            <div className="mt-5 space-y-5">
              <p>
                De website, software, vormgeving en oorspronkelijke teksten van
                Vinylofy zijn beschermd door intellectuele eigendomsrechten.
              </p>

              <p>
                Daarnaast investeert Vinylofy in het verzamelen, controleren,
                koppelen, verrijken, ordenen en presenteren van onder andere:
              </p>

              <LegalList items={databaseInvestments} />

              <p>
                De rechten op deze door Vinylofy opgebouwde en samengestelde
                databank berusten, voor zover van toepassing, bij Vinylofy of haar
                licentiegevers.
              </p>

              <p>
                Individuele prijzen, productgegevens, handelsnamen, afbeeldingen en
                andere informatie kunnen afkomstig zijn van winkels, fabrikanten,
                databronnen of andere derden. Deze blijven eigendom van de
                betreffende rechthebbenden. Vinylofy claimt daarop geen rechten die
                ons niet op grond van de wet of een licentie toekomen.
              </p>
            </div>
          </section>

          <section>
            <h2 className="text-2xl font-semibold tracking-tight text-neutral-950">
              2. Normaal gebruik van Vinylofy
            </h2>

            <div className="mt-5 space-y-5">
              <p>
                Vinylofy mag worden gebruikt om producten en prijzen te zoeken,
                aanbiedingen te vergelijken en nieuwe muziek te ontdekken.
              </p>

              <p>
                Je mag links naar openbare pagina&apos;s delen en incidenteel naar
                informatie op Vinylofy verwijzen, mits duidelijk wordt vermeld dat
                Vinylofy de bron is.
              </p>

              <p>
                Het is niet toegestaan om informatie zodanig te gebruiken dat ten
                onrechte de indruk ontstaat dat Vinylofy betrokken is bij,
                verantwoordelijk is voor of instemt met een andere website, dienst
                of onderneming.
              </p>
            </div>
          </section>

          <section>
            <h2 className="text-2xl font-semibold tracking-tight text-neutral-950">
              3. Geautomatiseerde toegang en scraping
            </h2>

            <div className="mt-5 space-y-5">
              <p>
                Zonder voorafgaande schriftelijke toestemming is het niet
                toegestaan om Vinylofy geautomatiseerd te benaderen met robots,
                spiders, crawlers, scrapers, scripts of vergelijkbare technieken.
              </p>

              <p>Het is in het bijzonder niet toegestaan om:</p>

              <LegalList items={scrapingRestrictions} />

              <p>
                Vinylofy kan geautomatiseerde toegang beperken of blokkeren wanneer
                deze voorwaarden of onze technische instructies niet worden
                gerespecteerd.
              </p>
            </div>
          </section>

          <section>
            <h2 className="text-2xl font-semibold tracking-tight text-neutral-950">
              4. Commercieel hergebruik
            </h2>

            <div className="mt-5 space-y-5">
              <p>
                Zonder schriftelijke toestemming mogen gegevens van Vinylofy niet
                worden gekopieerd, gepubliceerd, verkocht, doorgeleverd of gebruikt
                voor:
              </p>

              <LegalList items={commercialUses} />

              <p>
                Dit geldt ook wanneer steeds afzonderlijke of kleine hoeveelheden
                gegevens worden verzameld die gezamenlijk een belangrijk deel van
                de Vinylofy-database vormen.
              </p>
            </div>
          </section>

          <section>
            <h2 className="text-2xl font-semibold tracking-tight text-neutral-950">
              5. AI en tekst- en datamining
            </h2>

            <div className="mt-5 space-y-5">
              <p>
                Vinylofy behoudt uitdrukkelijk, voor zover wettelijk toegestaan, de
                rechten voor met betrekking tot commerciële tekst- en datamining.
              </p>

              <p>
                Zonder voorafgaande schriftelijke toestemming mogen de website,
                content en databanken van Vinylofy niet worden gebruikt voor het
                trainen, testen, evalueren, ontwikkelen of verbeteren van:
              </p>

              <LegalList items={aiUses} />

              <p>
                Vinylofy kan dit voorbehoud ook door middel van machineleesbare
                instructies, metadata en technische maatregelen kenbaar maken.
              </p>

              <p>
                Wettelijke uitzonderingen die niet contractueel kunnen worden
                uitgesloten, blijven van toepassing.
              </p>
            </div>
          </section>

          <section>
            <h2 className="text-2xl font-semibold tracking-tight text-neutral-950">
              6. Prijzen en informatie van winkels
            </h2>

            <div className="mt-5 space-y-5">
              <p>
                Vinylofy doet haar best om prijs-, voorraad- en productinformatie
                zo actueel en zorgvuldig mogelijk weer te geven. Toch kunnen
                prijzen, beschikbaarheid en verzendkosten veranderen.
              </p>

              <p>
                De informatie en voorwaarden op de website van de betreffende
                winkel zijn op het moment van aankoop altijd leidend. Aan de
                informatie op Vinylofy kunnen geen rechten worden ontleend.
              </p>

              <p>
                Vinylofy is geen partij bij de overeenkomst tussen een bezoeker en
                een winkel.
              </p>
            </div>
          </section>

          <section>
            <h2 className="text-2xl font-semibold tracking-tight text-neutral-950">
              7. Commerciële links en onafhankelijkheid
            </h2>

            <div className="mt-5 space-y-5">
              <p>
                Vinylofy kan in de toekomst gebruikmaken van affiliate-links of
                andere commerciële links. Vinylofy kan dan een vergoeding ontvangen
                wanneer een bezoeker via zo&apos;n link een aankoop doet.
              </p>

              <p>
                Een eventuele vergoeding heeft geen invloed op de
                standaardrangschikking van aanbiedingen. Aanbiedingen worden
                standaard gerangschikt op objectieve criteria, zoals prijs en
                bekende verzendkosten.
              </p>

              <p>
                Gesponsorde posities of advertenties worden altijd duidelijk als
                zodanig herkenbaar gemaakt.
              </p>
            </div>
          </section>

          <section>
            <h2 className="text-2xl font-semibold tracking-tight text-neutral-950">
              8. Toestemming en samenwerking
            </h2>

            <div className="mt-5 space-y-5">
              <p>
                Wil je Vinylofy-data gebruiken voor onderzoek, journalistiek, een
                applicatie of een commerciële samenwerking? Neem dan{" "}
                <Link
                  href="/contact"
                  className={textLinkClassName}
                >
                  contact met ons op
                </Link>{" "}
                en beschrijf:
              </p>

              <LegalList items={collaborationDetails} />

              <p>
                We beoordelen vervolgens of het gebruik kan worden toegestaan en of
                daarvoor aanvullende afspraken of een licentie nodig zijn.
              </p>

              <Link
                href="/contact"
                className={ctaLinkClassName}
              >
                Neem contact op
              </Link>
            </div>
          </section>

          <section>
            <h2 className="text-2xl font-semibold tracking-tight text-neutral-950">
              9. Wijzigingen
            </h2>

            <div className="mt-5 space-y-5">
              <p>
                Vinylofy kan deze gebruiksvoorwaarden aanpassen wanneer de website,
                dienstverlening of toepasselijke wetgeving verandert.
              </p>

              <p>De meest recente versie is altijd op deze pagina beschikbaar.</p>
            </div>
          </section>
        </div>

        <p className="border-t border-neutral-200 pt-6 text-sm font-medium text-neutral-600">
          Laatst bijgewerkt: {LAST_UPDATED_LABEL}
        </p>
      </main>

      <SiteFooter />
    </div>
  );
}
