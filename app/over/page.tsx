import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

export default function OverPage() {
  return (
    <div className="min-h-screen bg-white text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-4xl px-6 py-12">
        <div className="rounded-3xl border border-neutral-200 bg-white p-8 md:p-10">
          <p className="text-sm font-medium uppercase tracking-[0.14em] text-orange-600">
            Over Vinylofy
          </p>

          <h1 className="mt-4 text-3xl font-semibold tracking-tight md:text-4xl">
            Vind vinyl. Vergelijk prijzen. Kies zelf.
          </h1>

          <div className="mt-8 space-y-5 text-neutral-600">
            <p>
              Vinylofy is ontstaan vanuit een persoonlijke uitdaging: bij het
              kopen van een vinylplaat wilde ik eenvoudig kunnen zien waar deze
              op dat moment het voordeligst werd aangeboden. Dat bleek vaak
              meer zoekwerk dan verwacht. Vanuit die persoonlijke interesse ben
              ik Vinylofy gaan bouwen.
            </p>

            <p>
              Vinylofy vergelijkt prijzen van nieuwe vinylplaten bij
              verschillende winkels. Het doel is om je snel inzicht te geven in
              het beschikbare aanbod, zodat je zelf kunt bepalen waar je een
              plaat koopt.
            </p>

            <p>
              We doen ons best om prijzen, beschikbaarheid en aanbiedingen zo
              volledig en actueel mogelijk weer te geven. Toch kunnen we niet
              garanderen dat Vinylofy altijd iedere winkel, iedere aanbieding
              of op ieder moment de allerlaagste prijs toont. Prijzen en
              voorraden kunnen bovendien tussentijds veranderen. De informatie
              op de website van de aanbieder is daarom altijd leidend.
            </p>

            <p>
              <strong className="text-neutral-900">
                Meer dan alleen de laagste prijs
              </strong>
            </p>

            <p>
              De laagste prijs hoeft niet automatisch de beste keuze te zijn.
              Vinylofy beoordeelt niet de klantenservice, betrouwbaarheid,
              levertijd of de zorg waarmee een winkel platen verpakt en
              verzendt.
            </p>

            <p>
              Soms kan het daarom de moeite waard zijn om een paar euro meer te
              betalen voor een winkel waar je goede ervaringen mee hebt. Ook
              het bewust steunen van een kleine, zelfstandige of lokale
              platenzaak kan een goede reden zijn om niet uitsluitend voor de
              laagste prijs te kiezen.
            </p>

            <p>
              Je kunt verzendkosten bovendien soms voorkomen door een winkel
              zelf te bezoeken. Maak er een uitje van, blader door de bakken,
              ontdek iets wat je niet zocht en praat met de mensen achter de
              winkel. De echte vinylervaring vind je tenslotte niet alleen in
              de brievenbus, maar vooral in de platenzaak.
            </p>

            <p>
              <strong className="text-neutral-900">
                Waar Vinylofy zich op richt
              </strong>
            </p>

            <p>De huidige focus ligt op:</p>

            <p>
              • prijsvergelijking;
              <br />
              • het vinden van aanbiedingen;
              <br />
              • het vergelijken van winkels;
              <br />• nieuwe, ongebruikte vinylplaten.
            </p>

            <p>
              Tweedehands vinyl valt op dit moment nog buiten de vergelijking,
              maar kan mogelijk in een latere fase worden toegevoegd.
            </p>

            <p>
              Vinylofy is niet opgezet als community, reviewplatform of
              uitgebreide muziekdatabase. De kern blijft het inzichtelijk maken
              van prijzen en aanbiedingen.
            </p>

            <p>
              <strong className="text-neutral-900">
                Verdere ontwikkeling
              </strong>
            </p>

            <p>
              Vinylofy is nog volop in ontwikkeling. Mogelijke toekomstige
              uitbreidingen zijn onder andere:
            </p>

            <p>
              • prijsalerts bij prijsdalingen;
              <br />
              • persoonlijke verlanglijsten;
              <br />• uitgebreidere filters en persoonlijke voorkeuren.
            </p>

            <p>
              Deze functies vragen om een uitgebreidere en complexere
              technische opzet. Het zijn daarom ontwikkelrichtingen en nog geen
              vaststaande toezeggingen.
            </p>

            <p>
              <strong className="text-neutral-900">Mis je een winkel?</strong>
            </p>

            <p>
              Koop je regelmatig bij een platenzaak die nog niet in Vinylofy
              staat? Of wil je jouw eigen webshop laten opnemen in de
              prijsvergelijking? Stuur dan een bericht naar{" "}
              <a href="mailto:info@vinylofy.com">info@vinylofy.com</a>.
            </p>

            <p>
              Suggesties zijn welkom. Ze helpen om Vinylofy stap voor stap
              vollediger en bruikbaarder te maken.
            </p>
          </div>
        </div>
      </main>

      <SiteFooter />
    </div>
  );
}
