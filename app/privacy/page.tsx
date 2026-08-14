import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

export default function PrivacyPage() {
  return (
    <div className="min-h-screen bg-white text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-4xl px-6 py-12">
        <div className="rounded-3xl border border-neutral-200 bg-white p-8 md:p-10">
          <p className="text-sm font-medium uppercase tracking-[0.14em] text-orange-600">
            Privacy
          </p>

          <h1 className="mt-4 text-3xl font-semibold tracking-tight md:text-4xl">
            Privacyverklaring
          </h1>

          <div className="mt-8 space-y-5 text-neutral-600">
            <p>
              Vinylofy is opgezet om vinylprijzen te kunnen vergelijken zonder dat
              je een account hoeft aan te maken of onnodig persoonlijke gegevens
              hoeft achter te laten.
            </p>

            <p>
              Bij het bezoeken van de website kunnen beperkte technische gegevens
              worden verwerkt die nodig zijn om Vinylofy goed, veilig en
              betrouwbaar te laten werken. Denk aan serververzoeken, technische
              foutmeldingen en algemene informatie over het gebruik van de website.
              Deze gegevens worden gebruikt voor beheer, beveiliging,
              probleemoplossing en het verbeteren van Vinylofy.
            </p>

            <p>
              Op dit moment biedt Vinylofy geen persoonlijke accounts,
              verlanglijsten of prijsalerts aan. Vinylofy verkoopt geen
              persoonlijke gegevens.
            </p>

            <p>
              Wanneer je via een link op Vinylofy naar een externe winkel gaat,
              verlaat je de website van Vinylofy. De betreffende winkel verwerkt
              gegevens volgens het eigen privacy- en cookiebeleid. Vinylofy heeft
              geen invloed op de manier waarop externe winkels met jouw gegevens
              omgaan.
            </p>

            <p>
              Stuur je een e-mail naar Vinylofy, dan worden je e-mailadres en de
              inhoud van je bericht alleen gebruikt om je vraag, suggestie of
              verzoek te behandelen.
            </p>

            <p>
              Wanneer Vinylofy in de toekomst wordt uitgebreid met functies zoals
              accounts, verlanglijsten, prijsalerts of aanvullende
              analysemogelijkheden, wordt deze privacyverklaring hierop aangepast.
            </p>

            <p>
              Heb je een vraag over privacy of de verwerking van gegevens? Neem dan
              contact op via{" "}
              <a href="mailto:info@vinylofy.com">info@vinylofy.com</a>.
            </p>

            <p>Laatst bijgewerkt: juli 2026.</p>
          </div>
        </div>
      </main>

      <SiteFooter />
    </div>
  );
}