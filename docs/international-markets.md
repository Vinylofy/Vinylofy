# Internationale markten: controle op 29 september 2026

De bestaande `shops.country` is het **vestigingsland**. `shop_shipping_destinations` bevat per shop en bestemmingsland de afzonderlijke verzendstatus, officiële bron-URL en controledatum. Alle bevestigde bronnen staan als afzonderlijke records in [de migratie](../supabase/migrations/20260929120000_prepare_international_markets.sql). Een lege of `unknown` status geeft geen recht op een publiek aanbod.

`C` = verzending bevestigd via de officiële shopsite; `?` = onbekend. Er is geen bestemming als `not_supported` vastgelegd, omdat daarvoor bij deze controle geen betrouwbaar expliciet negatief bewijs is gevonden. De tabel is een momentopname van actieve shops; de database is leidend voor de actuele status.

| Shopdomein | NL | BE | GB | US |
| --- | :---: | :---: | :---: | :---: |
| 3345.nl | C | C | ? | ? |
| atthemoviesshop.com | C | C | C | C |
| blackvinyl.nl | C | C | ? | ? |
| bobsvinyl.nl | C | C | ? | ? |
| bol.com | C | ? | ? | ? |
| cdhal.nl | C | C | ? | ? |
| colouredvinyl.nl | C | C | ? | ? |
| dgmoutlet.nl | C | C | ? | ? |
| eustore.everythingjazz.com | C | C | C | C |
| fiftiesstore.nl | C | C | ? | ? |
| getbackmusic.nl | C | ? | ? | ? |
| groovespin.nl | C | C | C | C |
| hhv.de | C | C | C | C |
| imusic.nl | C | C | ? | ? |
| jpc.de | C | C | C | C |
| junorecords.com | ? | ? | ? | ? |
| musiconvinyl.com | C | C | C | C |
| myrecordstore.nl | ? | ? | ? | ? |
| northendhaarlem.nl | C | ? | ? | ? |
| platenzaak.nl | C | ? | ? | ? |
| platomania.nl | C | ? | ? | ? |
| recordsonvinyl.nl | C | C | ? | ? |
| sounds-venlo.nl | C | C | C | ? |
| sounds.nl | ? | ? | ? | ? |
| soundsdelft.nl | ? | ? | ? | ? |
| soundshaarlem.nl | C | C | C | C |
| suburban.nl | C | C | C | C |
| variaworld.nl | C | C | ? | ? |
| viprecords.nl | ? | ? | ? | ? |

**Telling bij controle:** NL 24 bevestigd / 5 onbekend; BE 19 / 10; GB 9 / 20; US 8 / 21. De aparte geschiktheidsfunctie telde shops met ten minste één recent, publiceerbaar aanbod: NL 20, BE 16, GB 0, US 0. Alleen NL heeft marktstatus `active`. Aanbiedingen in een andere valuta dan de marktvaluta tellen niet mee; er vindt geen valutaconversie plaats. Aantallen kunnen door de 48-uursgrens veranderen.

## Bewuste vrijgave van een volgende markt

1. Controleer de officiële verzendbronnen opnieuw en werk uitsluitend onderbouwde bestemmingsrecords bij, met URL en `verified_at`. Laat twijfel `unknown`.
2. Controleer `select * from public.market_release_readiness_v1('BE');` met een beheerdersrol. `eligible_shop_count` moet minstens 10 zijn. De functie telt afzonderlijke actieve shops met bevestigd vervoer en recent aanbod dat aan de bestaande EAN-, prijs-, voorraad- en formaatregels voldoet.
3. Controleer de publieke pagina's, productprijzen en valuta voor die markt. Schrijf de bewuste vrijgave als afzonderlijke migratie die `markets.status = 'active'` en `released_at` zet. De database weigert de eerste vrijgave onder 10 shops.
4. Controleer na de vrijgave regelmatig `market_release_readiness_v1`. Een actieve markt onder 10 blijft actief, maar `active = true and ready = false` is een interne terugval die onderzocht moet worden. Ongeldige aanbiedingen verdwijnen door de actuele marktfilters direct uit de site. Heeft een buitenlandse markt helemaal geen koopbaar aanbod meer, dan verdwijnt zij tijdelijk uit de publieke navigatie en valt een directe URL terug op NL; de databasestatus blijft `active`.

Er zijn geen buitenlandse scrapers toegevoegd. De huidige shops en prijspipeline blijven leidend; nieuwe shops volgen later het bestaande importercontract.
