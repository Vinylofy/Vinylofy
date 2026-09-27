insert into public.shops (name, domain, country, is_active)
values
  ('HHV', 'hhv.de', 'DE', true),
  ('Juno', 'junorecords.com', 'NL', true),
  ('Bol', 'bol.com', 'NL', true),
  ('Platomania', 'platomania.nl', 'NL', true),
  ('Sounds Delft', 'soundsdelft.nl', 'NL', true)
on conflict (domain) do nothing;
