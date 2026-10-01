begin;

-- Use only if the Belgian public release must be withdrawn.
update public.markets
set status = 'prepared', released_at = null
where country_code = 'BE' and status = 'active';

commit;
