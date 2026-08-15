-- Follow the Groove v1 schema rollback.
--
-- This permanently removes Follow-the-Groove artists, aliases, edges,
-- evidence, similarity, product links and collection-run audit data.
-- Existing Vinylofy products, prices, shops and all other commercial data are
-- neither modified nor removed. Child objects are removed before parents.

begin;

drop table if exists public.product_artists;
drop table if exists public.artist_similarity;
drop table if exists public.artist_relation_evidence;
drop table if exists public.artist_edges;
drop table if exists public.artist_aliases;
drop table if exists public.artists;
drop table if exists public.follow_the_groove_collection_runs;
drop function if exists public.ftg_canonical_uuid_array(uuid[]);

commit;
