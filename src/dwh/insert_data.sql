create or replace function data.insert_data(etl_object jsonb) returns jsonb
    language plpgsql
as
$$
declare
    etl_object jsonb;
begin
    raise notice '%', 'a';
end;
$$;