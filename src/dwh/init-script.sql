create schema if not exists service;

create extension if not exists "uuid-ossp";

create table if not exists service.mapping
(
    table_name_source text,
    field_source      text,
    table_name_dwh    text,
    field_dwh         text,
    unique_field      text,
    transformation    text
);

create table if not exists service.jobs
(
    id         bigserial
        constraint jobs_pk
            primary key,
    uid        uuid default uuid_generate_v4(),
    code       text,
    start_time timestamp without time zone
);

create unique index if not exists jobs_id_uindex
    on service.jobs (id);

create unique index if not exists jobs_uid_uindex
    on service.jobs (uid);

create table if not exists service.packages
(
    id         serial
        constraint packages_pk
            primary key,
    uid        uuid default uuid_generate_v4(),
    job_id     bigint not null
        constraint packages_jobs_id_fk
            references service.jobs,
    table_name text,
    package    jsonb
);

create unique index if not exists packages_id_uindex
    on service.packages (id);

create unique index if not exists packages_uid_uindex
    on service.packages (uid);

create table if not exists service.objects
(
    id         serial
        constraint objects_pk
            primary key,
    package_id bigint not null
        constraint objects_packages_id_fk
            references service.packages,
    table_name text,
    object     jsonb
);

create unique index if not exists objects_id_uindex
    on service.objects (id);

create or replace function service.create_job(code text) returns text
    language plpgsql
as
$$
declare
    start_time timestamp without time zone;
    job_uid    uuid;
begin
    start_time := to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');

    insert into service.jobs(code, start_time)
    values (code, start_time)
    returning uid into job_uid;

    return job_uid;
end;
$$;

create or replace function service.create_package(input_json jsonb)
    returns jsonb
    language plpgsql
as
$$
declare
    data              jsonb;
    logs              jsonb;
    meta              jsonb;
    table_name        text;
    data_package_size numeric;
    job_uid           uuid;
    data_length       numeric;
    data_package      jsonb;
    packages_number   numeric;
    job_id            bigint;
    lower_boundary    numeric;
    upper_boundary    numeric;
    output_json_item  jsonb;
    package_uid       uuid;
    output_json       jsonb;
begin
    data := input_json #>> '{data}';
    logs := input_json #>> '{logs}';
    meta := input_json #>> '{meta}';
    job_uid := input_json #>> '{meta, job_uid}';
    table_name := input_json #>> '{meta, table_name}';
    data_package_size := input_json #>> '{meta, data_package_size}';
    raise notice 'Data package size is %', data_package_size;
    data_length := jsonb_array_length(data);
    raise notice 'Data size is %', data_length;
    packages_number := div(data_length, data_package_size);

    select id into job_id from service.jobs where uid = job_uid;

    -- Calculate number of packages: if the length of data divided by data_package_size
    -- equals zero then we remain the division result else add 1 (for remaining data of last package)

    if mod(data_length, data_package_size) <> 0
    then
        packages_number := packages_number + 1;
    end if;

    raise notice 'Number of packages is %', packages_number;

    output_json := '[]'::jsonb;

    for package_number in 0..packages_number - 1
        loop

            lower_boundary := package_number * data_package_size;
            upper_boundary := lower_boundary + data_package_size - 1;

            data_package := jsonb_path_query_array(data,
                                                   format('$[%s to %s]', lower_boundary::text, upper_boundary::text)::jsonpath);

            insert into service.packages(job_id, table_name, package)
            values (job_id, table_name, data_package)
            returning uid into package_uid;

            output_json_item := json_build_object(
                    'data', data_package,
                    'logs', 'Data package created successfully',
                    'meta', json_build_object(
                            'table_name', table_name,
                            'job_uid', job_uid,
                            'package_uid', package_uid
                            )
                                );

            output_json := output_json || output_json_item;
        end loop;

    -- Delete data that is 30 days older than current transaction
--     delete
--     from service.packages
--     where id in (select id
--                  from service.packages
--                  where load_date < current_date - 30);

    return '[]'::jsonb;

end;
$$;

create or replace function service.create_mapping() returns jsonb
    language plpgsql
as
$$
declare
    output jsonb;
begin
    select jsonb_agg(mapping)
    into output
    from (select distinct jsonb_build_object('table_name_source', table_name_source,
                                             'table_name_dwh', table_name_dwh,
                                             'fields',
                                             jsonb_object_agg(field_source, field_destination)
                                             over (partition by table_name_source),
                                             'unique_field', unique_field,
                                             'transformations', jsonb_object_agg(field_source, transformation)
                                                                over (partition by table_name_source)) "mapping",
                          priority
          from (select jsonb_build_object(field_source, field_dwh)   fields_mapping,
                       jsonb_build_object(field_dwh, transformation) fields_transformation,
                       table_name_source,
                       table_name_dwh,
                       unique_field,
                       case
                           when table_name_dwh like '%_dict%' then '1'
                           when table_name_dwh like '%_dim%' then '2'
                           else '3' end                              priority
                from service.mapping
                group by table_name_source, table_name_dwh, fields_mapping, unique_field,
                         priority, fields_transformation) foo,
               jsonb_each(fields_mapping) as fields_mapping(field_source, field_destination),
               jsonb_each(fields_transformation) as fields_transformation(field_dwh, transformation)
          order by priority asc) foo1;
    return output;
end ;
$$;

create or replace procedure service.insert_data(inserted_table_name text, package_uid uuid)
    language plpgsql
as
$$
declare
    package_json jsonb;
begin

    raise notice 'Processing package with uuid %', package_uid;

    select package
    into package_json
    from service.packages
    where uid = package_uid;

    if package_json is not null then
        raise notice 'Package found!';
    else
        raise notice 'Package not found!';
    end if;

    raise notice 'Processing %', inserted_table_name;

    if inserted_table_name in ('heroes', 'hero_stats') then

        insert into data.heroes_dict(hero_id, localized_name, primary_attr, pro_ban, pro_win, pro_pick)
        select *
        from jsonb_to_recordset(package_json) as x(id bigint, localized_name text, primary_attr text, pro_ban int,
                                                   pro_win int, pro_pick int)
        on conflict (hero_id) do update set hero_id        = excluded.hero_id,
                                            localized_name = excluded.localized_name,
                                            primary_attr   = excluded.primary_attr,
                                            pro_ban        = excluded.pro_ban,
                                            pro_win        = excluded.pro_win,
                                            pro_pick       = excluded.pro_pick;

    elsif inserted_table_name = 'heroes_matchups' then

        insert into data.heroes_matchups_dim(hero_id, hero_against_id, games_played, wins)
        select hd.id  as hero_id,
               hda.id as hero_against_id,
               games_played,
               wins
        from jsonb_to_recordset(package_json) as x(hero_id int, hero_against_id int, games_played int, wins int)
                 join data.heroes_dict hd on x.hero_id = hd.hero_id
                 join data.heroes_dict hda on x.hero_against_id = hda.hero_id
        on conflict (hero_id, hero_against_id) do update set games_played = excluded.games_played,
                                                             wins         = excluded.wins;

    elsif inserted_table_name = 'leagues' then

        insert into data.leagues_dict(league_id, tier, name)
        select *
        from jsonb_to_recordset(package_json) as x(leagueid bigint, tier text, name text)
        on conflict (league_id) do update set league_id = excluded.league_id,
                                              tier      = excluded.tier,
                                              name      = excluded.name;

    elsif inserted_table_name = 'pro_matches' then
        -- Insert match to pro_matches_fact table and matches_dim table in single transaction
        with data as (select *
                      from jsonb_to_recordset(package_json) as x(match_id bigint, duration int, start_time bigint,
                                                                 radiant_team_id bigint, dire_team_id bigint,
                                                                 is_pick boolean, "order" bigint,
                                                                 team_id bigint, leagueid bigint, series_type int,
                                                                 radiant_score int, dire_score int, radiant_win boolean,
                                                                 first_blood_time numeric, radiant_gold_adv jsonb,
                                                                 radiant_xp_adv jsonb, patch numeric, region numeric,
                                                                 hero_id bigint, picks_bans jsonb)),
             insert_pro_matches as (insert into data.pro_matches_fact (match_id, duration, start_time, radiant_team_id,
                                                                       dire_team_id, league_id,
                                                                       series_type, radiant_score, dire_score,
                                                                       radiant_win)
                 select match_id,
                        duration,
                        to_timestamp(start_time),
                        radiant.id,
                        dire.id,
                        ld.id,
                        series_type,
                        radiant_score,
                        dire_score,
                        radiant_win
                 from data
                          left join data.teams_dict radiant on data.radiant_team_id = radiant.team_id
                          left join data.teams_dict dire on data.dire_team_id = dire.team_id
                          left join data.leagues_dict ld on data.leagueid = ld.league_id
                 on conflict (match_id) do update set match_id = excluded.match_id,
                     duration = excluded.duration,
                     start_time = excluded.start_time,
                     radiant_team_id = excluded.radiant_team_id,
                     dire_team_id = excluded.dire_team_id,
                     league_id = excluded.league_id,
                     series_type = excluded.series_type,
                     radiant_score = excluded.radiant_score,
                     dire_score = excluded.dire_score,
                     radiant_win = excluded.radiant_win returning id, match_id)
        insert
        into data.matches_dim(match_id, first_blood_time, league_id, radiant_gold_adv, radiant_xp_adv, patch, region)
        select imp.id,
               first_blood_time,
               ld.id,
               radiant_gold_adv,
               radiant_xp_adv,
               patch,
               region
        from data
                 join insert_pro_matches imp on data.match_id = imp.match_id
                 left join data.leagues_dict ld on data.leagueid = ld.league_id
        on conflict (match_id) do update set first_blood_time = excluded.first_blood_time,
                                             league_id        = excluded.league_id,
                                             radiant_gold_adv = excluded.radiant_gold_adv,
                                             radiant_xp_adv   = excluded.radiant_xp_adv,
                                             patch            = excluded.patch,
                                             region           = excluded.region;

    elsif inserted_table_name = 'pro_players' then

        insert into data.pro_players_dict(account_id, nick, full_history_time, fh_unavailable, name, team_id)
        select account_id, personaname, full_history_time, fh_unavailable, x.name, td.id
        from jsonb_to_recordset(package_json) as x(account_id bigint, personaname text, full_history_time timestamp,
                                                   fh_unavailable boolean, name text, team_id bigint)
                 left join data.teams_dict td on x.team_id = td.team_id
        on conflict (account_id) do update set account_id        = excluded.account_id,
                                               nick              = excluded.nick,
                                               full_history_time = excluded.full_history_time,
                                               fh_unavailable    = excluded.fh_unavailable,
                                               name              = excluded.name,
                                               team_id           = excluded.team_id;

    elsif inserted_table_name = 'pro_players_heroes' then

        insert into data.pro_players_heroes_dim(account_id, hero_id, last_played, games, win)
        select ppd.id as account_id, hd.id as hero_id, last_played, games, win
        from jsonb_to_recordset(package_json) as x(account_id bigint, hero_id bigint, last_played numeric, games int,
                                                   win int)
                 left join data.heroes_dict hd on x.hero_id = hd.hero_id
                 left join data.pro_players_dict ppd on x.account_id = ppd.account_id
        on conflict (account_id, hero_id) do update set last_played = excluded.last_played,
                                                        games       = excluded.games,
                                                        win         = excluded.win;

    elsif inserted_table_name = 'teams' then

        insert into data.teams_dict(team_id, rating, wins, losses, name)
        select *
        from jsonb_to_recordset(package_json) as x(team_id bigint, rating numeric, wins int, losses int, name text)
        on conflict (team_id) do update set team_id = excluded.team_id,
                                            rating  = excluded.rating,
                                            wins    = excluded.wins,
                                            losses  = excluded.losses,
                                            name    = excluded.name;

    elsif inserted_table_name = 'picks_bans' then

        insert into data.picks_bans_dim(match_id, is_pick, hero_id, team, ord)
        select pmf.id, is_pick, hd.id, team, ord
        from jsonb_to_recordset(package_json) as x(match_id bigint, is_pick boolean, hero_id integer, team integer,
                                                   ord integer)
                 left join data.pro_matches_fact pmf on x.match_id = pmf.match_id
                 left join data.heroes_dict hd on x.hero_id = hd.hero_id
        on conflict (match_id, hero_id) do update set is_pick = excluded.is_pick,
                                                      team    = excluded.team,
                                                      ord     = excluded.ord;

        --     else
--         raise EXCEPTION;
    end if;
exception
    when others
        then raise notice '%', SQLERRM;
end;
$$;

create or replace function service.insert_transformed_object(input_json jsonb) returns jsonb
    language plpgsql
as
$$
declare
    package_uid text;
    data        jsonb;
    meta        jsonb;
    data_length numeric;
    package_id  bigint;
    object      jsonb;
    table_name  text;
begin
    data := input_json #>> '{data}';
    meta := input_json #>> '{meta}';
    package_uid := meta #>> '{package_uid}';
    table_name := meta #>> '{table_name}';
    data_length := jsonb_array_length(data);

    select id
    into package_id
    from service.packages
    where uid::text = package_uid;

    for i in 0..data_length - 1
        loop
            object := data[i];
            insert into service.objects(package_id, table_name, object)
            values (package_id, table_name, object);

        end loop;
    return data;
end;
$$;

create schema if not exists data;

create table if not exists data.teams_dict
(
    id      bigserial
        constraint teams_dict_pk
            primary key,
    team_id bigint not null,
    rating  numeric,
    wins    numeric,
    losses  numeric,
    name    text
);

create unique index if not exists teams_dict_team_id_uindex
    on data.teams_dict (team_id);

create table if not exists data.pro_players_dict
(
    id                serial
        constraint pro_players_dict_pk
            primary key,
    account_id        bigint not null,
    nick              text,
    full_history_time timestamp,
    fh_unavailable    boolean,
    name              text,
    team_id           bigint
        constraint pro_players_dict_teams_dict_id_fk
            references data.teams_dict (id)
);

create unique index if not exists pro_players_dict_account_id_uindex
    on data.pro_players_dict (account_id);

create table if not exists data.leagues_dict
(
    id        serial
        constraint leagues_dict_pk
            primary key,
    league_id bigint not null,
    tier      text,
    name      text
);

create unique index if not exists leagues_dict_league_id_uindex
    on data.leagues_dict (league_id);

create table if not exists data.heroes_dict
(
    id             serial
        constraint heroes_dict_pk
            primary key,
    hero_id        bigint not null,
    localized_name text,
    primary_attr   text,
    pro_ban        numeric,
    pro_win        numeric,
    pro_pick       numeric
);

create unique index if not exists heroes_dict_hero_id_uindex
    on data.heroes_dict (hero_id);

create table if not exists data.pro_players_heroes_dim
(
    id          bigserial
        constraint pro_players_heroes_dim_pk
            primary key,
    account_id  bigint not null
        constraint pro_players_heroes_dim_pro_players_dict_account_id_fk
            references data.pro_players_dict (id),
    hero_id     bigint not null
        constraint pro_players_heroes_dim_heroes_dict_hero_id_fk
            references data.heroes_dict (id),
    last_played numeric,
    games       numeric,
    win         numeric
);

create unique index if not exists pro_players_heroes_dim_account_id_uindex
    on data.pro_players_heroes_dim (account_id, hero_id);

create table if not exists data.pro_matches_fact
(
    id              bigserial
        constraint pro_matches_fact_pk
            primary key,
    match_id        bigint,
    duration        numeric,
    start_time      bigint,
    radiant_team_id bigint
        constraint pro_matches_fact_teams_dict_team_id_fk
            references data.teams_dict (id),
    dire_team_id    bigint
        constraint pro_matches_fact_teams_dict_team_id_fk_2
            references data.teams_dict (id),
    league_id       bigint
        constraint pro_matches_fact_leagues_dict_league_id_fk
            references data.leagues_dict (id),
    series_type     numeric,
    radiant_score   numeric,
    dire_score      numeric,
    radiant_win     boolean
);

create unique index if not exists pro_matches_fact_match_id_uindex
    on data.pro_matches_fact (match_id);

create table if not exists data.matches_dim
(
    id               bigserial
        constraint matches_dim_pk
            primary key,
    match_id         bigint not null
        constraint matches_dim_pro_matches_match_id_fk
            references data.pro_matches_fact (id),
    first_blood_time numeric,
    league_id        bigint not null
        constraint matches_dim_leagues_dict_id_fk
            references data.leagues_dict (id),
    radiant_gold_adv jsonb,
    radiant_xp_adv   jsonb,
    patch            numeric,
    region           numeric
);

create unique index if not exists matches_dim_match_id_uindex
    on data.matches_dim (match_id);

create table if not exists data.heroes_matchups_dim
(
    id              bigserial
        constraint heroes_matchups_dim_pk
            primary key,
    hero_id         bigint not null
        constraint heroes_matchups_dim_heroes_dict_hero_id_fk
            references data.heroes_dict (id),
    hero_against_id bigint not null
        constraint heroes_matchups_dim_heroes_dict_hero_against_id_fk
            references data.heroes_dict (id),
    games_played    numeric,
    wins            numeric
);

create unique index if not exists heroes_matchups_dim__uindex
    on data.heroes_matchups_dim (hero_id, hero_against_id);

create table if not exists data.picks_bans_dim
(
    id       bigserial
        constraint picks_bans_dim_pk
            primary key,
    match_id bigint  not null
        constraint picks_bans_dim_pro_matches_fk
            references data.pro_matches_fact (id),
    is_pick  boolean not null,
    hero_id  bigint  not null
        constraint picks_bans_dim_heroes_dict_hero_id_fk
            references data.heroes_dict (id),
    team     integer not null,
    ord      integer not null
);

create index picks_bans_dim_match_id_index
    on data.picks_bans_dim (match_id);

create index picks_bans_dim_match_id_index
    on data.picks_bans_dim (match_id);

alter table data.picks_bans_dim
    add constraint picks_bans_dim_pk_2
        unique (match_id, hero_id);

create function data.insert_object(input_object jsonb) returns jsonb
    language plpgsql
as
$$
declare
    data              jsonb;
    table_name_source text;
    data_length       numeric;
    table_name_dwh    text;
    unique_field      text;
    keys              text;
    values            text;
    excluded          text;
    query             text;
    output            jsonb := '{}';
begin
    --     output = '[]'::jsonb;
    --     table_name_source := input_json #>> '{meta, table_name}';
--     data := input_json #>> '{data}';
--     data_length := jsonb_array_length(data);
--     raise notice 'Data length: %', data_length;

--     for i in 0..data_length - 1
--         loop
--             begin
--                 object := data[i];
    raise notice 'Object %', input_object;
    table_name_dwh := input_object #>> '{table_name_dwh}';
    unique_field := input_object #>> '{unique_field}';
    raise notice 'Table name: %', table_name_dwh;

    with foo as (select case
                            when value #>> '{table_name}' is null then replace(value::text, '"', '''')
                            else '(select id from data.' || (value #>> '{"table_name"}') || ' where ' ||
                                 (value #>> '{"field_name"}') ||
                                 ' = ' || (value #>> '{"value"}') || ')' end as value,
                        key
                 from jsonb_each(input_object)
                 where key not in ('table_name_dwh', 'unique_field'))
    select string_agg(key, ', '),
--                        replace(ltrim(rtrim(jsonb_agg(value)::text, ']'), '['), '"', ''''),
           string_agg(value, ', '),
           string_agg(key || ' = excluded.' || key, ', ')
    into keys, values, excluded
    from foo;

    select 'insert into data.' || table_name_dwh || '(' || keys || ') values (' || values || ')' ||
           ' on conflict(' || unique_field || ') do update set ' || excluded || ';'
    into query;

    raise notice 'Query: %', query;

    execute query;

    return '{
      "result": "Data was inserted successfully"
    }'::jsonb;

exception
    when others then
        raise notice '% %', SQLERRM, SQLSTATE;
        return jsonb_build_object('ERROR! ' || SQLERRM, SQLSTATE);
    -- end;
--         end loop;
end;
$$;

create schema if not exists logs;

create table if not exists logs.object_log
(
    id         bigserial
        constraint object_log_pk
            primary key,
    stage      text,
    date_start timestamp,
    date_end   timestamp,
    object     jsonb
);

create unique index if not exists object_log_id_uindex
    on logs.object_log (id);