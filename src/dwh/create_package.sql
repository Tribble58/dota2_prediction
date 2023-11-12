create or replace function data.create_package(etl_json jsonb) returns jsonb
    language plpgsql
as
$$
declare
    stage             text;
    data              jsonb;
    logs              jsonb;
    table_name        text;
    data_package_size numeric;
    job_uid           uuid;
    data_length       numeric;
    data_package      jsonb;
    packages_number   numeric;
    job_id            bigint;
    lower_boundary    numeric;
    upper_boundary    numeric;
    etl_json_item     jsonb;
    start_time_item   text;
    package_uid       uuid;
    etl_json_output   jsonb;
begin
    stage := etl_json #>> '{stage}';
    data := etl_json #>> '{data}';
    logs := etl_json #>> '{logs}';
    table_name := etl_json #>> '{meta, table_name}';
    data_package_size := etl_json #>> '{meta, data_package_size}';
    job_uid := etl_json #>> '{meta, job_uid}';
    data_length := jsonb_array_length(data);
    packages_number := div(data_length, data_package_size);

    -- Calculate the number of packages: if the length of data divided by data_package_size
    -- equals zero then we remain the division result else add 1 (for remaining data of last package)

    if mod(data_length, data_package_size) <> 0
    then
        packages_number := packages_number + 1;
    end if;


    -- Get the id of job that was created during ETL session for this particular data

    select id into job_id from service.jobs where uid = job_uid;

    etl_json_output := '[]'::jsonb;
    for package_number in 0..packages_number - 1
        loop

            start_time_item := to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');
            lower_boundary := package_number * data_package_size;
            upper_boundary := lower_boundary + data_package_size - 1;

            data_package := jsonb_path_query_array(data,
                                                   format('$[%s to %s]', lower_boundary::text, upper_boundary::text)::jsonpath);

            insert into service.packages(package, job_id, load_date)
            values (data_package, job_id, start_time_item::timestamp)
            returning uid into package_uid;

            etl_json_item := json_build_object(
                    'stage', 'package',
                    'start_time', start_time_item,
                    'end_time', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),
                    'data', data_package,
                    'logs', 'Data package created successfully',
                    'meta', json_build_object(
                            'table_name', table_name,
                            'package_uid', package_uid,
                            'package_number', package_number,
                            'packages_total', packages_number
                        )
                );

            etl_json_output := etl_json_output || etl_json_item;
        end loop;
    delete
    from service.packages
    where id in (
        select id
        from service.packages
        where load_date < current_date - 60
    );

    -- Delete data that is 60 days older than current transaction

    return etl_json_output;

end;
$$;

-- do
-- $$
--     declare
--         etl               jsonb;
--         stage             text;
--         data              jsonb;
--         logs              jsonb;
--         table_name        text;
--         data_package_size numeric;
--         job_uid           uuid;
--         data_length       numeric;
--         data_package      jsonb;
--         packages_number   numeric;
--         job_id            bigint;
--         lower_boundary    numeric;
--         upper_boundary    numeric;
--         etl_item          jsonb;
--         start_time_item   text;
--         package_uid       uuid;
--         etl_output        jsonb;
--     begin
--         select test_column::jsonb into etl from test.test where id = 3;
--         stage := etl #>> '{stage}';
--         data := etl #>> '{data}';
--         logs := etl #>> '{logs}';
--         table_name := etl #>> '{meta, table_name}';
--         data_package_size := etl #>> '{meta, data_package_size}';
--         job_uid := etl #>> '{meta, job_uid}';
--         data_length := jsonb_array_length(data);
--         packages_number := div(data_length, data_package_size);
--
--         -- Calculate the number of packages: if the length of data divided by data_package_size
--         -- equals zero then we remain the division result else add 1 (for remaining data of last package)
--
--         if mod(data_length, data_package_size) <> 0
--         then
--             packages_number := packages_number + 1;
--         end if;
--
--         -- Get the id of job that was created during ETL session for this particular data
--
--         select id into job_id from service.jobs where uid = job_uid;
--
--         etl_output := '[]'::jsonb;
--         for package_number in 0..packages_number - 1
--             loop
--
--                 start_time_item := to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');
--                 lower_boundary := package_number * data_package_size;
--                 upper_boundary := lower_boundary + data_package_size - 1;
--
--                 data_package := jsonb_path_query_array(data,
--                                                        format('$[%s to %s]', lower_boundary::text, upper_boundary::text)::jsonpath);
--
--                 insert into service.packages(package, job_id, load_date)
--                 values (data_package, job_id, start_time_item::timestamp)
--                 returning uid into package_uid;
--
--                 etl_item := json_build_object(
--                         'stage', 'package',
--                         'start_time', start_time_item,
--                         'end_time', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),
--                         'data', data_package,
--                         'logs', 'Data package created successfully',
--                         'meta', json_build_object(
--                                 'table_name', table_name,
--                                 'package_uid', package_uid,
--                                 'package_number', package_number,
--                                 'packages_total', packages_number
--                             )
--                     );
--
--                 etl_output := etl_output || etl_item;
--                 raise notice '%', etl_output;
--
--             end loop;
--
--         -- Delete data that is 60 days older than current transaction
--
--         delete
--         from service.packages
--         where id in (
--             select id
--             from service.packages
--             where load_date < current_date - 60
--         );
--
--
--     end;
-- $$;