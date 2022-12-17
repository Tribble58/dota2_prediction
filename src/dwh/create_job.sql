create or replace function service.create_job(etl_json jsonb) returns jsonb
    language plpgsql
as
$$
declare
    etl             jsonb;
    start_time      text;
    job_uid         uuid;
    etl_json_output jsonb;

begin
    start_time := to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');
    etl := json_build_object(
            'stage', 'job',
            'start_time', start_time,
            'end_time', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),
            'data', '[]'::jsonb,
            'logs', json_build_object('Try: 1', 'Job initialized successfully'),
            'meta', '{}'::jsonb
        );
    insert into service.jobs(load_date)
    values (start_time::timestamp, etl_json_output)
    returning uid into job_uid;

    return job_uid;
end;
$$;