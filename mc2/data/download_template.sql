-- change current_version_release -> cur_vers_release_date
CREATE TEMP FUNCTION os_chan_buckets(os string, chan string) as (
  case (os, chan) when ('Windows_NT', 'release') then 3
    else 1 end
);

with usage_base as (
SELECT
    --- TOTAL USAGE ON FIREFOX
    submission_date_s3 as date,
    os,
    active_hours_sum,
    client_id,
    -- version will be determined by value filled into
    -- template variable `app_version_field`.
    -- One of app_build_id, displ
    app_version,         -- release
    app_display_version, -- beta
    app_build_id         -- nightly
  FROM
    `moz-fx-data-shared-prod`.telemetry.clients_daily
  WHERE
    submission_date_s3 >= '{cur_vers_release_date}'
    AND submission_date_s3 <= DATE_ADD(
      DATE '{cur_vers_release_date}',
      INTERVAL {nday} DAY
    )
    AND os IN ('Linux', 'Windows_NT', 'Darwin')
    AND app_name = 'Firefox'
    AND normalized_channel = '{norm_channel}'
    AND MOD(
      ABS(FARM_FINGERPRINT(MD5(client_id))),
      os_chan_buckets(os, normalized_channel)
    ) = 0
),

crashes_base as (
  --- Total Crashes on Current Version
  --- NEED CLIENT_ID TO JOIN on DAILY TO GET CRASH RATE
  SELECT
    client_id,
    date(submission_timestamp) as date,
    environment.system.os.name as os,
    payload
  FROM
    `moz-fx-data-shared-prod`.telemetry.crash
  WHERE
    date(submission_timestamp) >= '{cur_vers_release_date}'
    AND date(submission_timestamp) <= DATE_ADD(
      DATE '{cur_vers_release_date}',
      INTERVAL {nday} DAY
    )
    AND environment.system.os.name IN ('Linux', 'Windows_NT', 'Darwin')
    AND application.name = 'Firefox'
    AND normalized_channel = '{norm_channel}'
    AND {crash_build_version_field} IN ({current_version_crash})
    -- aka 12418
    AND environment.profile.creation_date >= UNIX_DATE(date('2004-01-01'))
    -- no more than 2 days into the future
    AND environment.profile.creation_date <= UNIX_DATE(current_date()) + 2
    AND MOD(
      ABS(FARM_FINGERPRINT(MD5(client_id))),
      os_chan_buckets(environment.system.os.name, normalized_channel)
    ) = 0
),

--- TOTAL USAGE ON FIREFOX
u1 as (
  SELECT
    date,
    os,
    sum(active_hours_sum) as usage_all,
    count(distinct(client_id)) as dau_all
  FROM
    usage_base
  GROUP BY 1, 2
),

--- TOTAL USAGE ON FIREFOX ON LATEST VERSION
--- THIS AND THE ABOVE ARE USED FOR COMPUTING 'NVC'
u2 as (
  SELECT
    date,
    os,
    sum(active_hours_sum) as usage_cversion,
    count(distinct(client_id)) as dau_cversion
  FROM
    usage_base
  WHERE
    {app_version_field} = '{current_version}'
  GROUP BY 1, 2
),

usage as (
  SELECT
    u1.date,
    u1.os,
    u1.usage_all,
    u1.dau_all,
    u2.usage_cversion,
    u2.dau_cversion
  FROM
    u1
  JOIN
    u2 ON u1.date = u2.date
    AND u1.os = u2.os
),

crashes as (
  --- Total Crashes on Current Version
  --- NEED CLIENT_ID TO JOIN on DAILY TO GET CRASH RATE
  SELECT
    client_id,
    date,
    os,
    -- main crash definition at
    -- https://github.com/mozilla/bigquery-etl/blob/
    -- 072e4af3b8245c2fc7f4ba5d4c5a87bf10c9c107/sql/
    -- telemetry_derived/error_aggregates/query.sql#L100
    countif(payload.process_type = 'main'
            or payload.process_type is null) as cmain,
    -- content crash definition at
    -- https://github.com/mozilla/bigquery-etl/blob/
    -- 072e4af3b8245c2fc7f4ba5d4c5a87bf10c9c107/sql/
    -- telemetry_derived/error_aggregates/query.sql#L106
    countif(regexp_contains(payload.process_type, 'content')
            and not regexp_contains(
                coalesce(payload.metadata.ipc_channel_error, ''),
                'ShutDownKill')
    ) as ccontent
  FROM
    crashes_base
  GROUP BY 1, 2, 3
),

--- TOTAL HOURS FROM FOLKS WHO CRASHED
--- TO COMPUTE CRASH RATE
crasher_usage as (
  SELECT
    client_id,
    date,
    os,
    sum(active_hours_sum) as usage
  FROM
    usage_base
  WHERE
    {app_version_field} = '{current_version}'
  GROUP BY 1, 2, 3
),

crashes_nvc as (
  SELECT
    cr.date,
    cr.os,
    count(distinct(
        if(cr.cmain > 0, cr.client_id, null)
    )) as dau_cm_crasher_cversion,
    count(distinct(
        if(cr.ccontent > 0, cr.client_id, null)
    )) as dau_cc_crasher_cversion,
    count(distinct(
        if(cr.cmain > 0 or cr.ccontent > 0, cr.client_id, null)
    )) as dau_call_crasher_cversion,
    sum(if(cr.cmain > 0, cu.usage, 0)) as usage_cm_crasher_cversion,
    sum(if(cr.ccontent > 0, cu.usage, 0)) as usage_cc_crasher_cversion,
    sum(if(cr.cmain > 0 or cr.ccontent > 0, cu.usage, 0)) as usage_call_crasher_cversion,
    sum(cmain) as cmain,
    sum(ccontent) as ccontent,
    sum(cmain) + sum(ccontent) as call
  FROM
    crashes cr
  JOIN
    crasher_usage cu ON cr.client_id = cu.client_id
    AND cr.os = cu.os
    AND cr.date = cu.date
  WHERE
    -- see https://sql.telemetry.mozilla.org/queries/64354/source
    (cr.ccontent + cr.cmain) < 350
  GROUP BY 1, 2
),

res as (
  SELECT
    u.date,
    u.os,
    u.usage_all,
    u.dau_all,
    u.usage_cversion,
    u.dau_cversion,
    c.dau_cm_crasher_cversion,
    c.dau_cc_crasher_cversion,
    c.dau_call_crasher_cversion,
    c.usage_cm_crasher_cversion,
    c.usage_cc_crasher_cversion,
    c.usage_call_crasher_cversion,
    c.cmain,
    c.ccontent,
    c.call
  FROM
    crashes_nvc c
  JOIN
    usage u ON c.date = u.date
    AND c.os = u.os
)

SELECT *
FROM res
ORDER BY os, date
