WITH SAMPLING AS (
  SELECT
    'nightly' AS chan,
    'Linux' AS sos,
    1 AS NBUCKS
  UNION ALL
  SELECT
    'nightly' AS chan,
    'Darwin' AS sos,
    1 AS NBUCKS
  UNION ALL
  SELECT
    'nightly' AS chan,
    'Windows_NT' AS sos,
    1 AS NBUCKS
  UNION ALL
  SELECT
    'beta' AS chan,
    'Linux' AS sos,
    1 AS NBUCKS
  UNION ALL
  SELECT
    'beta' AS chan,
    'Darwin' AS sos,
    1 AS NBUCKS
  UNION ALL
  SELECT
    'beta' AS chan,
    'Windows_NT' AS sos,
    1 AS NBUCKS
  UNION ALL
  SELECT
    'release' AS chan,
    'Linux' AS sos,
    1 AS NBUCKS
  UNION ALL
  SELECT
    'release' AS chan,
    'Darwin' AS sos,
    1 AS NBUCKS
  UNION ALL
  SELECT
    'release' AS chan,
    'Windows_NT' AS sos,
    3 AS NBUCKS
),
a1 AS (
  SELECT
    --- TOTAL USAGE ON FIREFOX
    submission_date_s3 AS date,
    os,
    sum(active_hours_sum) AS usage_all,
    count(DISTINCT(client_id)) AS dau_all
  FROM
    telemetry.clients_daily_v6 HH
  LEFT JOIN
    SAMPLING ON HH.os = SAMPLING.sos
    AND HH.normalized_channel = SAMPLING.chan
  WHERE
    submission_date_s3 >= '{current_version_release}'
    AND submission_date_s3 <= DATE_ADD(
      DATE '{current_version_release}',
      INTERVAL {nday} DAY
    )
    AND os IN ('Linux', 'Windows_NT', 'Darwin')
    -- {linux_os_comment} AND os = 'Linux'
    AND app_name = 'Firefox'
    AND normalized_channel = '{norm_channel}'
    AND MOD(
      ABS(FARM_FINGERPRINT(MD5(client_id))),
      SAMPLING.NBUCKS
    ) = 0
  GROUP BY
    1,
    2
),
a2 AS (
  --- TOTAL USAGE ON FIREFOX ON LATEST VERSION
  --- THIS AND THE ABOVE ARE USED FOR COMPUTING 'NVC'
  SELECT
    submission_date_s3 AS date,
    os,
    sum(active_hours_sum) AS usage_cversion,
    count(DISTINCT(client_id)) AS dau_cversion
  FROM
    telemetry.clients_daily_v6 HH
  LEFT JOIN
    SAMPLING ON HH.os = SAMPLING.sos
    AND HH.normalized_channel = SAMPLING.chan
  WHERE
    submission_date_s3 >= '{current_version_release}'
    AND submission_date_s3 <= DATE_ADD(
      DATE '{current_version_release}',
      INTERVAL {nday} DAY
    )
    AND os IN ('Linux', 'Windows_NT', 'Darwin')
    -- {linux_os_comment} AND os = 'Linux'
    AND app_name = 'Firefox'
    AND normalized_channel = '{norm_channel}' -- and profile_creation_date>=12418 and profile_creation_date<=20089
    AND {app_version_field} = '{current_version}'
    -- {linux_release_cdaily_build_id}
    AND MOD(
      ABS(FARM_FINGERPRINT(MD5(client_id))),
      SAMPLING.NBUCKS
    ) = 0
  GROUP BY
    1,
    2
),
A AS (
  SELECT
    a1.date,
    a1.os,
    a1.usage_all,
    a1.dau_all,
    a2.usage_cversion,
    a2.dau_cversion
  FROM
    a1
  JOIN
    a2 ON a1.date = a2.date
    AND a1.os = a2.os
),
b1 AS (
  --- Total Crashes on Current Version
  --- NEED CLIENT_ID TO JOIN on DAILY TO GET CRASH RATE
  SELECT
    client_id,
    submission_date AS date,
    os_name AS os,
    sum(
      case
        WHEN payload.processType IS NULL
        OR payload.processType = 'main' THEN 1
        ELSE 0
      end
    ) AS cmain,
    sum(
      case
        WHEN payload.processType = 'content'
        AND (
          udf.get_key(payload.metadata, 'ipc_channel_error') IS NULL
          OR (
            udf.get_key(payload.metadata, 'ipc_channel_error') IS NOT NULL
            AND udf.get_key(payload.metadata, 'ipc_channel_error') != 'ShutdownKill'
          )
        ) THEN 1
        ELSE 0
      end
    ) AS ccontent
  FROM
    {crash_src} JJ
  LEFT JOIN
    SAMPLING ON JJ.os_name = SAMPLING.sos
    AND JJ.normalized_channel = SAMPLING.chan
  WHERE
    submission_date >= '{current_version_release}'
    AND submission_date <= DATE_ADD(
      DATE '{current_version_release}',
      INTERVAL {nday} DAY
    )
    AND os_name IN ('Linux', 'Windows_NT', 'Darwin')
    -- {linux_os_comment} AND os_name = 'Linux'
    AND application = 'Firefox'
    AND normalized_channel = '{norm_channel}'
    AND {build_version_field} IN ({current_version_crash})
    -- {linux_release_crash_build_id}
    AND profile_created >= 12418
    AND profile_created <= 20089
    AND MOD(
      ABS(FARM_FINGERPRINT(MD5(client_id))),
      SAMPLING.NBUCKS
    ) = 0
  GROUP BY
    1,
    2,
    3
),
--- TOTAL HOURS FROM FOLKS WHO CRASHED
--- TO COMPUTE CRASH RATE
b2 AS (
  SELECT
    client_id,
    submission_date_s3 AS date,
    os AS os,
    sum(active_hours_sum) AS usage,
    sum(coalesce(crashes_detected_plugin_sum, 0)) AS cplugin
  FROM
    telemetry.clients_daily_v6 HH
  LEFT JOIN
    SAMPLING ON HH.os = SAMPLING.sos
    AND HH.normalized_channel = SAMPLING.chan
  WHERE
    submission_date_s3 >= '{current_version_release}'
    AND submission_date_s3 <= DATE_ADD(
      DATE '{current_version_release}',
      INTERVAL {nday} DAY
    )
    AND os IN ('Linux', 'Windows_NT', 'Darwin')
    -- {linux_os_comment} AND os = 'Linux'
    AND app_name = 'Firefox'
    AND normalized_channel = '{norm_channel}'
    AND {app_version_field} = '{current_version}' -- and profile_creation_date>=12418 and profile_creation_date<=20089
    -- {linux_release_cdaily_build_id}
    AND MOD(
      ABS(FARM_FINGERPRINT(MD5(client_id))),
      SAMPLING.NBUCKS
    ) = 0
  GROUP BY
    1,
    2,
    3
),
b AS (
  SELECT
    b1.date,
    b1.os,
    count(
      DISTINCT(
        case
          WHEN b1.cmain > 0 THEN b1.client_id
          ELSE NULL
        end
      )
    ) AS dau_cm_crasher_cversion,
    count(
      DISTINCT(
        case
          WHEN b1.ccontent > 0 THEN b1.client_id
          ELSE NULL
        end
      )
    ) AS dau_cc_crasher_cversion,
    count(
      DISTINCT(
        case
          WHEN b2.cplugin > 0 THEN b1.client_id
          ELSE NULL
        end
      )
    ) AS dau_cp_crasher_cversion,
    count(
      DISTINCT(
        case
          WHEN (
            b1.cmain > 0
            OR b1.ccontent > 0
            OR b2.cplugin > 0
          ) THEN b1.client_id
          ELSE NULL
        end
      )
    ) AS dau_call_crasher_cversion,
    sum(
      case
        WHEN b1.cmain > 0 THEN b2.usage
        ELSE 0
      end
    ) AS usage_cm_crasher_cversion,
    sum(
      case
        WHEN b1.ccontent > 0 THEN b2.usage
        ELSE 0
      end
    ) AS usage_cc_crasher_cversion,
    sum(
      case
        WHEN b2.cplugin > 0 THEN b2.usage
        ELSE 0
      end
    ) AS usage_cp_crasher_cversion,
    sum(
      case
        WHEN (
          b1.cmain > 0
          OR b1.ccontent > 0
          OR b2.cplugin > 0
        ) THEN b2.usage
        ELSE 0
      end
    ) AS usage_call_crasher_cversion,
    sum(cmain) AS cmain,
    sum(ccontent) AS ccontent,
    sum(cplugin) AS cplugin,
    sum(cmain) + sum(ccontent) + sum(cplugin) AS call
  FROM
    b1
  JOIN
    b2 ON b1.client_id = b2.client_id
    AND b1.os = b2.os
    AND b1.date = b2.date
  WHERE
    (b1.ccontent + b1.cmain) < 350 -- see https://sql.telemetry.mozilla.org/queries/64354/source
  GROUP BY
    1,
    2
),
d AS (
  SELECT
    A.date,
    A.os,
    A.usage_all,
    A.dau_all,
    A.usage_cversion,
    A.dau_cversion,
    b.dau_cm_crasher_cversion,
    b.dau_cc_crasher_cversion,
    b.dau_cp_crasher_cversion,
    b.dau_call_crasher_cversion,
    b.usage_cm_crasher_cversion,
    b.usage_cc_crasher_cversion,
    b.usage_cp_crasher_cversion,
    b.usage_call_crasher_cversion,
    b.cmain,
    b.ccontent,
    b.cplugin,
    b.call
  FROM
    b
  JOIN
    A ON b.date = A.date
    AND b.os = A.os
)
SELECT
  *
FROM
  d
ORDER BY
  os,
  date