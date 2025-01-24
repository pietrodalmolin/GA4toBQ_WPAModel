INSERT INTO `steam-mantis-108908.WPA_Tables.00_LastNonDirectTraffic`
(
WITH last_non_direct AS (
  SELECT
    property_id,
    date,
    session_id,
    user_pseudo_id,
    IFNULL(
      session_first_traffic_source,
      LAST_VALUE(session_last_traffic_source IGNORE NULLS) OVER (
        PARTITION BY user_pseudo_id
        ORDER BY ga_session_id
        RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING -- 30-day lookback
      )
    ) AS session_traffic_source_last_non_direct
  FROM (
    SELECT
      property_id,
      date,
      CONCAT(user_pseudo_id, ga_session_id) AS session_id,
      ga_session_id,
      user_pseudo_id,

      ARRAY_AGG(
        IF(collected_traffic_source IS NOT NULL,
          (SELECT AS STRUCT collected_traffic_source.* EXCEPT(manual_source, manual_medium),
            IF(collected_traffic_source.gclid IS NOT NULL, 'google', collected_traffic_source.manual_source) AS manual_source,
            IF(collected_traffic_source.gclid IS NOT NULL, 'cpc', collected_traffic_source.manual_medium) AS manual_medium
          ), NULL)
        ORDER BY event_timestamp ASC LIMIT 1
      ) [SAFE_OFFSET(0)] AS session_first_traffic_source,

      ARRAY_AGG(
        IF(collected_traffic_source IS NOT NULL,
          (SELECT AS STRUCT collected_traffic_source.* EXCEPT(manual_source, manual_medium),
            IF(collected_traffic_source.gclid IS NOT NULL, 'google', collected_traffic_source.manual_source) AS manual_source,
            IF(collected_traffic_source.gclid IS NOT NULL, 'cpc', collected_traffic_source.manual_medium) AS manual_medium
          ), NULL)
        IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1
      ) [SAFE_OFFSET(0)] AS session_last_traffic_source

    FROM `steam-mantis-108908.WPA.*` -- Replace with actual source table.

    WHERE event_name NOT IN ('session_start', 'first_visit')
      AND CONCAT(user_pseudo_id, ga_session_id) IS NOT NULL
      AND DATE BETWEEN TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 32 DAY)) 
                   AND TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)) -- Ensure 30-day lookback

    GROUP BY property_id, date, session_id, ga_session_id, user_pseudo_id
  )
)

SELECT
  property_id,
  date,
  session_id,
  user_pseudo_id,
  IFNULL(session_traffic_source_last_non_direct.manual_source, '(direct)') AS lnd_source,
  IFNULL(session_traffic_source_last_non_direct.manual_medium, '(none)') AS lnd_medium
FROM last_non_direct
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)) -- Only insert sessions from 2 days ago
);
