CREATE OR REPLACE TABLE `steam-mantis-108908.WPA_Tables.00_LastNonDirectTraffic`
      PARTITION BY DATE(date)
      CLUSTER BY property_id
AS
(
with last_non_direct as 
(SELECT
  property_id
  ,date
  ,session_id
  ,user_pseudo_id
  ,ifnull(
    session_first_traffic_source,
    last_value(session_last_traffic_source ignore nulls) over(
      partition by user_pseudo_id
      order by
        ga_session_id range between 2592000 preceding
        and 1 preceding -- 30 day lookback
    )
    ) as session_traffic_source_last_non_direct
FROM
(SELECT
  property_id
  ,date
  ,CONCAT(user_pseudo_id,ga_session_id) AS session_id
  ,ga_session_id
  ,user_pseudo_id

  ,ARRAY_AGG(IF(collected_traffic_source IS NOT NULL,
      (SELECT AS STRUCT collected_traffic_source.* EXCEPT(manual_source, manual_medium),
      IF(collected_traffic_source.gclid IS NOT NULL, 'google', collected_traffic_source.manual_source) AS manual_source,
      IF(collected_traffic_source.gclid IS NOT NULL, 'cpc', collected_traffic_source.manual_medium) AS manual_medium
      ),NULL) ORDER BY event_timestamp ASC LIMIT 1) [SAFE_OFFSET(0)] 
    AS session_first_traffic_source
  
  ,ARRAY_AGG(IF(collected_traffic_source IS NOT NULL,
      (SELECT AS STRUCT collected_traffic_source.* EXCEPT(manual_source, manual_medium),
      IF(collected_traffic_source.gclid IS NOT NULL, 'google', collected_traffic_source.manual_source) AS manual_source,
      IF(collected_traffic_source.gclid IS NOT NULL, 'cpc', collected_traffic_source.manual_medium) AS manual_medium
      ),NULL) IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] 
    AS session_last_traffic_source

FROM `steam-mantis-108908.WPA.*` -- Replace with your actual source table name.

WHERE event_name NOT IN ('session_start', 'first_visit')
  AND CONCAT(user_pseudo_id,ga_session_id) IS NOT NULL -- Ensure valid sessions.

GROUP BY property_id, date, session_id, ga_session_id, user_pseudo_id)
)

SELECT
  property_id
  ,date
  ,session_id
  ,user_pseudo_id
  ,ifnull(session_traffic_source_last_non_direct.manual_source, '(direct)') as lnd_source
  ,ifnull(session_traffic_source_last_non_direct.manual_medium, '(none)') as lnd_medium
FROM
  last_non_direct
)