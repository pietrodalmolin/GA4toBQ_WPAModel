INSERT INTO `steam-mantis-108908.WPA_Events.00_LastNonDirectTraffic` (
SELECT 
*, 
CASE
WHEN lnd_source = '(direct)' AND lnd_medium = '(none)' THEN 'Direct'

-- Paid Search condition
WHEN (
REGEXP_CONTAINS(LOWER(lnd_source), r'(ppc|ma_ppc|google_ads|bing_ads|apple_search|others_ads|cpc)')
OR (
REGEXP_CONTAINS(LOWER(lnd_source), r'(baidu|bing|duckduckgo|ecosia|google|yahoo|yandex)')
AND REGEXP_CONTAINS(LOWER(lnd_medium), r'(cp|ppc|ads|paid)')
)
OR REGEXP_CONTAINS(LOWER(lnd_medium), r'(ppc|paid|cpc|ads)')
) THEN 'Paid Search'

-- Paid Social condition
WHEN (
(
REGEXP_CONTAINS(LOWER(lnd_source), r'(facebook|instagram|linkedin|pinterest|tiktok|twitter|whatsapp)')
AND REGEXP_CONTAINS(LOWER(lnd_medium), r'(cp|ppc|soc|cpm|paid)')
)
OR REGEXP_CONTAINS(LOWER(lnd_medium), r'(soc)')
) THEN 'Paid Social'

-- Display condition
WHEN (
lnd_medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm')
OR REGEXP_CONTAINS(LOWER(lnd_medium), r'(dir|digitalmarketing)')
) THEN 'Display'

-- Programmatic condition
WHEN (
REGEXP_CONTAINS(LOWER(lnd_medium), r'(adm|pgm|dis)')
) THEN 'Programmatic'

-- Organic Search condition
WHEN (
(
REGEXP_CONTAINS(LOWER(lnd_source), r'(baidu|bing|duckduckgo|ecosia|google|yahoo|yandex)')
AND REGEXP_CONTAINS(LOWER(lnd_medium), r'(organic|seo)')
)
OR REGEXP_CONTAINS(LOWER(lnd_medium), r'(seo|ma_seo)')
) THEN 'Organic Search'

-- Direct condition
WHEN lnd_source = '(direct)' AND lnd_medium = '(none)' THEN 'Direct'

-- Affiliates condition
WHEN (
LOWER(lnd_medium) = 'affiliate'
OR REGEXP_CONTAINS(LOWER(lnd_medium), r'affiliate')
) THEN 'Affiliates'

-- Fallback (Others)
ELSE 'Others'
END AS Channel_Grouping
FROM 
(
WITH last_non_direct AS (
SELECT 
property_id, 
date, 
session_id, 
user_pseudo_id, 
IFNULL(
session_first_traffic_source, 
LAST_VALUE(
session_last_traffic_source IGNORE NULLS
) OVER (
PARTITION BY user_pseudo_id 
ORDER BY 
ga_session_id RANGE BETWEEN 2592000 PRECEDING 
AND 1 PRECEDING -- 30-day lookback
)
) AS session_traffic_source_last_non_direct 
FROM 
(
SELECT 
CAST(property_id AS INT64) AS property_id, 
date, 
FARM_FINGERPRINT(
CONCAT(user_pseudo_id, ga_session_id)
) AS session_id, 
ga_session_id, 
FARM_FINGERPRINT(user_pseudo_id) AS user_pseudo_id, 
ARRAY_AGG(
IF(
collected_traffic_source IS NOT NULL, 
(
SELECT 
AS STRUCT collected_traffic_source.* 
EXCEPT 
(manual_source, manual_medium), 
IF(
collected_traffic_source.gclid IS NOT NULL, 
'google', collected_traffic_source.manual_source
) AS manual_source, 
IF(
collected_traffic_source.gclid IS NOT NULL, 
'cpc', collected_traffic_source.manual_medium
) AS manual_medium
), 
NULL
) 
ORDER BY 
event_timestamp ASC 
LIMIT 
1
) [SAFE_OFFSET(0) ] AS session_first_traffic_source, 
ARRAY_AGG(
IF(
collected_traffic_source IS NOT NULL, 
(
SELECT 
AS STRUCT collected_traffic_source.* 
EXCEPT 
(manual_source, manual_medium), 
IF(
collected_traffic_source.gclid IS NOT NULL, 
'google', collected_traffic_source.manual_source
) AS manual_source, 
IF(
collected_traffic_source.gclid IS NOT NULL, 
'cpc', collected_traffic_source.manual_medium
) AS manual_medium
), 
NULL
) IGNORE NULLS 
ORDER BY 
event_timestamp DESC 
LIMIT 
1
) [SAFE_OFFSET(0) ] AS session_last_traffic_source 
FROM 
`steam-mantis-108908.WPA.*` -- Replace with actual source table.
WHERE 
event_name NOT IN ('session_start', 'first_visit') 
AND CONCAT(user_pseudo_id, ga_session_id) IS NOT NULL 
AND DATE BETWEEN TIMESTAMP(
DATE_SUB(
CURRENT_DATE(), 
INTERVAL 32 DAY
)
) 
AND TIMESTAMP(
DATE_SUB(
CURRENT_DATE(), 
INTERVAL 2 DAY
)
) -- Ensure 30-day lookback
GROUP BY 
property_id, 
date, 
session_id, 
ga_session_id, 
user_pseudo_id
)
) 
SELECT 
property_id, 
date, 
session_id, 
user_pseudo_id, 
IFNULL(
session_traffic_source_last_non_direct.manual_source, 
'(direct)'
) AS lnd_source, 
IFNULL(
session_traffic_source_last_non_direct.manual_medium, 
'(none)'
) AS lnd_medium 
FROM 
last_non_direct 
WHERE 
date = TIMESTAMP(
DATE_SUB(
CURRENT_DATE(), 
INTERVAL 2 DAY
)
) -- Only insert sessions from 2 days ago
)
)
