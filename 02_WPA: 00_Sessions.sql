CREATE OR REPLACE TABLE `steam-mantis-108908.WPA_Events.00_Sessions`
PARTITION BY DATE(Date)
CLUSTER BY Key_Interface_Brand, IsSportsbook, IsGaming AS
(
SELECT
--KEYS
CAST(wpa.property_id AS INT64) AS property_id
,wpa.date
,FARM_FINGERPRINT(CONCAT(wpa.user_pseudo_id, ga_session_id)) AS session_id
,FARM_FINGERPRINT(wpa.user_pseudo_id) AS user_pseudo_id
--TRAFFIC
,MAX(lnd.lnd_source) as lnd_source
,MAX(lnd.lnd_medium) as lnd_medium
,MAX(collected_traffic_source.manual_source) as lc_source
,MAX(collected_traffic_source.manual_medium) as lc_medium
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(lnd.channel_grouping), ''), 'Unknown')), 100000)) AS Key_channel_grouping
--GLOBAL DIMENSIONS
,MIN(ga_session_number) AS Key_ga_session_number
,CAST(MAX(session_engaged) AS INT64) AS Key_session_engaged
,TIMESTAMP_DIFF(TIMESTAMP_MICROS(MAX(event_timestamp)),TIMESTAMP_MICROS(MIN(event_timestamp)), SECOND) AS session_length
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(Interface_Brand), ''), 'Unknown')), 100000)) AS Key_Interface_Brand
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(CASE WHEN event_name = 'session_start' THEN Customer_Status_Start END), ''), 'Unknown')), 100000)) AS Key_Customer_Status_Start
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(CASE WHEN event_name = 'session_start' THEN geo.city END), ''), 'Unknown'))) AS Key_city
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(CASE WHEN event_name = 'session_start' THEN geo.country END), ''), 'Unknown')), 100000)) AS Key_country
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(CASE WHEN event_name = 'session_start' THEN geo.region END), ''), 'Unknown'))) AS Key_region
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(CASE WHEN event_name = 'session_start' THEN device.mobile_brand_name END), ''), 'Unknown'))) AS Key_mobile_brand_name
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(CASE WHEN event_name = 'session_start' THEN device.web_info.browser END), ''), 'Unknown'))) AS Key_device_web_info_browser
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(CASE WHEN event_name = 'session_start' THEN device.web_info.browser_version END), ''), 'Unknown'))) AS Key_device_web_info_browser_version
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(CASE WHEN event_name = 'session_start' THEN device.category END), ''), 'Unknown'))) AS Key_device_category
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(CASE WHEN event_name = 'session_start' THEN device.mobile_marketing_name END), ''), 'Unknown'))) AS Key_mobile_marketing_name
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(CASE WHEN event_name = 'session_start' THEN device.mobile_model_name END), ''), 'Unknown'))) AS Key_mobile_model_name
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(CASE WHEN event_name = 'session_start' THEN device.operating_system END), ''), 'Unknown'))) AS Key_device_operating_system
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(MAX(CASE WHEN event_name = 'session_start' THEN device.operating_system_version END), ''), 'Unknown'))) AS Key_device_operating_system_version
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF((CASE WHEN MAX(CASE WHEN Login_Status = 'LoggedIn' THEN 1 ELSE 0 END) = 1 THEN 'LoggedIn' ELSE 'LoggedOut' END), ''), 'Unknown')), 100000)) AS Key_Login_Status
--VERTICAL SPLIT
,CASE WHEN MAX(CASE WHEN REGEXP_CONTAINS(content_group, r'(CASINO|LIVE_CASINO|JACKPOTS|SLOTS)') THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END AS IsGaming
,CASE WHEN MAX(CASE WHEN event_name LIKE '%Sportsbook%' OR Content_Group LIKE '%SPORTSBOOK%' THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END AS IsSportsbook
--CASINO SPECIFIC DIMENSIONS
,CASE WHEN MAX(CASE WHEN LOWER(Content_Group) = 'casino' THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END AS IsCasino
,CASE WHEN MAX(CASE WHEN LOWER(Content_Group) = 'live_casino' THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END AS IsLiveCasino
,CASE WHEN MAX(CASE WHEN LOWER(Content_Group) = 'jackpots' THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END AS IsJackpots
,CASE WHEN MAX(CASE WHEN LOWER(Sub_Area) = 'product lobby page' THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END AS isProductLobby
,CASE WHEN MAX(CASE WHEN LOWER(Sub_Area) = 'game categories' THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END AS isGameCategories
,CASE WHEN MAX(CASE WHEN LOWER(EventAction) = 'game played for real' THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END AS IsReal
,CASE WHEN MAX(CASE WHEN LOWER(EventAction) = 'game played for fun' THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END AS IsForFun

FROM `steam-mantis-108908.WPA.*` wpa
LEFT JOIN 
(SELECT session_id,lnd_source,lnd_medium,channel_grouping FROM `steam-mantis-108908.WPA_Events.00_LastNonDirectTraffic`

GROUP BY session_id,lnd_source,lnd_medium,channel_grouping) lnd ON FARM_FINGERPRINT(CONCAT(wpa.user_pseudo_id, ga_session_id)) = lnd.session_id

GROUP BY
wpa.date, wpa.property_id, session_id, wpa.user_pseudo_id
)
