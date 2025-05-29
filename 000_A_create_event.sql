CREATE OR REPLACE TABLE `steam-mantis-108908.WPA_Events.TABLE NAME` --*(1)
PARTITION BY DATE(Date)
CLUSTER BY Key_Interface_Brand AS
(
SELECT

--"SESSION LEVEL" COLUMNS
CAST(wpa.property_id AS INT64) AS property_id
,date
,FARM_FINGERPRINT(CONCAT(wpa.user_pseudo_id, ga_session_id)) AS session_id
,FARM_FINGERPRINT(wpa.user_pseudo_id) AS user_pseudo_id
,GUID_Event
,ga_session_number AS Key_ga_session_number
,CAST(session_engaged AS INT64) AS Key_session_engaged
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(wpa.Interface_Brand, ''), 'Unknown')), 100000)) AS Key_Interface_Brand
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(Customer_Status_Start, ''), 'Unknown')), 100000)) AS Key_Customer_Status_Start
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(geo.city, ''), 'Unknown'))) AS Key_city
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(geo.country, ''), 'Unknown')), 100000)) AS Key_country
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(geo.region, ''), 'Unknown'))) AS Key_region
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(Technical_PlatformUsed, ''), 'Unknown'))) AS Key_Technical_PlatformUsed
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.mobile_brand_name, ''), 'Unknown'))) AS Key_mobile_brand_name
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.web_info.browser, ''), 'Unknown'))) AS Key_device_web_info_browser
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.web_info.browser_version, ''), 'Unknown'))) AS Key_device_web_info_browser_version
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.category, ''), 'Unknown'))) AS Key_device_category
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.mobile_marketing_name, ''), 'Unknown'))) AS Key_mobile_marketing_name
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.mobile_model_name, ''), 'Unknown'))) AS Key_mobile_model_name
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.operating_system, ''), 'Unknown'))) AS Key_device_operating_system
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.operating_system_version, ''), 'Unknown'))) AS Key_device_operating_system_version

--TRAFFIC COLUMNS
,lnd.lnd_source
,lnd.lnd_medium
,collected_traffic_source.manual_source as lc_source
,collected_traffic_source.manual_medium as lc_medium
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(lnd.channel_grouping, ''), 'Unknown')), 100000)) AS Key_channel_grouping

--GLOBAL DIMENSIONS
,Content_Group
,event_name
,EventAction
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(Customer_Status_Event, ''), 'Unknown')), 100000)) AS Key_Customer_Status_Event
,device.web_info.hostname AS device_web_info_hostname
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(Login_Status, ''), 'Unknown')), 100000)) AS Key_Login_Status
,page_location
,page_referrer
,page_title
,Technical_EventName
,Technical_PlatformName
,Technical_ScreenOrientation
,Technical_ScreenResolution
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(User_CustomerLevel, ''), 'Unknown')), 100000)) AS Key_User_CustomerLevel
,Sub_Area
--EVENT SPECIFIC DIMENSIONS *(2)
,Registration_Type                /*EXAMPLE FOR NRC*/
,User_Reg_Method                  /*EXAMPLE FOR NRC*/
,User_Reg_Step                    /*EXAMPLE FOR NRC*/
--CUSTOM DIMENSIONS
,CAST(ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF((COMMERCIAL_AREA), ''), 'Unknown')), 100000))AS INT64) AS Key_COMMERCIAL_AREA

--METRICS
,COUNT(*) AS Event_Count

FROM 
--*(3)
(SELECT * FROM steam-mantis-108908.WPA.270389480 UNION ALL -- ALOHASHARK GA4 [WEB] 
SELECT * FROM steam-mantis-108908.WPA.345721586 UNION ALL -- BETBONANZA GA4 [WEB] 
SELECT * FROM steam-mantis-108908.WPA.282392101 UNION ALL -- BETS10 GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.326546621 UNION ALL -- BETSAFE ESTONIA GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.326546361 UNION ALL -- BETSAFE KENYA GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.326547182 UNION ALL -- BETSAFE LATVIA GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.326497634 UNION ALL -- BETSAFE LITHUANIA GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.344917462 UNION ALL -- BETSAFE PERU GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.270369193 UNION ALL -- BETSAFE.COM GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.323032716 UNION ALL -- BETSSON ARGENTINA GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.369344384 UNION ALL -- BETSSON BELGIUM GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.326671725 UNION ALL -- BETSSON BRAZIL GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.326660191 UNION ALL -- BETSSON COLOMBIA GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.326408170 UNION ALL -- BETSSON CZECH [WEB]
SELECT * FROM steam-mantis-108908.WPA.326649423 UNION ALL -- BETSSON GREECE GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.270372273 UNION ALL -- BETSSON IT GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.326629578 UNION ALL -- BETSSON MEXICO GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.326650420 UNION ALL -- BETSSON NETHERLANDS GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.344980665 UNION ALL -- BETSSON PERU GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.326624957 UNION ALL -- BETSSON SPAIN GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.270382730 UNION ALL -- BETSSON.COM GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.435048955 UNION ALL -- BETSSON.KZ GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.216367908 UNION ALL -- BETSSONDK GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.270372101 UNION ALL -- CASINOEURO GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.282543507 UNION ALL -- CasinoMaxi GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.282510694 UNION ALL -- CasinoMetropol GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.270352833 UNION ALL -- CASINOWINNER GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.270382267 UNION ALL -- EUROCASINO GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.270412437 UNION ALL -- EUROPEBET GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.347097957 UNION ALL -- GUTS GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.346483255 UNION ALL -- INKABET GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.270410004 UNION ALL -- JALLACASINO SE GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.326628458 UNION ALL -- JALLACASINO.EE (ex SUPERCASINO.EE) GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.346955191 UNION ALL -- KABOO GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.270391072 UNION ALL -- LIVEROULETTE GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.270400311 UNION ALL -- LOYALCASINO GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.282401733 UNION ALL -- MOBILBAHIS GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.326507697 UNION ALL -- NORDICBET DK GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.270380201 UNION ALL -- NORDICBET.COM GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.346480383 UNION ALL -- NORGESAUTOMATEN GA4 [WEB] 
SELECT * FROM steam-mantis-108908.WPA.270354118 UNION ALL -- RACEBETS GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.347130828 UNION ALL -- RIZK POLAND GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.347145362 UNION ALL -- RIZK.COM GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.347121847 UNION ALL -- RIZK.HR GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.347189984 UNION ALL -- RIZK.RS GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.346938887 UNION ALL -- RIZKSLOTS.DE GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.216807831 UNION ALL -- STARCASINO GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.270355736 UNION ALL -- SUPERCASINO.COM GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.346970928 UNION ALL -- THRILLS.COM GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.462436475 UNION ALL -- BETSSON PARAGUAY [WEB]
SELECT * FROM steam-mantis-108908.WPA.479948234 UNION ALL -- CRASHCASINO GA4 [WEB]
SELECT * FROM steam-mantis-108908.WPA.479302236 -- BETSSON UK GA4 [WEB]
) wpa

--MAPPING WITH COMMERCIAL AREA
LEFT JOIN `steam-mantis-108908.WPA_Keys.map_COMMERCIAL_AREA` ca
ON wpa.interface_brand = ca.interface_brand
AND wpa.geo.country=ca.country

--MAPPING WITH LND TRAFFIC SOURCE
LEFT JOIN 
(SELECT session_id,MAX(lnd_source) as lnd_source,MAX(lnd_medium) as lnd_medium,MAX(channel_grouping) as channel_grouping 
FROM `steam-mantis-108908.WPA_Events.00_LastNonDirectTraffic`
WHERE date <= '2025-05-01' --*(4)
GROUP BY session_id) lnd
ON FARM_FINGERPRINT(CONCAT(wpa.user_pseudo_id, ga_session_id)) = lnd.session_id

--DATE FILTER
WHERE wpa.date <= '2025-05-01' --*(4)

--EVENT FILTERS *(5)
AND Event_Name='Registration_Funnel'    /*EXAMPLE FOR NRC*/
AND EventAction='NRC'                   /*EXAMPLE FOR NRC*/

GROUP BY
--SESSION LEVEL COLUMNS
property_id,date,FARM_FINGERPRINT(CONCAT(wpa.user_pseudo_id, ga_session_id)),FARM_FINGERPRINT(wpa.user_pseudo_id), GUID_Event, ga_session_number,session_engaged,wpa.Interface_Brand,Customer_Status_Start,geo.city,geo.country,geo.region,device.mobile_brand_name,device.web_info.browser,device.web_info.browser_version,device.category,device.mobile_marketing_name,device.mobile_model_name,device.operating_system,device.operating_system_version
--TRAFFIC
,lnd_source,lnd_medium,collected_traffic_source.manual_source,collected_traffic_source.manual_medium,lnd.channel_grouping
--GLOBAL DIMENSIONS
,Content_Group,event_name,EventAction,Customer_Status_Event,device.web_info.hostname,Login_Status,page_location,page_referrer,page_title,Technical_EventName,Technical_PlatformName,Technical_PlatformUsed, Technical_ScreenOrientation,Technical_ScreenResolution,User_CustomerLevel,Sub_Area
--CUSTOM DIMENSIONS
,COMMERCIAL_AREA
--EVENT SPECIFIC DIMENSIONS *(6)
,Registration_Type                /*EXAMPLE FOR NRC*/
,User_Reg_Method                  /*EXAMPLE FOR NRC*/
,User_Reg_Step                    /*EXAMPLE FOR NRC*/
)
