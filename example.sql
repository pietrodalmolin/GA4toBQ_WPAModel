CREATE OR REPLACE TABLE `steam-mantis-108908.WPA_Events.ComponentInteraction` --*(1)
PARTITION BY DATE(Date)
CLUSTER BY Key_Interface_Brand AS

WITH 
AtB AS (
SELECT DISTINCT
date
,FARM_FINGERPRINT(CONCAT(user_pseudo_id, ga_session_id)) AS session_id
FROM `steam-mantis-108908.WPA_Events.AllProperties`
WHERE Event_Name='Sportsbook' AND STARTS_WITH(EventAction, 'SB Widget Click - Add')
AND date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
),
BC AS (
SELECT DISTINCT
date
,FARM_FINGERPRINT(CONCAT(user_pseudo_id, ga_session_id)) AS session_id
FROM `steam-mantis-108908.WPA_Events.AllProperties`
WHERE Event_Name='Sportsbook' AND EventAction='Bet Confirmed'
AND date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
)

SELECT
--"SESSION LEVEL" COLUMNS
/*1*/   CAST(wpa.property_id AS INT64) AS property_id
/*2*/   ,wpa.date
/*3*/   ,FARM_FINGERPRINT(CONCAT(wpa.user_pseudo_id, ga_session_id)) AS session_id
/*4*/   ,FARM_FINGERPRINT(wpa.user_pseudo_id) AS user_pseudo_id
/*5*/   ,GUID_Event
/*6*/   ,ga_session_number AS Key_ga_session_number
/*7*/   ,CAST(session_engaged AS INT64) AS Key_session_engaged
/*8*/   ,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(wpa.Interface_Brand, ''), 'Unknown')), 100000)) AS Key_Interface_Brand
/*9*/   ,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(Customer_Status_Start, ''), 'Unknown')), 100000)) AS Key_Customer_Status_Start
/*10*/  ,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(geo.city, ''), 'Unknown'))) AS Key_city
/*11*/  ,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(geo.country, ''), 'Unknown')), 100000)) AS Key_country
/*12*/  ,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(geo.region, ''), 'Unknown'))) AS Key_region
/*13*/  ,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(Technical_PlatformUsed, ''), 'Unknown'))) AS Key_Technical_PlatformUsed
/*14*/  ,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.mobile_brand_name, ''), 'Unknown'))) AS Key_mobile_brand_name
/*15*/  ,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.web_info.browser, ''), 'Unknown'))) AS Key_device_web_info_browser
/*16*/  ,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.web_info.browser_version, ''), 'Unknown'))) AS Key_device_web_info_browser_version
/*17*/  ,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.category, ''), 'Unknown'))) AS Key_device_category
/*18*/  ,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.mobile_marketing_name, ''), 'Unknown'))) AS Key_mobile_marketing_name
/*19*/  ,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.mobile_model_name, ''), 'Unknown'))) AS Key_mobile_model_name
/*20*/  ,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.operating_system, ''), 'Unknown'))) AS Key_device_operating_system
/*21*/  ,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.operating_system_version, ''), 'Unknown'))) AS Key_device_operating_system_version
--GLOBAL DIMENSIONS
/*22*/  ,Content_Group
/*23*/  ,event_name
/*24*/  ,EventAction
/*25*/  ,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(Customer_Status_Event, ''), 'Unknown')), 100000)) AS Key_Customer_Status_Event
/*26*/  ,device.web_info.hostname AS device_web_info_hostname
/*27*/  ,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(Login_Status, ''), 'Unknown')), 100000)) AS Key_Login_Status
/*28*/  ,page_location
/*29*/  ,page_referrer
/*30*/  ,page_title
/*31*/  ,Technical_EventName
/*32*/  ,Technical_PlatformName
/*33*/  ,Technical_ScreenOrientation
/*34*/  ,Technical_ScreenResolution
/*35*/  ,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(User_CustomerLevel, ''), 'Unknown')), 100000)) AS Key_User_CustomerLevel
/*36*/  ,Sub_Area
--COMMERCIAL AREA
/*37*/  ,CAST(ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF((COMMERCIAL_AREA), ''), 'Unknown')), 100000))AS INT64) AS Key_COMMERCIAL_AREA
--EVENT SPECIFIC DIMENSIONS
/*38*/  ,Interface_Component
/*39*/  ,Interface_SectionName
--SEGMENTS
/*40*/  ,CASE WHEN AtB.session_id IS NOT NULL THEN 1 ELSE 0 END AS Segment_AddtoBetslip
/*41*/  ,CASE WHEN BC.session_id IS NOT NULL THEN 1 ELSE 0 END AS Segment_BetConfirmed
--CALCULATED COLUMNS
,CASE 
  WHEN EventAction='Quick Links Scroller' THEN 'QuickLinksScroller'
  WHEN EventAction='Sportsbook Banner Click' THEN 'SportsbookBannerClick'
  WHEN EventAction='Create Coupon Submit' THEN 'CreateCouponSubmit'
  WHEN STARTS_WITH(EventAction, 'SubCategory Click') THEN 'SubCategoryClick'
  ELSE 'Others' END AS Event
,CASE 
  WHEN STARTS_WITH(EventAction, 'SubCategory Click') AND STRPOS(interface_component, ' -') > 0 THEN 
    SAFE_CAST(SUBSTR(interface_component, 1, STRPOS(interface_component, ' -') - 1) AS INT64) 
  ELSE NULL 
END AS CategoryID,

CASE 
  WHEN STARTS_WITH(EventAction, 'SubCategory Click') AND STRPOS(interface_component, ' - ') > 0 THEN 
    ARRAY_REVERSE(SPLIT(SUBSTR(interface_component, STRPOS(interface_component, ' - ') + 3), '.'))[OFFSET(0)] 
  ELSE NULL 
END AS SubCategoryID


--METRICS
,COUNT(*) AS Event_Count

FROM `steam-mantis-108908.WPA_Events.AllProperties` wpa

--MAPPING WITH COMMERCIAL AREA
LEFT JOIN `steam-mantis-108908.WPA_Keys.map_COMMERCIAL_AREA` ca
ON wpa.interface_brand = ca.interface_brand
AND wpa.geo.country=ca.country

--SEGMENTS
LEFT JOIN AtB ON AtB.date = wpa.date AND AtB.session_id = FARM_FINGERPRINT(CONCAT(wpa.user_pseudo_id, ga_session_id))
LEFT JOIN BC ON BC.date = wpa.date AND BC.session_id = FARM_FINGERPRINT(CONCAT(wpa.user_pseudo_id, ga_session_id))

WHERE
--EVENT FILTERS
Event_Name='Sportsbook' --filter event_name
AND (EventAction IN ("Quick Links Scroller", "Sportsbook Banner Click", "Create Coupon Submit") OR STARTS_WITH(EventAction, 'SubCategory Click'))
--DATE FILTER
AND wpa.date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41
