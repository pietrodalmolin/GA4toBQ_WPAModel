CREATE OR REPLACE TABLE `steam-mantis-108908.WPA_Tables.CA_NRC`
PARTITION BY DATE(Date)
CLUSTER BY property_id, event_name, eventaction AS
 (
 SELECT
--SESSION LEVEL COLUMNS
property_id,date,CONCAT(user_pseudo_id, ga_session_id) AS session_id,user_pseudo_id,ga_session_number,session_engaged,Interface_Brand,Customer_Status_Start,geo.city AS city,geo.country AS country,geo.region AS region,device.mobile_brand_name AS mobile_brand_name,device.web_info.browser AS device_web_info_browser,device.web_info.browser_version AS device_web_info_browser_version,device.category AS device_category,device.mobile_marketing_name AS mobile_marketing_name,device.mobile_model_name AS mobile_model_name,device.operating_system AS device_operating_system,device.operating_system_version AS device_operating_system_version
--TRAFFIC
,lnd.lnd_source,lnd.lnd_medium,collected_traffic_source.manual_source as lc_source,collected_traffic_source.manual_medium as lc_medium,lnd.channel_grouping
--GLOBAL DIMENSIONS
,Content_Group,event_name,EventAction,Customer_Status_Event,device.web_info.hostname AS device_web_info_hostname,Login_Status,page_location,page_referrer,page_title,Technical_EventName,Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,Registration_Type
,User_Reg_Method
,User_Reg_Step
--METRICS
,COUNT(*) AS Event_Count

FROM(
SELECT * FROM steam-mantis-108908.WPA.270389480 UNION ALL SELECT * FROM steam-mantis-108908.WPA.345721586 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282392101 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326546621 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326546361 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326547182 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326497634 UNION ALL SELECT * FROM steam-mantis-108908.WPA.344917462 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270369193 UNION ALL SELECT * FROM steam-mantis-108908.WPA.323032716 UNION ALL SELECT * FROM steam-mantis-108908.WPA.369344384 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326671725 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326660191 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326408170 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326649423 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270372273 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326629578 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326650420 UNION ALL SELECT * FROM steam-mantis-108908.WPA.344980665 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326624957 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270382730 UNION ALL SELECT * FROM steam-mantis-108908.WPA.435048955 UNION ALL SELECT * FROM steam-mantis-108908.WPA.216367908 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270372101 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282543507 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282510694 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270352833 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270382267 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270412437 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347097957 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346483255 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270410004 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326628458 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346955191 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270391072 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270400311 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282401733 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326507697 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270380201 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346480383 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270354118 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347130828 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347145362 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347121847 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347189984 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346938887 UNION ALL SELECT * FROM steam-mantis-108908.WPA.216807831 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270355736 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346970928 UNION ALL SELECT * FROM steam-mantis-108908.WPA.462436475) wpa
LEFT JOIN 
(SELECT session_id,MAX(lnd_source) as lnd_source,MAX(lnd_medium) as lnd_medium,MAX(channel_grouping) as channel_grouping FROM `steam-mantis-108908.WPA_Tables.00_LastNonDirectTraffic`
WHERE date <='2025-02-22'
GROUP BY session_id) lnd
ON CONCAT(wpa.user_pseudo_id, wpa.ga_session_id) = lnd.session_id

WHERE wpa.date<='2025-02-22'
--EVENT FILTERS
AND Event_Name='Registration_Funnel'
AND EventAction='NRC'

GROUP BY
--SESSION LEVEL COLUMNS
property_id,date,CONCAT(user_pseudo_id, ga_session_id),user_pseudo_id,ga_session_number,session_engaged,Interface_Brand,Customer_Status_Start,geo.city,geo.country,geo.region,device.mobile_brand_name,device.web_info.browser,device.web_info.browser_version,device.category,device.mobile_marketing_name,device.mobile_model_name,device.operating_system,device.operating_system_version
--TRAFFIC
,lnd_source,lnd_medium,collected_traffic_source.manual_source,collected_traffic_source.manual_medium,lnd.channel_grouping
--GLOBAL DIMENSIONS
,Content_Group,event_name,EventAction,Customer_Status_Event,device.web_info.hostname,Login_Status,page_location,page_referrer,page_title,Technical_EventName,Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,Registration_Type
,User_Reg_Method
,User_Reg_Step
)
