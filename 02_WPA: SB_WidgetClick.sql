CREATE OR REPLACE TABLE `steam-mantis-108908.WPA_Tables.SB_WidgetClick`
PARTITION BY DATE(Date)
CLUSTER BY property_id, event_name, eventaction AS
SELECT
--SESSION LEVEL COLUMNS
property_id,date,CONCAT(user_pseudo_id, ga_session_id) AS session_id,user_pseudo_id,ga_session_number,session_engaged,Interface_Brand,Customer_Status_Start,geo.city AS city,geo.country AS country,geo.region AS region,device.mobile_brand_name AS mobile_brand_name,device.web_info.browser AS device_web_info_browser,device.web_info.browser_version AS device_web_info_browser_version,device.category AS device_category,device.mobile_marketing_name AS mobile_marketing_name,device.mobile_model_name AS mobile_model_name,device.operating_system AS device_operating_system,device.operating_system_version AS device_operating_system_version
--GLOBAL DIMENSIONS
,Content_Group,event_name,EventAction,Login_Status,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,Sportsbook_EventDetails
,Sportsbook_CategoryDetails
,Sportsbook_BetType
,Sportsbook_RememberStakeFlag
,Bonus_ID
,Bonus_EventPhase
,Bonus_Type
,Sportsbook_MarketName
,Sportsbook_MarketTemplateID
,Sportsbook_EventPhase
,Sportsbook_TabName
--CUSTOM COLUMNS
,CASE WHEN STRPOS(EventAction, 'slip on ') > 0 THEN SUBSTR(EventAction, STRPOS(EventAction, 'slip on ') + 8) ELSE NULL END AS AtB_Location
,COALESCE(SAFE_CAST(TRIM(SUBSTR(Sportsbook_CategoryDetails, CASE WHEN INSTR(Sportsbook_CategoryDetails, '- ') > 0 THEN INSTR(Sportsbook_CategoryDetails, '- ') + 2 ELSE 0 END)) AS INT64),0) AS CategoryID
,CASE WHEN INSTR(Sportsbook_EventDetails, ' - f') > 0 THEN TRIM(SUBSTR(Sportsbook_EventDetails, 1, INSTR(Sportsbook_EventDetails, ' - f') - 1)) ELSE NULL END AS Sportsbook_EventName

--METRICS
,COUNT(*) AS Event_Count

FROM(
SELECT * FROM steam-mantis-108908.WPA.270389480 UNION ALL SELECT * FROM steam-mantis-108908.WPA.345721586 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282392101 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326546621 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326546361 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326547182 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326497634 UNION ALL SELECT * FROM steam-mantis-108908.WPA.344917462 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270369193 UNION ALL SELECT * FROM steam-mantis-108908.WPA.323032716 UNION ALL SELECT * FROM steam-mantis-108908.WPA.369344384 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326671725 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326660191 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326408170 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326649423 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270372273 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326629578 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326650420 UNION ALL SELECT * FROM steam-mantis-108908.WPA.344980665 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326624957 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270382730 UNION ALL SELECT * FROM steam-mantis-108908.WPA.435048955 UNION ALL SELECT * FROM steam-mantis-108908.WPA.216367908 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270372101 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282543507 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282510694 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270352833 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270382267 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270412437 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347097957 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346483255 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270410004 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326628458 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346955191 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270391072 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270400311 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282401733 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326507697 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270380201 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346480383 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270354118 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347130828 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347145362 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347121847 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347189984 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346938887 UNION ALL SELECT * FROM steam-mantis-108908.WPA.216807831 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270355736 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346970928
) wpa

WHERE wpa.date <='2025-02-03'
--EVENT FILTERS
AND Event_Name='Sportsbook' --filter event_name
AND EventAction LIKE 'SB Widget Click -%' --filter event_action

GROUP BY
--SESSION LEVEL COLUMNS
property_id,date,CONCAT(user_pseudo_id, ga_session_id),user_pseudo_id,ga_session_number,session_engaged,Interface_Brand,Customer_Status_Start,geo.city,geo.country,geo.region,device.mobile_brand_name,device.web_info.browser,device.web_info.browser_version,device.category,device.mobile_marketing_name,device.mobile_model_name,device.operating_system,device.operating_system_version
--GLOBAL DIMENSIONS
,Content_Group,event_name,EventAction,Login_Status,Sub_Area
    --EVENT SPECIFIC DIMENSIONS
    ,Sportsbook_EventDetails
    ,Sportsbook_CategoryDetails
    ,Sportsbook_BetType
    ,Sportsbook_RememberStakeFlag
    ,Bonus_ID
    ,Bonus_EventPhase
    ,Bonus_Type
    ,Sportsbook_MarketName
    ,Sportsbook_MarketTemplateID
    ,Sportsbook_EventPhase
    ,Sportsbook_TabName
