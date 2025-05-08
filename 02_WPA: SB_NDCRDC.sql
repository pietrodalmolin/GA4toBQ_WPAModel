INSERT INTO `steam-mantis-108908.WPA_Events.SB_NDCRDC`

SELECT 
property_id
,date
,NDCRDC.session_id
,user_pseudo_id
,Key_ga_session_number
,Key_session_engaged
,Key_Interface_Brand
,Key_Customer_Status_Start
,Key_city
,Key_country
,Key_region
,Key_mobile_brand_name
,Key_device_web_info_browser
,Key_device_web_info_browser_version
,Key_device_category
,Key_mobile_marketing_name
,Key_mobile_model_name
,Key_device_operating_system
,Key_device_operating_system_version
,Content_Group
,event_name
,EventAction
,Key_Login_Status
,Technical_EventName
,Technical_PlatformName
,Key_COMMERCIAL_AREA
,Event_Count

FROM 
    `steam-mantis-108908.WPA_Events.CA_NDCRDC` AS NDCRDC
INNER JOIN 
    (SELECT session_id FROM `steam-mantis-108908.WPA_Events.00_Sessions` WHERE IsSportsbook=1 AND date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))) AS Sessions
ON 
    NDCRDC.session_id = Sessions.session_id

WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
