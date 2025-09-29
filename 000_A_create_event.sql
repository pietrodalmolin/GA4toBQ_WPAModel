CREATE OR REPLACE TABLE `steam-mantis-108908.WPA_Events.TABLENAME` --*(1)
PARTITION BY DATE(Date)
CLUSTER BY Key_Interface_Brand AS

SELECT
--"SESSION LEVEL" COLUMNS
property_id
,date
,event_datetime
,event_timestamp
,session_id
,user_pseudo_id
,GUID
,Key_ga_session_number
,Key_session_engaged
,Key_Interface_Brand
,Key_Customer_Status_Start
,Key_city
,Key_country
,Key_region
,Key_Commercial_Area
,Key_Technical_PlatformUsed
,Key_mobile_brand_name
,Key_device_web_info_browser
,Key_device_web_info_browser_version
,Key_device_category
,Key_mobile_marketing_name
,Key_mobile_model_name
,Key_device_operating_system
,Key_device_operating_system_version
,Key_app_info_id
,Key_app_info_version
,Key_app_info_install_source

--TRAFFIC COLUMNS
,lnd_source
,lnd_medium
,lc_source
,lc_medium
,Key_channel_grouping

--GLOBAL DIMENSIONS
,Key_device_web_info_hostname
,Key_Interface_SiteLanguage
,Key_Jurisdiction
,content_group
,event_name
,Technical_EventName
,EventAction
,Key_Customer_Status_Event
,Key_Login_Status
,page_location
,page_referrer
,page_title
,Key_Technical_PlatformName
,Technical_ScreenOrientation
,Technical_ScreenResolution
,Key_User_CustomerLevel
,Sub_Area
--EVENT SPECIFIC DIMENSIONS *(2)
,Registration_Type
,User_Reg_Method
,User_Reg_Step

--METRICS
,1 AS Event_Count

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)) --*(3)

--EVENT FILTERS *(4)
AND Event_Name='Registration_Funnel'
AND Technical_EventName='ConfirmedRegistration'
AND EventAction='NRC'
