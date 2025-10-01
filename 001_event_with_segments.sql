INSERT INTO `steam-mantis-108908.WPA_Events.SB_ProductSearch` --*(1)

SELECT
-- SESSION LEVEL COLUMNS
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

-- GLOBAL DIMENSIONS
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

-- EVENT SPECIFIC DIMENSIONS
,Interface_SearchTerm

-- SEGMENTS
,MAX(CASE WHEN Event_Name='Sportsbook' AND technical_eventname='SportsbookWidgetClick' AND STARTS_WITH(EventAction,'SB Widget Click - Add') THEN 1 ELSE 0 END)
OVER (PARTITION BY date, session_id) AS Segment_AddtoBetslip
,MAX(CASE WHEN Event_Name='Sportsbook' AND Technical_EventName='BetConfirmed' AND EventAction='Bet Confirmed' THEN 1 ELSE 0 END)
OVER (PARTITION BY date, session_id) AS Segment_BetConfirmed

--FUNNEL TIMESTAMPS
,MIN(CASE WHEN Event_Name='Sportsbook' AND technical_eventname='SportsbookWidgetClick' AND STARTS_WITH(EventAction,'SB Widget Click - Add') THEN event_timestamp END)
OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_AddtoBetslip
,MIN(CASE WHEN Event_Name='Sportsbook' AND Technical_EventName='BetConfirmed' AND EventAction='Bet Confirmed' THEN event_timestamp END)
OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_BetConfirmed

-- METRICS
,1 AS Event_Count

FROM `steam-mantis-108908.WPA.00_MasterTable`
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND (
(Event_Name='Search' AND Technical_EventName IN ('ProductSearch','SportsbookSearchClick') AND EventAction='Sportsbook Search')
OR
(Event_Name='Sportsbook' AND technical_eventname='SportsbookWidgetClick' AND STARTS_WITH(EventAction,'SB Widget Click - Add'))
OR
(Event_Name='Sportsbook' AND Technical_EventName='BetConfirmed' AND EventAction='Bet Confirmed')
)
QUALIFY Event_Name='Search'
