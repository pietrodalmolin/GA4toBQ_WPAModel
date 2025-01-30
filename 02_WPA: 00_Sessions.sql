CREATE OR REPLACE TABLE `steam-mantis-108908.WPA_Tables.00_Sessions`
PARTITION BY DATE(Date)
CLUSTER BY property_id, IsSportsbook, IsGaming AS
(
SELECT
--KEYS
wpa.property_id,wpa.date,CONCAT(wpa.user_pseudo_id, ga_session_id) AS session_id,wpa.user_pseudo_id
--TRAFFIC
,MAX(lnd.lnd_source) as lnd_source,MAX(lnd.lnd_medium) as lnd_medium,MAX(collected_traffic_source.manual_source) as lc_source,MAX(collected_traffic_source.manual_medium) as lc_medium,MAX(lnd.channel_grouping) as channel_grouping
--GLOBAL DIMENSIONS
,MIN(ga_session_number) AS ga_session_number,MAX(session_engaged) AS session_engaged,TIMESTAMP_DIFF(TIMESTAMP_MICROS(MAX(event_timestamp)), TIMESTAMP_MICROS(MIN(event_timestamp)), SECOND) AS session_length,MAX(Interface_Brand) AS Interface_Brand,MAX(CASE WHEN event_name = 'session_start' THEN Customer_Status_Start END) AS Customer_Status_Start,MAX(CASE WHEN event_name = 'session_start' THEN geo.city END) AS city,MAX(CASE WHEN event_name = 'session_start' THEN geo.continent END) AS continent,MAX(CASE WHEN event_name = 'session_start' THEN geo.country END) AS country,MAX(CASE WHEN event_name = 'session_start' THEN geo.region END) AS region,MAX(CASE WHEN event_name = 'session_start' THEN device.mobile_brand_name END) AS mobile_brand_name,MAX(CASE WHEN event_name = 'session_start' THEN device.web_info.browser END) AS device_web_info_browser,MAX(CASE WHEN event_name = 'session_start' THEN device.web_info.browser_version END) AS device_web_info_browser_version,MAX(CASE WHEN event_name = 'session_start' THEN device.category END) AS device_category,MAX(CASE WHEN event_name = 'session_start' THEN device.mobile_marketing_name END) AS mobile_marketing_name,MAX(CASE WHEN event_name = 'session_start' THEN device.mobile_model_name END) AS mobile_model_name,MAX(CASE WHEN event_name = 'session_start' THEN device.operating_system END) AS device_operating_system,MAX(CASE WHEN event_name = 'session_start' THEN device.operating_system_version END) AS device_operating_system_version,CASE WHEN MAX(CASE WHEN Login_Status = 'LoggedIn' THEN 1 ELSE 0 END) = 1 THEN 'LoggedIn' ELSE 'LoggedOut' END AS Login_Status
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
--TIME TO CONVERSION
,TIMESTAMP_DIFF(
  TIMESTAMP_MICROS(MIN(CASE WHEN event_name = 'Login_Funnel' AND EventAction='Login Form Open' THEN event_timestamp ELSE NULL END)),
  TIMESTAMP_MICROS(MIN(event_timestamp)),SECOND) 
  AS TTC_LoginFormOpen
,TIMESTAMP_DIFF(
  TIMESTAMP_MICROS(MIN(CASE WHEN event_name = 'Login_Funnel' AND EventAction LIKE 'Login Success%' THEN event_timestamp ELSE NULL END)),
  TIMESTAMP_MICROS(MIN(event_timestamp)),SECOND) 
  AS TTC_LoginSuccess
,TIMESTAMP_DIFF(
  TIMESTAMP_MICROS(MIN(CASE WHEN event_name = 'Sportsbook' AND (LOWER(EventAction) LIKE '%add to betslip%' OR LOWER(EventAction) LIKE '%add to bet slip%') THEN event_timestamp ELSE NULL END)),
  TIMESTAMP_MICROS(MIN(event_timestamp)),SECOND) 
  AS TTC_AddtoBetslip
,TIMESTAMP_DIFF(
  TIMESTAMP_MICROS(MIN(CASE WHEN Event_Name='Sportsbook' AND EventAction='Place Bet Click' THEN event_timestamp ELSE NULL END)),
  TIMESTAMP_MICROS(MIN(event_timestamp)),SECOND) 
  AS TTC_PlaceBetClick
,TIMESTAMP_DIFF(
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name = 'Registration_Funnel' AND EventAction LIKE 'Registration Form Open' THEN event_timestamp ELSE NULL END)),
    TIMESTAMP_MICROS(MIN(event_timestamp)),SECOND)
    AS TTC_Reg_Form_Open
,TIMESTAMP_DIFF(
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name = 'Registration_Funnel' AND User_Reg_Step = "Step 1 (personal group - name)" THEN event_timestamp ELSE NULL END)),
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name = 'Registration_Funnel' AND EventAction LIKE 'Registration Form Open' THEN event_timestamp ELSE NULL END)),SECOND)
    AS TTC_Step1
,TIMESTAMP_DIFF(
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name= 'Registration_Funnel' AND User_Reg_Step = "Step 2 (personal group - password)" THEN event_timestamp ELSE NULL END)),
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name = 'Registration_Funnel' AND User_Reg_Step = "Step 1 (personal group - name)" THEN event_timestamp ELSE NULL END)),SECOND)
    AS TTC_Step2
,TIMESTAMP_DIFF(
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name= 'Registration_Funnel' AND User_Reg_Step = "Step 3 (address group - address)" THEN event_timestamp ELSE NULL END)),
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name= 'Registration_Funnel' AND User_Reg_Step = "Step 2 (personal group - password)" THEN event_timestamp ELSE NULL END)),SECOND)
    AS TTC_Step3
,TIMESTAMP_DIFF(
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name= 'Registration_Funnel' AND User_Reg_Step = "Step 4 (extra group - phone)" THEN event_timestamp ELSE NULL END)),
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name= 'Registration_Funnel' AND User_Reg_Step = "Step 3 (address group - address)" THEN event_timestamp ELSE NULL END)),SECOND)
    AS TTC_Step4
,TIMESTAMP_DIFF(
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name= 'Registration_Funnel' AND User_Reg_Step = "Step 5 (extra group - additional)" THEN event_timestamp ELSE NULL END)),
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name= 'Registration_Funnel' AND User_Reg_Step = "Step 4 (extra group - phone)" THEN event_timestamp ELSE NULL END)),SECOND)
    AS TTC_Step5
,TIMESTAMP_DIFF(
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name= 'Registration_Funnel' AND User_Reg_Step = "Step 6 (extra group - deposit-limit)" THEN event_timestamp ELSE NULL END)),
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name= 'Registration_Funnel' AND User_Reg_Step = "Step 5 (extra group - additional)" THEN event_timestamp ELSE NULL END)),SECOND)
    AS TTC_Step6
,TIMESTAMP_DIFF(
    TIMESTAMP_MICROS(MIN(CASE WHEN eventaction = 'NRC' THEN event_timestamp ELSE NULL END)),
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name= 'Registration_Funnel' AND User_Reg_Step = "Step 6 (extra group - deposit-limit)" THEN event_timestamp ELSE NULL END)),SECOND)
    AS TTC_NRC
,TIMESTAMP_DIFF(
    TIMESTAMP_MICROS(MIN(CASE WHEN event_name = 'Payments' AND EventAction LIKE 'Open Deposit Page' THEN event_timestamp ELSE NULL END)),
    TIMESTAMP_MICROS(MIN(event_timestamp)),SECOND)
    AS TTC_Open_Deposit_Page
,TIMESTAMP_DIFF(
    TIMESTAMP_MICROS(MIN(CASE WHEN eventaction = 'NDC' THEN event_timestamp ELSE NULL END)),
    TIMESTAMP_MICROS(MIN(CASE WHEN eventaction = 'NRC' THEN event_timestamp ELSE NULL END)),SECOND)
    AS TTC_NDC

FROM(
SELECT * FROM steam-mantis-108908.WPA.270389480 UNION ALL SELECT * FROM steam-mantis-108908.WPA.345721586 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282392101 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326546621 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326546361 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326547182 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326497634 UNION ALL SELECT * FROM steam-mantis-108908.WPA.344917462 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270369193 UNION ALL SELECT * FROM steam-mantis-108908.WPA.323032716 UNION ALL SELECT * FROM steam-mantis-108908.WPA.369344384 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326671725 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326660191 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326408170 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326649423 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270372273 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326629578 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326650420 UNION ALL SELECT * FROM steam-mantis-108908.WPA.344980665 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326624957 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270382730 UNION ALL SELECT * FROM steam-mantis-108908.WPA.435048955 UNION ALL SELECT * FROM steam-mantis-108908.WPA.216367908 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270372101 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282543507 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282510694 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270352833 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270382267 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270412437 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347097957 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346483255 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270410004 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326628458 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346955191 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270391072 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270400311 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282401733 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326507697 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270380201 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346480383 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270354118 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347130828 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347145362 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347121847 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347189984 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346938887 UNION ALL SELECT * FROM steam-mantis-108908.WPA.216807831 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270355736 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346970928
) wpa
LEFT JOIN 
(SELECT session_id,lnd_source,lnd_medium,channel_grouping FROM `steam-mantis-108908.WPA_Tables.00_LastNonDirectTraffic` WHERE date <='2025-01-26'
GROUP BY session_id,lnd_source,lnd_medium,channel_grouping) lnd ON CONCAT(wpa.user_pseudo_id, wpa.ga_session_id) = lnd.session_id

WHERE wpa.date <='2025-01-28'
AND CONCAT(wpa.user_pseudo_id, ga_session_id) IS NOT NULL

GROUP BY
wpa.date, wpa.property_id, session_id, wpa.user_pseudo_id
)
