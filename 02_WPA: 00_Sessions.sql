INSERT INTO `steam-mantis-108908.WPA_Events.00_Sessions`
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
--CUSTOM DIMENSIONS
,CAST(ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(
(--BETSSON
CASE 
WHEN MAX(Interface_Brand) = 'Betsson' AND MAX(CASE WHEN event_name = 'session_start' THEN geo.region END) IN ('Netherlands', 'Spain', 'Belgium', 'Germany', 'Switzerland', 'Austria') THEN 'WESTERN & SOUTHERN EUROPE'
WHEN MAX(Interface_Brand) = 'Betsson' AND MAX(CASE WHEN event_name = 'session_start' THEN geo.region END) IN ('Sweden', 'Norway', 'Denmark', 'Finland', 'Iceland') THEN 'NORDICS'
WHEN MAX(Interface_Brand) = 'Betsson' AND MAX(CASE WHEN event_name = 'session_start' THEN geo.region END) IN ('Bolivia', 'Colombia', 'Paraguay', 'Ecuador', 'Chile', 'Peru', 'Mexico', 'Brazil', 'Argentina') THEN 'LATAM'
WHEN MAX(Interface_Brand) = 'Betsson' AND MAX(CASE WHEN event_name = 'session_start' THEN geo.region END) IN ('Poland', 'Latvia', 'Hungary', 'Serbia') THEN 'CEECA'
WHEN MAX(Interface_Brand) = 'Betsson' THEN 'EMERGING MARKETS'
--BETSAFE
WHEN MAX(Interface_Brand) = 'Betsafe' AND MAX(CASE WHEN event_name = 'session_start' THEN geo.region END) IN ('Poland', 'Russia', 'Hungary', 'Latvia', 'Serbia') THEN 'CEECA'
WHEN MAX(Interface_Brand) = 'Betsafe' AND MAX(CASE WHEN event_name = 'session_start' THEN geo.region END) IN ('Mexico', 'Brazil', 'Peru', 'Chile') THEN 'LATAM'
WHEN MAX(Interface_Brand) = 'Betsafe' AND MAX(CASE WHEN event_name = 'session_start' THEN geo.region END) IN ('Sweden', 'Norway', 'Iceland', 'Finland') THEN 'NORDICS'
WHEN MAX(Interface_Brand) = 'Betsafe' AND MAX(CASE WHEN event_name = 'session_start' THEN geo.region END) = 'Canada' THEN 'US & CANADA'
WHEN MAX(Interface_Brand) = 'Betsafe' AND MAX(CASE WHEN event_name = 'session_start' THEN geo.region END) IN ('Switzerland', 'Netherlands', 'Belgium', 'Spain', 'Austria', 'Germany') THEN 'WESTERN & SOUTHERN EUROPE'
WHEN MAX(Interface_Brand) = 'Betsafe' THEN 'EMERGING MARKETS'
--CasinoEuro
WHEN MAX(Interface_Brand) = 'CasinoEuro' AND MAX(CASE WHEN event_name = 'session_start' THEN geo.region END) = 'Finland' THEN 'NORDICS'
WHEN MAX(Interface_Brand) = 'CasinoEuro' THEN 'EMERGING MARKETS'
--EuroCasino
WHEN MAX(Interface_Brand) = 'EuroCasino' AND MAX(CASE WHEN event_name = 'session_start' THEN geo.region END) = 'Finland' THEN 'NORDICS'
WHEN MAX(Interface_Brand) = 'EuroCasino' THEN 'EMERGING MARKETS'
-- LATAM brands
WHEN MAX(Interface_Brand) IN (
'BetssonArgentina', 'BetssonArgentinaPBA', 'Inkabet', 'BetssonCO', 'BetssonPE',
'BetssonArgentinaCDA', 'BetssonArgentinaCBA', 'BetsafePE', 'BetssonBR',
'NordicBet', 'BetssonMX', 'BetssonPY'
) THEN 'LATAM'
-- WESTERN & SOUTHERN EUROPE
WHEN MAX(Interface_Brand) IN (
'BetssonGR', 'Betsson ES', 'RaceBets', 'BetssonSpain', 'RaceBetsDE', 'BetssonBE'
) THEN 'WESTERN & SOUTHERN EUROPE'
-- ITALY
WHEN MAX(Interface_Brand) IN (
'BetssonIT', 'StarCasino', 'BingoIT'
) THEN 'ITALY'
-- CEECA
WHEN MAX(Interface_Brand) IN (
'BetsafeBaltics', 'BetssonKZ', 'JallaCasinoEE', 'SuperCasinoEE'
) THEN 'CEECA'
-- NORDICS
WHEN MAX(Interface_Brand) IN (
'NordicBetDk', 'JallaCasino', 'CasinoDk', 'NorgesAutomaten'
) THEN 'NORDICS'
-- ZECURE
WHEN MAX(Interface_Brand) IN (
'RizkHR', 'RizkRS', 'CrashCasino', 'Guts.com', 'Rizk.com', 'Kaboo', 'RizkDE', 'RizkPL'
) THEN 'ZECURE'
-- REALM
WHEN MAX(Interface_Brand) IN (
'Bets10', 'MobilBahis', 'CasinoMaxi', 'CasinoMetropol'
) THEN 'REALM'
-- ALTA
WHEN MAX(Interface_Brand) IN (
'BetSolid', 'ArcticBet', 'BetSmith'
) THEN 'ALTA'
-- EUROPEBET
WHEN MAX(Interface_Brand) = 'EuropeBet' THEN 'EUROPEBET'
-- AFRICA
WHEN MAX(Interface_Brand) = 'BetsafeKenya' THEN 'AFRICA'
-- EMERGING MARKETS
WHEN MAX(Interface_Brand) = 'SuperCasino' THEN 'EMERGING MARKETS'
ELSE 'Unassigned' END)
, ''), 'Unknown')), 100000))AS INT64)
AS Key_COMMERCIAL_AREA

FROM `steam-mantis-108908.WPA.*` wpa
LEFT JOIN 
(SELECT session_id,MAX(lnd_source) as lnd_source,MAX(lnd_medium) as lnd_medium,MAX(channel_grouping) as channel_grouping FROM `steam-mantis-108908.WPA_Events.00_LastNonDirectTraffic`
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
GROUP BY session_id) lnd
ON FARM_FINGERPRINT(CONCAT(wpa.user_pseudo_id, ga_session_id)) = lnd.session_id

WHERE wpa.date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND CONCAT(wpa.user_pseudo_id, ga_session_id) IS NOT NULL

GROUP BY
wpa.date, wpa.property_id, session_id, wpa.user_pseudo_id
)
