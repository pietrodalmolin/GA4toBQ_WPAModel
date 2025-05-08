INSERT INTO `steam-mantis-108908.WPA_Events.SB_BannerClick`
(
SELECT
--SESSION LEVEL COLUMNS
CAST(wpa.property_id AS INT64) AS property_id
,date
,FARM_FINGERPRINT(CONCAT(wpa.user_pseudo_id, ga_session_id)) AS session_id
,FARM_FINGERPRINT(wpa.user_pseudo_id) AS user_pseudo_id
,ga_session_number AS Key_ga_session_number
,CAST(session_engaged AS INT64) AS Key_session_engaged
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(Interface_Brand, ''), 'Unknown')), 100000)) AS Key_Interface_Brand
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(Customer_Status_Start, ''), 'Unknown')), 100000)) AS Key_Customer_Status_Start
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(geo.city, ''), 'Unknown'))) AS Key_city
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(geo.country, ''), 'Unknown')), 100000)) AS Key_country
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(geo.region, ''), 'Unknown'))) AS Key_region
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.mobile_brand_name, ''), 'Unknown'))) AS Key_mobile_brand_name
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.web_info.browser, ''), 'Unknown'))) AS Key_device_web_info_browser
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.web_info.browser_version, ''), 'Unknown'))) AS Key_device_web_info_browser_version
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.category, ''), 'Unknown'))) AS Key_device_category
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.mobile_marketing_name, ''), 'Unknown'))) AS Key_mobile_marketing_name
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.mobile_model_name, ''), 'Unknown'))) AS Key_mobile_model_name
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.operating_system, ''), 'Unknown'))) AS Key_device_operating_system
,ABS(FARM_FINGERPRINT(IFNULL(NULLIF(device.operating_system_version, ''), 'Unknown'))) AS Key_device_operating_system_version
--GLOBAL DIMENSIONS
,Content_Group
,event_name
,EventAction
,ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(Login_Status, ''), 'Unknown')), 100000)) AS Key_Login_Status
,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,Interface_Component
,Interface_SectionName
--CUSTOM DIMENSIONS
,CAST(ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(
(--BETSSON
CASE 
WHEN Interface_Brand = 'Betsson' AND geo.country IN ('Netherlands', 'Spain', 'Belgium', 'Germany', 'Switzerland', 'Austria') THEN 'WESTERN & SOUTHERN EUROPE'
WHEN Interface_Brand = 'Betsson' AND geo.country IN ('Sweden', 'Norway', 'Denmark', 'Finland', 'Iceland') THEN 'NORDICS'
WHEN Interface_Brand = 'Betsson' AND geo.country IN ('Bolivia', 'Colombia', 'Paraguay', 'Ecuador', 'Chile', 'Peru', 'Mexico', 'Brazil', 'Argentina') THEN 'LATAM'
WHEN Interface_Brand = 'Betsson' AND geo.country IN ('Poland', 'Latvia', 'Hungary', 'Serbia') THEN 'CEECA'
WHEN Interface_Brand = 'Betsson' THEN 'EMERGING MARKETS'
--BETSAFE
WHEN Interface_Brand = 'Betsafe' AND geo.country IN ('Poland', 'Russia', 'Hungary', 'Latvia', 'Serbia') THEN 'CEECA'
WHEN Interface_Brand = 'Betsafe' AND geo.country IN ('Mexico', 'Brazil', 'Peru', 'Chile') THEN 'LATAM'
WHEN Interface_Brand = 'Betsafe' AND geo.country IN ('Sweden', 'Norway', 'Iceland', 'Finland') THEN 'NORDICS'
WHEN Interface_Brand = 'Betsafe' AND geo.country = 'Canada' THEN 'US & CANADA'
WHEN Interface_Brand = 'Betsafe' AND geo.country IN ('Switzerland', 'Netherlands', 'Belgium', 'Spain', 'Austria', 'Germany') THEN 'WESTERN & SOUTHERN EUROPE'
WHEN Interface_Brand = 'Betsafe' THEN 'EMERGING MARKETS'
--CasinoEuro
WHEN Interface_Brand = 'CasinoEuro' AND geo.country = 'Finland' THEN 'NORDICS'
WHEN Interface_Brand = 'CasinoEuro' THEN 'EMERGING MARKETS'
--EuroCasino
WHEN Interface_Brand = 'EuroCasino' AND geo.country = 'Finland' THEN 'NORDICS'
WHEN Interface_Brand = 'EuroCasino' THEN 'EMERGING MARKETS'
-- LATAM brands
WHEN Interface_Brand IN (
'BetssonArgentina', 'BetssonArgentinaPBA', 'Inkabet', 'BetssonCO', 'BetssonPE',
'BetssonArgentinaCDA', 'BetssonArgentinaCBA', 'BetsafePE', 'BetssonBR',
'NordicBet', 'BetssonMX', 'BetssonPY'
) THEN 'LATAM'
-- WESTERN & SOUTHERN EUROPE
WHEN Interface_Brand IN (
'BetssonGR', 'Betsson ES', 'RaceBets', 'BetssonSpain', 'RaceBetsDE', 'BetssonBE'
) THEN 'WESTERN & SOUTHERN EUROPE'
-- ITALY
WHEN Interface_Brand IN (
'BetssonIT', 'StarCasino', 'BingoIT'
) THEN 'ITALY'
-- CEECA
WHEN Interface_Brand IN (
'BetsafeBaltics', 'BetssonKZ', 'JallaCasinoEE', 'SuperCasinoEE'
) THEN 'CEECA'
-- NORDICS
WHEN Interface_Brand IN (
'NordicBetDk', 'JallaCasino', 'CasinoDk', 'NorgesAutomaten'
) THEN 'NORDICS'
-- ZECURE
WHEN Interface_Brand IN (
'RizkHR', 'RizkRS', 'CrashCasino', 'Guts.com', 'Rizk.com', 'Kaboo', 'RizkDE', 'RizkPL'
) THEN 'ZECURE'
-- REALM
WHEN Interface_Brand IN (
'Bets10', 'MobilBahis', 'CasinoMaxi', 'CasinoMetropol'
) THEN 'REALM'
-- ALTA
WHEN Interface_Brand IN (
'BetSolid', 'ArcticBet', 'BetSmith'
) THEN 'ALTA'
-- EUROPEBET
WHEN Interface_Brand = 'EuropeBet' THEN 'EUROPEBET'
-- AFRICA
WHEN Interface_Brand = 'BetsafeKenya' THEN 'AFRICA'
-- EMERGING MARKETS
WHEN Interface_Brand = 'SuperCasino' THEN 'EMERGING MARKETS'
ELSE 'Unassigned' END)
, ''), 'Unknown')), 100000))AS INT64)
AS Key_COMMERCIAL_AREA
--METRICS
,COUNT(*) AS Event_Count

FROM (
SELECT * FROM steam-mantis-108908.WPA.270389480 UNION ALL -- ALOHASHARK GA4 [WEB] 
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
SELECT * FROM steam-mantis-108908.WPA.479948234 -- CRASHCASINO GA4 [WEB]
) wpa

WHERE wpa.date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
--EVENT FILTERS
AND Event_Name='Sportsbook' --filter event_name
AND EventAction='Sportsbook Banner Click' --filter event_action

GROUP BY
--SESSION LEVEL COLUMNS
property_id,date,FARM_FINGERPRINT(CONCAT(wpa.user_pseudo_id, ga_session_id)),FARM_FINGERPRINT(wpa.user_pseudo_id),ga_session_number,session_engaged,Interface_Brand,Customer_Status_Start,geo.city,geo.country,geo.region,device.mobile_brand_name,device.web_info.browser,device.web_info.browser_version,device.category,device.mobile_marketing_name,device.mobile_model_name,device.operating_system,device.operating_system_version
--GLOBAL DIMENSIONS
,Content_Group,event_name,EventAction,Login_Status,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,Interface_Component
,Interface_SectionName
)
