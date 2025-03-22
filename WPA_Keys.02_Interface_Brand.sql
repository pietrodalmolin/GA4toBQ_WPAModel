CREATE OR REPLACE TABLE `steam-mantis-108908.WPA_Keys.02_Interface_Brand` AS
SELECT DISTINCT
ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(Interface_Brand, ''), 'Unknown')), 100000)) AS Key_Interface_Brand,
IFNULL(NULLIF(Interface_Brand, ''), 'Unknown') AS Interface_Brand
FROM `steam-mantis-108908.WPA.*`
