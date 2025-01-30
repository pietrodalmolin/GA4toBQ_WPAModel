CREATE OR REPLACE TABLE `steam-mantis-108908.WPA_Dimensions.02_mobile_brand_name`
AS
SELECT distinct(TRIM(LOWER(mobile_brand_name))) as mobile_brand_name FROM `steam-mantis-108908.WPA_Tables.00_Sessions`
WHERE mobile_brand_name IS NOT NULL AND mobile_brand_name<>''
ORDER BY mobile_brand_name ASC
