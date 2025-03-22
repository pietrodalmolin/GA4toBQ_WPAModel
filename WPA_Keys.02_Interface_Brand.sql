CREATE OR REPLACE TABLE `steam-mantis-108908.WPA_Keys.02_Interface_Brand` AS
SELECT DISTINCT
ABS(MOD(FARM_FINGERPRINT(IFNULL(NULLIF(Interface_Brand, ''), 'Unknown')), 100000)) AS Key_Interface_Brand,
Interface_Brand,
FROM(
SELECT * FROM steam-mantis-108908.WPA.270389480 UNION ALL SELECT * FROM steam-mantis-108908.WPA.345721586 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282392101 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326546621 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326546361 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326547182 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326497634 UNION ALL SELECT * FROM steam-mantis-108908.WPA.344917462 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270369193 UNION ALL SELECT * FROM steam-mantis-108908.WPA.323032716 UNION ALL SELECT * FROM steam-mantis-108908.WPA.369344384 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326671725 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326660191 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326408170 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326649423 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270372273 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326629578 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326650420 UNION ALL SELECT * FROM steam-mantis-108908.WPA.344980665 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326624957 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270382730 UNION ALL SELECT * FROM steam-mantis-108908.WPA.435048955 UNION ALL SELECT * FROM steam-mantis-108908.WPA.216367908 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270372101 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282543507 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282510694 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270352833 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270382267 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270412437 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347097957 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346483255 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270410004 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326628458 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346955191 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270391072 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270400311 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282401733 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326507697 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270380201 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346480383 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270354118 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347130828 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347145362 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347121847 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347189984 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346938887 UNION ALL SELECT * FROM steam-mantis-108908.WPA.216807831 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270355736 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346970928 UNION ALL SELECT * FROM steam-mantis-108908.WPA.462436475)
