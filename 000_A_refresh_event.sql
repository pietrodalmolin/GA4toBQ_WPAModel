INSERT INTO `steam-mantis-108908.WPA_Events.TABLE NAME`
(
...

--MAPPING WITH LND TRAFFIC SOURCE
LEFT JOIN 
(SELECT session_id,MAX(lnd_source) as lnd_source,MAX(lnd_medium) as lnd_medium,MAX(channel_grouping) as channel_grouping 
FROM `steam-mantis-108908.WPA_Events.00_LastNonDirectTraffic`
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
GROUP BY session_id) lnd
ON FARM_FINGERPRINT(CONCAT(wpa.user_pseudo_id, ga_session_id)) = lnd.session_id

--DATE FILTER
WHERE wpa.date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))

...
)
