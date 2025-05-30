INSERT INTO `steam-mantis-108908.WPA_Events.SEGMENT_SB_BetConfirmed_SB_NDCRDC`
(
SELECT
  a.property_id,
  a.date,
  a.session_id,
  a.user_pseudo_id,
  a.Key_ga_session_number,
  a.Key_session_engaged,
  a.Key_Interface_Brand,
  a.Key_Customer_Status_Start,
  a.Key_city,
  a.Key_country,
  a.Key_region,
  a.Key_mobile_brand_name,
  a.Key_device_web_info_browser,
  a.Key_device_web_info_browser_version,
  a.Key_device_category,
  a.Key_mobile_marketing_name,
  a.Key_mobile_model_name,
  a.Key_device_operating_system,
  a.Key_device_operating_system_version,
  a.Key_Login_Status,
  a.Key_COMMERCIAL_AREA,
  a.Event_Count
FROM `steam-mantis-108908.WPA_Events.SB_BetConfirmed` a
INNER JOIN `steam-mantis-108908.WPA_Events.SB_NDCRDC` b
  ON a.session_id = b.session_id
WHERE a.date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  AND b.date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
)
