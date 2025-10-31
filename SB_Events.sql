INSERT INTO `steam-mantis-108908.WPA_Events.SB_Events`

SELECT *
-- SEGMENTS
,MAX(CASE WHEN event='AddtoBetslip' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_AddtoBetslip
,MAX(CASE WHEN event='AutoCollapse' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_AutoCollapse
,MAX(CASE WHEN event='AutoExpand' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_AutoExpand
,MAX(CASE WHEN event='BetConfirmed' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_BetConfirmed
,MAX(CASE WHEN event='CashOut' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_CashOut
,MAX(CASE WHEN event='Search' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_Search
,MAX(CASE WHEN event='SortAndFilter' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_SortAndFilter
,MAX(CASE WHEN event='PreDefStake' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_PreDefStake
,MAX(CASE WHEN event='QuickLinksScroller' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_QuickLinksScroller
,MAX(CASE WHEN event='SportsbookBannerClick' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_SportsbookBannerClick
,MAX(CASE WHEN event='CreateCouponSubmit' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_CreateCouponSubmit
,MAX(CASE WHEN event='SportsbookSubCategoryClick' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_SportsbookSubCategoryClick
,MAX(CASE WHEN event='SportsbookSettings' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_SportsbookSettings
,MAX(CASE WHEN event='BurgerMenuClick' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_BurgerMenuClick
,MAX(CASE WHEN event='BottomNavigationBar' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_BottomNavigationBar
,MAX(CASE WHEN event='ScrollablePopularCompetitionsWidget' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_ScrollablePopularCompetitionsWidget
,MAX(CASE WHEN event='BetFailed' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_BetFailed
,MAX(CASE WHEN event='MarketSelectorClick' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_MarketSelectorClick
,MAX(CASE WHEN event='MarketSelectorApplied' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_MarketSelectorApplied
,MAX(CASE WHEN event='MultiMarketToggle' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_MultiMarketToggle
,MAX(CASE WHEN event='PlaceBetClick' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_PlaceBetClick
,MAX(CASE WHEN event='EventVisit' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_EventVisit
,MAX(CASE WHEN event='LiveStreamInteraction' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_LiveStreamInteraction
,MAX(CASE WHEN event='PinClick' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_PinClick
,MAX(CASE WHEN event='PreferenceChange' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_PreferenceChange
,MAX(CASE WHEN event='BetShare' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_BetShare
,MAX(CASE WHEN event='DeposittoPlaceBet' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_DeposittoPlaceBet
,MAX(CASE WHEN event='TCI_BetHistory' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_TCI_BetHistory
,MAX(CASE WHEN event='TCI_EventPage' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_TCI_EventPage
,MAX(CASE WHEN event='ProductFavouriteClick' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_ProductFavouriteClick
,MAX(CASE WHEN event='CarouselCardClick' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_CarouselCardClick
,MAX(CASE WHEN event='CarouselNavigationClick' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_CarouselNavigationClick
,MAX(CASE WHEN event='NDCRDC' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_NDCRDC
,MAX(CASE WHEN event='OpenDepPage' THEN 1 ELSE 0 END) OVER (PARTITION BY date, session_id) AS Segment_OpenDepPage


-- FUNNEL TIMESTAMPS
,MIN(CASE WHEN event='AddtoBetslip' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_AddtoBetslip
,MIN(CASE WHEN event='AutoCollapse' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_AutoCollapse
,MIN(CASE WHEN event='AutoExpand' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_AutoExpand
,MIN(CASE WHEN event='BetConfirmed' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_BetConfirmed
,MIN(CASE WHEN event='CashOut' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_CashOut
,MIN(CASE WHEN event='Search' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_Search
,MIN(CASE WHEN event='SortAndFilter' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_SortAndFilter
,MIN(CASE WHEN event='PreDefStake' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_PreDefStake
,MIN(CASE WHEN event='QuickLinksScroller' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_QuickLinksScroller
,MIN(CASE WHEN event='SportsbookBannerClick' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_SportsbookBannerClick
,MIN(CASE WHEN event='CreateCouponSubmit' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_CreateCouponSubmit
,MIN(CASE WHEN event='SportsbookSubCategoryClick' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_SportsbookSubCategoryClick
,MIN(CASE WHEN event='SportsbookSettings' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_SportsbookSettings
,MIN(CASE WHEN event='BurgerMenuClick' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_BurgerMenuClick
,MIN(CASE WHEN event='BottomNavigationBar' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_BottomNavigationBar
,MIN(CASE WHEN event='ScrollablePopularCompetitionsWidget' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_ScrollablePopularCompetitionsWidget
,MIN(CASE WHEN event='BetFailed' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_BetFailed
,MIN(CASE WHEN event='MarketSelectorClick' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_MarketSelectorClick
,MIN(CASE WHEN event='MarketSelectorApplied' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_MarketSelectorApplied
,MIN(CASE WHEN event='MultiMarketToggle' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_MultiMarketToggle
,MIN(CASE WHEN event='PlaceBetClick' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_PlaceBetClick
,MIN(CASE WHEN event='EventVisit' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_EventVisit
,MIN(CASE WHEN event='LiveStreamInteraction' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_LiveStreamInteraction
,MIN(CASE WHEN event='PinClick' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_PinClick
,MIN(CASE WHEN event='PreferenceChange' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_PreferenceChange
,MIN(CASE WHEN event='BetShare' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_BetShare
,MIN(CASE WHEN event='DeposittoPlaceBet' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_DepositoPlaceBet
,MIN(CASE WHEN event='TCI_BetHistory' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_TCI_BetHistory
,MIN(CASE WHEN event='TCI_EventPage' THEN event_timestamp END) 
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_TCI_EventPage
,MIN(CASE WHEN event='ProductFavouriteClick' THEN event_timestamp END)
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_ProductFavouriteClick
,MIN(CASE WHEN event='CarouselCardClick' THEN event_timestamp END)
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_CarouselCardClick
,MIN(CASE WHEN event='CarouselNavigationClick' THEN event_timestamp END)
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_CarouselNavigationClick
,MIN(CASE WHEN event='NDCRDC' THEN event_timestamp END)
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_NDCRDC
,MIN(CASE WHEN event='OpenDepPage' THEN event_timestamp END)
  OVER (PARTITION BY date, session_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS timestamp_OpenDepPage


-- METRICS
,1 AS Event_Count

FROM
(
----------AddtoBetslip
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'AddtoBetslip' AS event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,CASE
  WHEN EventAction LIKE '%sportsbook.event Event Market Group%' THEN 'Homepage'
  WHEN EventAction LIKE '%sportsbook Event Market Group%' THEN 'Homepage'
  WHEN EventAction LIKE '%sportsbook.event Event Table%' THEN 'Homepage'
  WHEN EventAction LIKE '%sportsbook Event Table%' THEN 'Homepage'
  WHEN EventAction LIKE '%Price Boost Widget - Home%' THEN 'Homepage'
  WHEN EventAction LIKE '%Popular Bets - Home%' THEN 'Homepage'
  WHEN EventAction LIKE '%Homepage Popular Bets%' THEN 'Homepage'
  WHEN EventAction LIKE '%Popular Pre Built Bets - Home%' THEN 'Homepage'
  WHEN ENDS_WITH(EventAction, 'on sportsbook Popular Pre-Built Bets') THEN 'Homepage'
  WHEN EventAction LIKE '%sportsbook Carousel%' THEN 'Homepage'
  WHEN EventAction LIKE '%sportsbook.event Carousel%' THEN 'Homepage'
  WHEN EventAction LIKE '%on Bet of the Day%' THEN 'Homepage'
  -- Category
  WHEN EventAction LIKE '%sportsbook.category%' THEN 'Category Page'
  WHEN EventAction LIKE '%my-coupon%' THEN 'Category Page'
  WHEN EventAction LIKE '%Popular Pre Built Bets - Category%' THEN 'Category Page'
  WHEN EventAction LIKE '%Popular Bets - Category%' THEN 'Category Page'
  -- Live
  WHEN EventAction LIKE '%sportsbook.live-category%' THEN 'Live Page'
  WHEN EventAction LIKE '%sportsbook.live%' THEN 'Live Page'
  -- Competition
  WHEN EventAction LIKE '%sportsbook.competition%' THEN 'Competition Page'
  WHEN EventAction LIKE '%Popular Pre Built Bets - Competition%' THEN 'Competition Page'
  -- Search
  WHEN EventAction LIKE '%sportsbook.search%' THEN 'After Search'
  -- Starting Soon
  WHEN EventAction LIKE '%sportsbook.starting-soon-tab%' THEN 'Starting Soon Page'
  WHEN EventAction LIKE '%sportsbook.starting-soon%' THEN 'Starting Soon Page'
  -- Customer Favourites
  WHEN EventAction LIKE '%sportsbook.customer-favourites%' THEN 'Customer Favourites Page'
  -- Region
  WHEN EventAction LIKE '%sportsbook.region%' THEN 'Region Page'
  -- Live Stream Calendar
  WHEN EventAction LIKE '%sportsbook.live-stream-calendar%' THEN 'Live Stream Calendar Page'
  -- Bet History
  WHEN EventAction LIKE '%sportsbook.bet-history%' THEN 'Bet History'
  -- Price Boost
  WHEN EventAction LIKE '%sportsbook.price-boost-category%' THEN 'Price Boost Page'
  WHEN EventAction LIKE '%sportsbook.price-boost%' THEN 'Price Boost Page'
  -- Settings
  WHEN EventAction LIKE '%sportsbook.settings%' THEN 'Settings Page'
  ELSE 'Other Locations' END AS Location
,CASE
  WHEN LOWER(EventAction) LIKE '%event market group%' THEN 'Event Page'
  WHEN LOWER(EventAction) LIKE '%event table%' THEN 'Events Table'
  WHEN LOWER(EventAction) LIKE '%outrights%' THEN 'Events Table'
  WHEN LOWER(EventAction) LIKE '%carousel%' THEN 'Carousel'
  WHEN LOWER(EventAction) LIKE '%cards%' THEN 'Cards'
  WHEN LOWER(EventAction) LIKE '%popular bets%' THEN 'Popular Bets'
  WHEN LOWER(EventAction) LIKE '%popular pre-built bets%' THEN 'Popular Pre-Built Bets'
  WHEN LOWER(EventAction) LIKE '%price-boost%' THEN 'Price Boost (Page)'
  WHEN EventAction LIKE '%Price Boost Widget%' THEN 'Price Boost Widget'
  WHEN LOWER(EventAction) LIKE '%my-coupon%' THEN 'My Coupon'
  WHEN LOWER(EventAction) LIKE '%bet of the day%' THEN 'Bet of the Day'
  WHEN EventAction LIKE '%Popular Pre Built Bets%' THEN 'Popular Pre-Built Bets'
  ELSE 'Other Widgets' END AS Widget
,CASE WHEN STRPOS(EventAction, 'Group') > 0 THEN LTRIM(REPLACE(REPLACE(SUBSTR(EventAction, STRPOS(EventAction, 'Group') + 5), '(', ''), ')', '')) ELSE 'none' END AS EventPage_Filter
,COALESCE(SAFE_CAST(REGEXP_EXTRACT(Sportsbook_CategoryDetails, r'(\d+)$') AS INT64),0) AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
--EVENT FILTERS
AND Event_Name='Sportsbook'
AND Technical_EventName ='SportsbookWidgetClick'
AND STARTS_WITH(EventAction, 'SB Widget Click - Add')

UNION ALL

----------AutoCollapse&AutoExpand
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,CASE WHEN STARTS_WITH(EventAction, 'SB Widget Click - Auto Collapse') THEN 'AutoCollapse'
  WHEN STARTS_WITH(EventAction, 'SB Widget Click - Auto Expand') THEN 'AutoExpand' END AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,REGEXP_EXTRACT(EventAction, r' on (.+)') AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,COALESCE(SAFE_CAST(REGEXP_EXTRACT(Sportsbook_CategoryDetails, r'(\d+)$') AS INT64),0) AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
--EVENT FILTERS
AND Event_Name='Sportsbook'
AND Technical_EventName ='SportsbookWidgetClick'
AND STARTS_WITH(EventAction, 'SB Widget Click - Auto')

UNION ALL

----------CashOut
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'CashOut' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,REGEXP_EXTRACT(EventAction, r' on (.+)') AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,TRIM(REGEXP_EXTRACT(EventAction, r'Cash out - (.+?) on ')) AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
--EVENT FILTERS
AND Event_Name='Sportsbook'
AND Technical_EventName ='SportsbookWidgetClick'
AND STARTS_WITH(EventAction, 'SB Widget Click - Cash out')

UNION ALL

----------BetConfirmed
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp
,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'BetConfirmed' AS event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
--EVENT FILTERS
AND Event_Name='Sportsbook'
AND Technical_EventName ='BetConfirmed'
AND EventAction='Bet Confirmed'

UNION ALL

----------Search
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'Search' AS event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
--EVENT FILTERS
AND Event_Name='Search'
AND Technical_EventName IN ('ProductSearch','SportsbookSearchClick')
AND EventAction='Sportsbook Search'

UNION ALL

----------SortAndFilter
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'SortAndFilter' AS event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,TRIM(
  REGEXP_EXTRACT(
    REGEXP_REPLACE(
      Filters_Applied,
      r'^Sportsbook( - Product)? Filter (Click|Applied) - ',
      ''
    ),
    r'^([^:-]+)'
  )
) AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,TRIM(
  CASE 
    WHEN REGEXP_CONTAINS(Filters_Applied, r':') THEN
      REGEXP_EXTRACT(
        REGEXP_REPLACE(
          Filters_Applied,
          r'^Sportsbook( - Product)? Filter (Click|Applied) - ',
          ''
        ),
        r':\s*(.+)$'
      )
    ELSE
      REGEXP_EXTRACT(
        REGEXP_REPLACE(
          Filters_Applied,
          r'^Sportsbook( - Product)? Filter (Click|Applied) - ',
          ''
        ),
        r'^[^-]+-\s*(.+)$'
      )
  END
) AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
--EVENT FILTERS
AND (
(Event_Name='Clicks' AND Technical_EventName = 'GenericSortAndFilter' AND STARTS_WITH(EventAction, 'Sportsbook'))
OR
(Event_Name='Click' AND Technical_EventName = 'GenericSortAndFilter' AND STARTS_WITH(EventAction, 'Sportsbook'))
OR
(Event_Name='Clicks' AND STARTS_WITH(Technical_EventName, 'ProductFilter') AND STARTS_WITH(EventAction, 'Sportsbook')) --OBGA
)

UNION ALL

----------PreDefStake
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'PreDefStake' AS event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,SPLIT(Interface_Component, ' - ')[SAFE_OFFSET(2)] AS Currency
,CAST(SPLIT(Interface_Component, ' - ')[SAFE_OFFSET(3)] AS INT64) AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND (
(Event_Name='Clicks' AND Technical_EventName = 'GenericButtonClick' AND Interface_SectionName='Quick Betslip')
OR
(Event_Name='Missing_FormName' AND Technical_EventName = 'GenericButtonClick' AND Interface_SectionName='Quick Betslip')
)

UNION ALL

----------QuickLinksScroller
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'QuickLinksScroller' AS event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook' AND Technical_EventName IN ('ComponentInteraction','QuickLinksScroller') AND EventAction='Quick Links Scroller'
AND NOT Interface_Component IN ('Sportsbook Search','Navigation Panel')

UNION ALL

----------SportsbookBannerClick
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'SportsbookBannerClick' AS event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook' AND Technical_EventName IN ('ComponentInteraction','SportsbookBannerClick') AND EventAction='Sportsbook Banner Click'

UNION ALL

----------CreateCouponSubmit
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'CreateCouponSubmit' AS event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook' AND Technical_EventName IN ('ComponentInteraction','CreateCouponSubmit') AND EventAction='Create Coupon Submit'

UNION ALL

----------SportsbookSubCategoryClick
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'SportsbookSubCategoryClick' AS event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,CASE
  WHEN STRPOS(interface_component, ' -') > 0 THEN
    SAFE_CAST(SUBSTR(interface_component, 1, STRPOS(interface_component, ' -') - 1) AS INT64)
  WHEN STRPOS(interface_component, '.') > 0 THEN
    SAFE_CAST(SUBSTR(interface_component, 1, STRPOS(interface_component, '.') - 1) AS INT64)
  ELSE
    NULL
END AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,CASE 
  WHEN STRPOS(interface_component, ' - ') > 0 THEN 
    ARRAY_REVERSE(SPLIT(SUBSTR(interface_component, STRPOS(interface_component, ' - ') + 3), '.'))[OFFSET(0)]
  WHEN STRPOS(interface_component, '.') > 0 THEN 
    ARRAY_REVERSE(SPLIT(interface_component, '.'))[OFFSET(0)]
  ELSE 
    NULL 
END AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook' AND Technical_EventName IN ('ComponentInteraction','SportsbookSubCategoryClick') AND STARTS_WITH(EventAction,'SubCategory Click')

UNION ALL

----------SportsbookSettings
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'SportsbookSettings' AS event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND (
--OBG
(Event_Name='Clicks' AND Technical_EventName ='GenericButtonClick' AND EventAction='Sportsbook Settings Clicks')
OR
--FABRIC WRONG
(Event_Name='Sportsbook' AND Technical_EventName ='LinkClick' AND STARTS_WITH(Interface_Component, 'Sportsbook Settings'))
OR
--FABRIC CORRECT
(Event_Name='Sportsbook' AND Technical_EventName ='ComponentInteraction' AND EventAction='Settings Click')
)

UNION ALL

----------BurgerMenuClick
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'BurgerMenuClick' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND (
  (Event_Name = 'Sportsbook'
   AND EventAction = 'Burger Menu Click')
  OR
  (Event_Name = 'Clicks'
   AND Technical_EventName = 'ProductNavigationClick'
   AND (
        Sub_Area = 'Sportsbook Burger Menu'
        OR EventDetails = 'Sportsbook Burger Menu'
       )
  )
)

UNION ALL

----------BottomNavigationBar
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'BottomNavigationBar' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Clicks'
AND Technical_EventName='ProductNavigationClick'
--AND Product_Type='Sportsbook'
AND Interface_SectionName='Bottom Navigation Bar'

UNION ALL

----------ScrollablePopularCompetitionsWidget
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'ScrollablePopularCompetitionsWidget' Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Clicks'
AND Technical_EventName='ProductNavigationClick'
--AND Product_Type='Sportsbook'
AND Interface_SectionName='Scrollable Popular Competitions Widget'

UNION ALL

----------BetFailed
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'BetFailed' Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook'
AND Technical_EventName IN ('BetFailed','ProductFailed')
AND EventAction='Bet Failed'

UNION ALL

----------MarketSelector
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,CASE 
  WHEN STARTS_WITH(EventAction, 'Market Selector Click') THEN 'MarketSelectorClick'
  WHEN STARTS_WITH(EventAction, 'Market Selector Applied') THEN 'MarketSelectorApplied'
 ELSE NULL
END AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,COALESCE(SAFE_CAST(REGEXP_EXTRACT(Sportsbook_CategoryDetails, r'(\d+)$') AS INT64),0) AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,CASE
    WHEN EventAction LIKE '%Applied%' THEN
        TRIM(
            CASE 
                WHEN INSTR(EventAction, '/') > 0 THEN
                    SUBSTR(EventAction, INSTR(EventAction, '/') + 1)
                ELSE
                    REGEXP_EXTRACT(EventAction, r'(\S+)$')  -- last word
            END
        )
    ELSE NULL
END AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook'
AND Technical_EventName IN ('MarketInteraction','MarketSelectorClick','MarketSelectorApplied')
AND (STARTS_WITH(EventAction, 'Market Selector Click') OR STARTS_WITH(EventAction, 'Market Selector Applied'))

UNION ALL

----------MultiMarketToggle
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'MultiMarketToggle' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,COALESCE(SAFE_CAST(REGEXP_EXTRACT(Sportsbook_CategoryDetails, r'(\d+)$') AS INT64),0) AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook'
AND Technical_EventName IN ('MarketInteraction','MultiMarketToggle')
AND STARTS_WITH(EventAction, 'Multi Market Toggle')

UNION ALL

----------PlaceBetClick
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'PlaceBetClick' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook'
AND Technical_EventName IN ('PlaceBetClick','SportsbookInteraction')
AND EventAction='Place Bet Click'

UNION ALL

----------EventVisit
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'EventVisit' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook'
AND Technical_EventName IN ('LiveEventVisit','SportsbookInteraction')
AND EventAction IN ("Live Event Visit", "Prematch Event Visit")

UNION ALL

----------LiveStreamInteraction
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'LiveStreamInteraction' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook'
AND Technical_EventName IN ('LiveStreamInteraction','SportsbookInteraction')
AND EventAction IN ('Live Stream Interaction', 'LiveStreamInteraction')
AND Interface_Component NOT IN ('Play', 'Pause', 'play', 'pause')
AND sub_area IN ('Coupon', 'Events Table')
AND interface_sectionname IN ('Bet History', 'Open Bets', 'Live Page')

UNION ALL

----------PinClick
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'PinClick' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook'
AND Technical_EventName IN ('PinClick','SportsbookInteraction')
AND EventAction IN ("Pin", "Unpin")

UNION ALL

----------PreferenceChange
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'PreferenceChange' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook'
AND Technical_EventName IN ('SportsbookPreferenceChange','SportsbookInteraction')
AND EventAction = 'Preference Change'

UNION ALL

----------BetShare
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,CASE WHEN Sub_Area = 'Bet Share Choose Size' THEN 'unknown' ELSE Sub_Area END AS Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'BetShare' AS Event
,Bonus_ID
,CASE WHEN EventDetails = 'Bet Share Choose Size' THEN 'unknown' ELSE EventDetails END  AS EventDetails
,Direction
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,CASE
    WHEN interface_component = 'Bet Share Choose Size - Bet Share' 
         THEN 'unknown'
    ELSE TRIM(SPLIT(interface_component, '-')[OFFSET(1)])  
 END as ItemClicked
,CASE 
WHEN ARRAY_LENGTH(SPLIT(interface_component, '-')) >= 3 THEN
TRIM(SPLIT(interface_component, '-')[OFFSET(ARRAY_LENGTH(SPLIT(interface_component, '-')) - 1)])
ELSE
NULL
END AS Option
,CASE
WHEN 
Interface_Component LIKE '%Bet Share Image%' OR
Interface_Component LIKE '%Bet Share Link%' OR
Interface_Component = 'Bet Share Choose Size - Bet Share'
THEN 'share'
ELSE 'open'
END AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND event_name IN ('Clicks','Sportsbook')
AND Technical_EventName ='LinkClick'
AND EventAction = 'LinkClick'
AND Interface_Component LIKE '%Bet Share%'
AND Interface_Component != 'Bet Share Choose Size - Hide Stake'
AND Interface_Component != 'Bet Share Choose Size - Show Stake'

UNION ALL

----------DeposittoPlaceBet
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'DeposittoPlaceBet' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,CASE 
WHEN ARRAY_LENGTH(SPLIT(interface_component, '-')) >= 3 THEN
TRIM(SPLIT(interface_component, '-')[OFFSET(ARRAY_LENGTH(SPLIT(interface_component, '-')) - 1)])
ELSE
NULL
END AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND event_name IN ('Clicks','Sportsbook')
AND Technical_EventName ='LinkClick'
AND EventAction = 'LinkClick'
AND Interface_Component LIKE '%Deposit to Place Bet%'

UNION ALL

----------TCI_BetHistory
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'TCI_BetHistory' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,CASE WHEN EventAction LIKE '%Click%' THEN
SPLIT(Interface_Component, '::')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(Interface_Component, '::')) - 1)] ELSE NULL END AS ItemClicked
,NULL AS Option
,NULL AS Type
,CASE WHEN EventAction LIKE '%Click%' THEN
SPLIT(Interface_Component, '::')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(Interface_Component, '::')) - 2)] ELSE 
SPLIT(Interface_Component, '::')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(Interface_Component, '::')) - 1)] END AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook' AND Technical_EventName ='TabbedComponentInteraction'
AND (Sub_Area = 'Bet History' OR EventDetails = 'Bet History')

UNION ALL

----------TCI_EventPage
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'TCI_EventPage' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,CASE WHEN EventAction LIKE '%Click%' THEN
SPLIT(Interface_Component, '::')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(Interface_Component, '::')) - 1)] ELSE NULL END AS ItemClicked
,NULL AS Option
,NULL AS Type
,CASE WHEN EventAction LIKE '%Click%' THEN
SPLIT(Interface_Component, '::')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(Interface_Component, '::')) - 2)] ELSE 
SPLIT(Interface_Component, '::')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(Interface_Component, '::')) - 1)] END AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Sportsbook' AND Technical_EventName ='TabbedComponentInteraction'
AND (Sub_Area = 'Event Page' OR EventDetails = 'Event Page')

UNION ALL

----------ProductFavouriteClick
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'ProductFavouriteClick' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,SAFE_CAST(REGEXP_EXTRACT(Interface_Component, r'-\s*(\d+)\s*$') AS INT64) AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,TRIM(SPLIT(EventAction, ' on ')[SAFE_OFFSET(1)]) AS SubCategoryID
,NULL AS ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Clicks' AND Technical_EventName ='ProductFavouriteClick'
AND STARTS_WITH(EventAction,'Favourite Selection on')

UNION ALL

----------CarouselCardClick
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'CarouselCardClick' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,CASE 
    WHEN EventAction LIKE '%Homepage%' OR EventAction LIKE '%Home%' OR EventAction LIKE '%Bet of the Day%' OR EventAction LIKE '%sportsbook Carousel%' OR EventAction LIKE '%Popular Pre-Built Bets%' THEN 'Homepage'
    WHEN EventAction LIKE '%Region%' OR EventAction LIKE '%region%' THEN 'Region Page'
    WHEN EventAction LIKE '%Category%' OR EventAction LIKE '%category%' THEN 'Category Page'
    WHEN EventAction LIKE '%Competition%' OR EventAction LIKE '%competition%' THEN 'Competition Page'
    ELSE 'Other'
END AS Location
,CASE 
    WHEN (EventAction LIKE '%Homepage%' OR EventAction LIKE '%sportsbook Carousel%') AND EventAction NOT LIKE '%Homepage Popular Bets%' THEN 'Carousel'
    WHEN EventAction LIKE '%Bet of the Day%' THEN 'BetoftheDay'
    WHEN EventAction LIKE '%Price Boost Widget%' THEN 'PriceBoostWidget'
    WHEN EventAction LIKE '%Popular Built Bets%' OR EventAction LIKE '%Popular Bets%' THEN 'PopularBets'
    WHEN EventAction LIKE '%Popular Pre Built Bets%' OR EventAction LIKE '%Popular Pre-Built Bets%' THEN 'PopularPreBuiltBets'
    WHEN EventAction LIKE '%Cards%' OR EventAction LIKE '%Competition Page%' OR EventAction LIKE '%Category Page%' OR EventAction LIKE '%Region Page%' THEN 'Cards'
    ELSE 'Other'
END AS Widget
,NULL AS EventPage_Filter
,CASE WHEN EventAction LIKE '%Category%' THEN SAFE_CAST(TRIM(REGEXP_EXTRACT(EventAction, r'-\s*(\d+)\s*$')) AS INT64) ELSE NULL END AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,CASE
    WHEN EventAction LIKE '%Competition%'
    THEN TRIM(REGEXP_EXTRACT(EventAction, r'-\s*(\d+)\s*$'))
    ELSE NULL
  END AS SubCategoryID
,NULL AS ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND
(Event_Name='Clicks' AND Technical_EventName ='CarouselCardClick'
AND (EventAction LIKE '%Homepage%'
     OR EventAction LIKE '%Category Page%'
     OR EventAction LIKE '%Popular Built Bets%'
     OR EventAction LIKE '%Popular Bets%'
     OR EventAction LIKE '%Bet of the Day%'
     OR EventAction LIKE '%Price Boost Widget%'
     OR EventAction LIKE '%Competition Page%'
     OR EventAction LIKE '%Popular Pre Built Bets%'
     OR EventAction LIKE '%Region Page%'))

UNION ALL

----------CarouselNavigationClick
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'CarouselNavigationClick' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,CASE 
    WHEN EventAction LIKE '%Homepage%' OR EventAction LIKE '%Home%' OR EventAction LIKE '%Bet of the Day%' OR EventAction LIKE '%sportsbook Carousel%' OR EventAction LIKE '%Popular Pre-Built Bets%' THEN 'Homepage'
    WHEN EventAction LIKE '%Region%' OR EventAction LIKE '%region%' THEN 'Region Page'
    WHEN EventAction LIKE '%Category%' OR EventAction LIKE '%category%' THEN 'Category Page'
    WHEN EventAction LIKE '%Competition%' OR EventAction LIKE '%competition%' THEN 'Competition Page'
    ELSE 'Other'
END AS Location
,CASE 
    WHEN (EventAction LIKE '%Homepage%' OR EventAction LIKE '%sportsbook Carousel%') AND EventAction NOT LIKE '%Homepage Popular Bets%' THEN 'Carousel'
    WHEN EventAction LIKE '%Bet of the Day%' THEN 'BetoftheDay'
    WHEN EventAction LIKE '%Price Boost Widget%' THEN 'PriceBoostWidget'
    WHEN EventAction LIKE '%Popular Built Bets%' OR EventAction LIKE '%Popular Bets%' THEN 'PopularBets'
    WHEN EventAction LIKE '%Popular Pre Built Bets%' OR EventAction LIKE '%Popular Pre-Built Bets%' THEN 'PopularPreBuiltBets'
    WHEN EventAction LIKE '%Cards%' OR EventAction LIKE '%Competition Page%' OR EventAction LIKE '%Category Page%' OR EventAction LIKE '%Region Page%' THEN 'Cards'
    ELSE 'Other'
END AS Widget
,NULL AS EventPage_Filter
,CASE WHEN EventAction LIKE '%Category%' THEN SAFE_CAST(TRIM(REGEXP_EXTRACT(EventAction, r'-\s*(\d+)\s*$')) AS INT64) ELSE NULL END AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,CASE
    WHEN EventAction LIKE '%Competition%'
    THEN TRIM(REGEXP_EXTRACT(EventAction, r'-\s*(\d+)\s*$'))
    ELSE NULL
  END AS SubCategoryID
,NULL AS ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND
(Event_Name='Clicks' AND Technical_EventName ='CarouselNavigationClick'
AND (EventAction LIKE '%Homepage%'
     OR EventAction LIKE '%Category Page%'
     OR EventAction LIKE '%Popular Built Bets%'
     OR EventAction LIKE '%Popular Bets%'
     OR EventAction LIKE '%Bet of the Day%'
     OR EventAction LIKE '%Price Boost Widget%'
     OR EventAction LIKE '%Competition Page%'
     OR EventAction LIKE '%Popular Pre Built Bets%'
     OR EventAction LIKE '%Region Page%'))

UNION ALL

----------NDCRDC
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,m.session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'NDCRDC' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m
INNER JOIN (
  SELECT session_id
  FROM `steam-mantis-108908.WPA_Events.00_Sessions`
  WHERE IsSportsbook = 1
) s
ON m.session_id = s.session_id

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Payments'
AND Technical_EventName IN ('ConfirmedDeposit','FirstDeposit')

UNION ALL

----------OpenDepPage
SELECT
--SESSION LEVEL COLUMNS
property_id,date,event_timestamp,m.session_id,user_pseudo_id,GUID,Key_ga_session_number,Key_session_engaged,Key_Interface_Brand,Key_Customer_Status_Start,Key_city,Key_country,Key_region,Key_Commercial_Area,Key_Technical_PlatformUsed,Key_mobile_brand_name,Key_device_web_info_browser,Key_device_web_info_browser_version,Key_device_category,Key_mobile_marketing_name,Key_mobile_model_name,Key_device_operating_system,Key_device_operating_system_version,Key_app_info_id,Key_app_info_version,Key_app_info_install_source
--GLOBAL DIMENSIONS
Key_device_web_info_hostname,Key_Interface_SiteLanguage,Key_Jurisdiction,content_group,event_name,Technical_EventName,EventAction,Key_Customer_Status_Event,Key_Login_Status,page_location,page_referrer,page_title,Key_Technical_PlatformName,Technical_ScreenOrientation,Technical_ScreenResolution,Key_User_CustomerLevel,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,'OpenDepPage' AS Event
,Bonus_ID
,Direction
,EventDetails
,Filters_Applied
,Interface_Component
,Interface_SectionName
,Interface_SearchTerm
,Slide_Number
,Sportsbook_BetType
,Sportsbook_CategoryDetails
,Sportsbook_CompetitionDetails
,Sportsbook_CouponType
,Sportsbook_EventDetails
,Sportsbook_EventPhase
,Sportsbook_MarketTemplateID
,Sportsbook_RememberStakeFlag
,Sportsbook_ViewType
,COALESCE(Sportsbook_TabName, TabName) AS Sportsbook_TabName
,Technical_Error
--CUSTOM COLUMNS
,NULL AS Location
,NULL AS Widget
,NULL AS EventPage_Filter
,NULL AS CategoryID
,NULL AS Cashout_Type
,NULL AS Filter
,NULL AS Currency
,NULL AS Amount
,NULL AS SubCategoryID
,NULL as ItemClicked
,NULL AS Option
,NULL AS Type
,NULL AS ComponentType

FROM `steam-mantis-108908.WPA.00_MasterTable` m
INNER JOIN (
  SELECT session_id
  FROM `steam-mantis-108908.WPA_Events.00_Sessions`
  WHERE IsSportsbook = 1
) s
ON m.session_id = s.session_id

--DATE FILTER
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
AND Event_Name='Payments'
AND Technical_EventName ='OpenDepPage'
AND EventAction='Open Deposit Page'
)
