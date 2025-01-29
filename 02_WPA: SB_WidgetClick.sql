CREATE TABLE `steam-mantis-108908.WPA_Tables.SB_WidgetClick`
PARTITION BY DATE(Date)
CLUSTER BY property_id, event_name, eventaction AS
 (
 SELECT
--SESSION LEVEL COLUMNS
property_id,date,CONCAT(user_pseudo_id, ga_session_id) AS session_id,user_pseudo_id,ga_session_number,session_engaged,Interface_Brand,Customer_Status_Start,geo.city AS city,geo.country AS country,geo.region AS region,device.mobile_brand_name AS mobile_brand_name,device.browser AS device_browser,device.browser_version AS device_browser_version,device.category AS device_category,device.mobile_marketing_name AS mobile_marketing_name,device.mobile_model_name AS mobile_model_name,device.operating_system AS device_operating_system,device.operating_system_version AS device_operating_system_version
--GLOBAL DIMENSIONS
,Content_Group,event_name,EventAction,Login_Status,Sub_Area
--EVENT SPECIFIC DIMENSIONS
,Sportsbook_EventDetails
,Sportsbook_CategoryDetails
,Sportsbook_BetType
,Sportsbook_RememberStakeFlag
,Bonus_ID
,Bonus_EventPhase
,Bonus_Type
,Sportsbook_MarketName
,Sportsbook_MarketTemplateID
,Sportsbook_EventPhase
,Sportsbook_TabName
--CUSTOM COLUMNS
    ,CASE 
    WHEN LOWER(EventAction) LIKE '%add to betslip%' OR LOWER(EventAction) LIKE '%add to bet slip%' THEN
        CASE 
            WHEN LOWER(EventAction) LIKE '%event market group%' THEN 'Event Page'
            WHEN LOWER(EventAction) LIKE '%event table%' THEN 'Events Table'
            WHEN LOWER(EventAction) LIKE '%outrights%' THEN 'Events Table'
            WHEN LOWER(EventAction) LIKE '%carousel%' THEN 'Carousel'
            WHEN LOWER(EventAction) LIKE '%cards%' THEN 'Cards'
            WHEN LOWER(EventAction) LIKE '%popular bets%' THEN 'Popular Bets'
            WHEN LOWER(EventAction) LIKE '%popular pre-built bets%' THEN 'Popular Pre-Built Bets'
            WHEN LOWER(EventAction) LIKE '%price-boost%' THEN 'Price Boost (Page)'
            ELSE 'Other Widgets'
        END
    ELSE NULL
    END AS AtB_by_Widget    
    ,     CASE 
        WHEN EventAction LIKE '%Add to Betslip%' OR EventAction LIKE '%Add to Bet Slip%' THEN
            CASE 
                WHEN Sportsbook_TabName LIKE '%prematch%' THEN 'Prematch'
                WHEN Sportsbook_TabName LIKE '%common.upcoming%' THEN 'Starting Soon'
                WHEN Sportsbook_TabName LIKE '%common.live-and-upcoming%' THEN 'Live & Upcoming'
                WHEN Sportsbook_TabName LIKE '%common.live%' THEN 'Live'
                WHEN Sportsbook_TabName LIKE '%common.outrights%' THEN 'Outrights'
                WHEN Sportsbook_TabName LIKE '%common.competitions-and-leagues%' THEN 'Competitions & Leagues'
                WHEN Sportsbook_TabName LIKE '%common.all-leagues%' THEN 'All Leagues'
                WHEN Sportsbook_TabName LIKE '%undefined%' THEN 'Undefined'
                ELSE Sportsbook_TabName
            END
        ELSE NULL
    END AS AtB_EventsTable_TabName

    ,CASE 
    WHEN 
        (LOWER(EventAction) LIKE '%add to betslip%' OR LOWER(EventAction) LIKE '%add to bet slip%') THEN 
            CASE 
                -- Landing Page conditions
                WHEN LOWER(EventAction) LIKE '%sportsbook event table%' THEN 'Landing Page'
                WHEN LOWER(EventAction) LIKE '%carousel%' THEN 'Landing Page'
                WHEN LOWER(EventAction) LIKE '%popular bets%' THEN 'Landing Page'
                WHEN LOWER(EventAction) LIKE '%popular pre-built bets%' THEN 'Landing Page'
                
                -- Event Page
                WHEN LOWER(EventAction) LIKE '%event market group%' THEN 'Event Page, across multiple locations'
                
                -- Other Pages
                WHEN LOWER(EventAction) LIKE '%sportsbook.category%' THEN 'Category Page'
                WHEN LOWER(EventAction) LIKE '%sportsbook.competition%' THEN 'Competition Page'
                WHEN LOWER(EventAction) LIKE '%sportsbook.live-category%' THEN 'Live Page'
                WHEN LOWER(EventAction) LIKE '%sportsbook.live%' THEN 'Live Page'
                WHEN LOWER(EventAction) LIKE '%sportsbook.region%' THEN 'Region Page'
                WHEN LOWER(EventAction) LIKE '%sportsbook.search%' THEN 'After Search'
                WHEN LOWER(EventAction) LIKE '%sportsbook.starting-soon-tab%' THEN 'Starting Soon Page'
                WHEN LOWER(EventAction) LIKE '%sportsbook.live-stream-calendar%' THEN 'Live Stream Calendar Page'
                WHEN LOWER(EventAction) LIKE '%price-boost%' THEN 'Price Boost Page'
                
                ELSE 'Other Locations'
            END
    ELSE NULL  -- or whatever default value you prefer if the condition is not met
    END AS AtB_by_Location


    ,    CASE 
        WHEN (LOWER(EventAction) LIKE '%add to betslip%' OR LOWER(EventAction) LIKE '%add to bet slip%') 
             AND LOWER(EventAction) LIKE '%event market group%' THEN 
            REPLACE(
                RIGHT(
                    EventAction, 
                    CHAR_LENGTH(EventAction) - INSTR(EventAction, '(')
                ), 
                ')', 
                ''
            )
        ELSE NULL
    END AS AtB_EventPage_TabName

    ,    COALESCE(
        SAFE_CAST(
            TRIM(SUBSTR(Sportsbook_CategoryDetails, 
                         CASE 
                             WHEN INSTR(Sportsbook_CategoryDetails, '- ') > 0 THEN INSTR(Sportsbook_CategoryDetails, '- ') + 2
                             ELSE 0
                         END)) AS INT64
        ), 
        0
    ) AS SportID

    ,    CASE 
        WHEN INSTR(Sportsbook_EventDetails, ' - f') > 0 THEN 
            TRIM(SUBSTR(Sportsbook_EventDetails, 
                         1, 
                         INSTR(Sportsbook_EventDetails, ' - f') - 1))
        ELSE 
            NULL  -- or any default value you prefer
    END AS Sportsbook_EventName

--METRICS
,COUNT(*) AS Event_Count

FROM(
SELECT * FROM steam-mantis-108908.WPA.270389480 UNION ALL SELECT * FROM steam-mantis-108908.WPA.345721586 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282392101 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326546621 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326546361 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326547182 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326497634 UNION ALL SELECT * FROM steam-mantis-108908.WPA.344917462 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270369193 UNION ALL SELECT * FROM steam-mantis-108908.WPA.323032716 UNION ALL SELECT * FROM steam-mantis-108908.WPA.369344384 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326671725 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326660191 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326408170 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326649423 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270372273 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326629578 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326650420 UNION ALL SELECT * FROM steam-mantis-108908.WPA.344980665 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326624957 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270382730 UNION ALL SELECT * FROM steam-mantis-108908.WPA.435048955 UNION ALL SELECT * FROM steam-mantis-108908.WPA.216367908 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270372101 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282543507 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282510694 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270352833 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270382267 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270412437 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347097957 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346483255 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270410004 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326628458 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346955191 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270391072 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270400311 UNION ALL SELECT * FROM steam-mantis-108908.WPA.282401733 UNION ALL SELECT * FROM steam-mantis-108908.WPA.326507697 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270380201 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346480383 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270354118 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347130828 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347145362 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347121847 UNION ALL SELECT * FROM steam-mantis-108908.WPA.347189984 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346938887 UNION ALL SELECT * FROM steam-mantis-108908.WPA.216807831 UNION ALL SELECT * FROM steam-mantis-108908.WPA.270355736 UNION ALL SELECT * FROM steam-mantis-108908.WPA.346970928
) wpa

WHERE wpa.date <='2025-01-26'
--EVENT FILTERS
AND Event_Name='Sportsbook' --filter event_name
AND EventAction LIKE 'SB Widget Click -%' --filter event_action

GROUP BY
--SESSION LEVEL COLUMNS
property_id,date,CONCAT(user_pseudo_id, ga_session_id),user_pseudo_id,ga_session_number,session_engaged,Interface_Brand,Customer_Status_Start,geo.city,geo.country,geo.region,device.mobile_brand_name,device.browser,device.browser_version,device.category,device.mobile_marketing_name,device.mobile_model_name,device.operating_system,device.operating_system_version
--GLOBAL DIMENSIONS
,Content_Group,event_name,EventAction,Login_Status,Sub_Area
    --EVENT SPECIFIC DIMENSIONS
    ,Sportsbook_EventDetails
    ,Sportsbook_CategoryDetails
    ,Sportsbook_BetType
    ,Sportsbook_RememberStakeFlag
    ,Bonus_ID
    ,Bonus_EventPhase
    ,Bonus_Type
    ,Sportsbook_MarketName
    ,Sportsbook_MarketTemplateID
    ,Sportsbook_EventPhase
    ,Sportsbook_TabName
)
