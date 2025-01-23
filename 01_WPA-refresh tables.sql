-- Declare variables for property IDs
DECLARE property_ids ARRAY<STRING>;
DECLARE property_id STRING;
DECLARE idx INT64 DEFAULT 0;

-- Initialize property IDs
SET property_ids = [
    '270389480', '345721586', '282392101', '326546621', '326546361',
    '326547182', '326497634', '344917462', '270369193', '323032716',
    '369344384', '326671725', '326660191', '326408170', '326649423',
    '270372273', '326629578', '326650420', '344980665', '326624957',
    '270382730', '435048955', '216367908', '270372101', '282543507',
    '282510694', '270352833', '270382267', '270412437', '347097957',
    '346483255', '270361811', '270410004', '326628458', '346955191',
    '270391072', '270400311', '282401733', '326507697', '270380201',
    '346480383', '270354118', '347130828', '347145362', '347121847',
    '347189984', '346938887', '216807831', '270355736', '346970928'
];

-- Loop through each property ID using an index
LOOP
  SET property_id = property_ids[SAFE_OFFSET(idx)];
  
  -- Exit the loop if no more property IDs
  IF property_id IS NULL THEN
    LEAVE;
  END IF;

  -- Append data to the existing table for each property ID
  EXECUTE IMMEDIATE (
    FORMAT("""
    INSERT INTO `steam-mantis-108908.WPA.%s`
      (
      SELECT
          '%s' AS property_id
          ,PARSE_TIMESTAMP('%%Y%%m%%d', CAST(event_date AS STRING)) AS date
          ,event_name
          ,* EXCEPT (event_date, event_name, event_params, user_properties, ecommerce, items)
          -- Extracting values from event_params using UNNEST()
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Bonus_EventPhase') AS Bonus_EventPhase
          ,COALESCE(
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Bonus_ID'), 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'Bonus_ID') AS STRING)
            ) AS Bonus_ID
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Bonus_Title') AS Bonus_Title
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Bonus_Type') AS Bonus_Type
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS campaign
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign_id') AS campaign_id
          ,COALESCE(
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'CardClicked'), 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'CardClicked') AS STRING)
            ) AS CardClicked
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Casino_CouponType') AS Casino_CouponType
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Casino_EventDetails') AS Casino_EventDetails
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'CategoryDetails') AS CategoryDetails
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'CD_Position') AS CD_Position
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'content') AS content
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'content_group') AS content_group
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency') AS currency
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Customer_Status_End') AS Customer_Status_End_event_params
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Customer_Status_Event') AS Customer_Status_Event
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'dclid') AS dclid
          ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'debug_mode') AS debug_mode
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Direction') AS Direction
          ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engaged_session_event') AS engaged_session_event
          ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS engagement_time_msec
          ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') AS entrances
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'EventAction') AS EventAction
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'EventDetails') AS EventDetails
          ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'firebase_conversion') AS firebase_conversion
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Full_Page_Url') AS Full_Page_Url
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Full_Url') AS Full_Url
          ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id
          ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS ga_session_number
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Game_Category') AS Game_Category
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Game_Collection_Position') AS Game_Collection_Position
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Game_Name') AS Game_Name
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Game_Provider') AS Game_Provider
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'GameProvider') AS GameProvider
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'gclid') AS gclid
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'gclsrc') AS gclsrc
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'GUID_Event') AS GUID_Event
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ignore_referrer') AS ignore_referrer
          ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'Interface_BannerPositon') AS Interface_BannerPositon
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Interface_Brand') AS Interface_Brand
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Interface_CardClicked') AS Interface_CardClicked
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Interface_Component') AS Interface_Component
          ,COALESCE( (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Interface_SearchTerm'), 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'Interface_SearchTerm') AS STRING),
            CAST((SELECT value.float_value FROM UNNEST(event_params) WHERE key = 'Interface_SearchTerm') AS STRING),
            CAST((SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'Interface_SearchTerm') AS STRING))
            AS Interface_SearchTerm
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Interface_SectionName') AS Interface_SectionName
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Interface_SiteLanguage') AS Interface_SiteLanguage
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Interface_WidgetName') AS Interface_WidgetName
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Jurisdiction') AS Jurisdiction
          ,COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Login_Balance'),
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'Login_Balance') AS STRING),
            CAST((SELECT value.float_value FROM UNNEST(event_params) WHERE key = 'Login_Balance') AS STRING),
            CAST((SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'Login_Balance') AS STRING))
            AS Login_Balance
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Login_Currency') AS Login_Currency
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Login_Method') AS Login_Method
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Login_Status') AS Login_Status
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'LoginStatus') AS LoginStatus
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS medium
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Native_App_Name') AS Native_App_Name
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Native_App_Version') AS Native_App_Version
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_hostname') AS page_hostname
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Page_Medium') AS Page_Medium
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer
          ,COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Page_Source'),
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'Page_Source') AS STRING),
            CAST((SELECT value.float_value FROM UNNEST(event_params) WHERE key = 'Page_Source') AS STRING),
            CAST((SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'Page_Source') AS STRING))
            AS Page_Source
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title
          ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'Payment_Amount') AS Payment_Amount
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Payment_Currency') AS Payment_Currency
          ,COALESCE( (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Payment_DepositID'),
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'Payment_DepositID') AS STRING),
            CAST((SELECT value.float_value FROM UNNEST(event_params) WHERE key = 'Payment_DepositID') AS STRING),
            CAST((SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'Payment_DepositID') AS STRING))
            AS Payment_DepositID
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Payment_DepositType') AS Payment_DepositType
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Payment_Method') AS Payment_Method
          ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'Payment_Sel_Amount') AS Payment_Sel_Amount
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Payment_Sel_Currency') AS Payment_Sel_Currency
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Payment_Status') AS Payment_Status
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Product_Theme') AS Product_Theme
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Raw_HostName') AS Raw_HostName
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Registration_Type') AS Registration_Type
          ,COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged'),
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'session_engaged') AS STRING),
            CAST((SELECT value.float_value FROM UNNEST(event_params) WHERE key = 'session_engaged') AS STRING),
            CAST((SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'session_engaged') AS STRING)) AS session_engaged
          ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'Slide_Number') AS Slide_Number
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS source
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_BetType') AS Sportsbook_BetType
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_CategoryDetails') AS Sportsbook_CategoryDetails
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_CompetitionDetails') AS Sportsbook_CompetitionDetails
          ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_CouponDetails') AS Sportsbook_CouponDetails
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_CouponType') AS Sportsbook_CouponType
          ,COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_EventDetails'),
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_EventDetails') AS STRING),
            CAST((SELECT value.float_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_EventDetails') AS STRING),
            CAST((SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_EventDetails') AS STRING))
            AS Sportsbook_EventDetails
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_EventPhase') AS Sportsbook_EventPhase
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_MarketName') AS Sportsbook_MarketName
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_MarketTemplateID') AS Sportsbook_MarketTemplateID
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_RememberStakeFlag') AS Sportsbook_RememberStakeFlag
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_TabName') AS Sportsbook_TabName
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Sportsbook_ViewType') AS Sportsbook_ViewType
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Sub_Area') AS Sub_Area
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_AdBlocker') AS Technical_AdBlocker
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_Error') AS Technical_Error
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_EventName') AS Technical_EventName
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_GTMContainerInfo') AS Technical_GTMContainerInfo
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_PageType') AS Technical_PageType
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_PlatformDelivery') AS Technical_PlatformDelivery
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_PlatformEnv') AS Technical_PlatformEnv
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_PlatformName') AS Technical_PlatformName
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_PlatformUsed') AS Technical_PlatformUsed
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_RawUserAgentInfo') AS Technical_RawUserAgentInfo
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_ReleaseName') AS Technical_ReleaseName
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_ReleaseVersion') AS Technical_ReleaseVersion
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_ScreenOrientation') AS Technical_ScreenOrientation
          ,COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_ScreenResolution'),
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'Technical_ScreenResolution') AS STRING),
            CAST((SELECT value.float_value FROM UNNEST(event_params) WHERE key = 'Technical_ScreenResolution') AS STRING),
            CAST((SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'Technical_ScreenResolution') AS STRING))
            AS Technical_ScreenResolution
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_TargetingConsent') AS Technical_TargetingConsent
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Technical_WindowType') AS Technical_WindowType
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'term') AS term
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Tournament_ID') AS Tournament_ID
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'Tournament_Type') AS Tournament_Type
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') AS transaction_id
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'User_Country') AS User_Country
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'User_CustomerLevel') AS User_CustomerLevel
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'User_Reg_Country') AS User_Reg_Country
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'User_Reg_KYCStatus') AS User_Reg_KYCStatus_event_params
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'User_Reg_Method') AS User_Reg_Method
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'User_Reg_Step') AS User_Reg_Step
          ,(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'User_UniqueID_Event') AS User_UniqueID_Event
          ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'value') AS value
          -- Extracting values from user_properties using UNNEST()
          ,(SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'Customer_Status_Acq') AS Customer_Status_Acq
          ,(SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'Customer_Status_End') AS Customer_Status_End_user_properties
          ,(SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'Customer_Status_Start') AS Customer_Status_Start
          ,(SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'Customer_Status_User') AS Customer_Status_User
          ,(SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'GUID_User') AS GUID_User
          ,(SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'user_id') AS user_id_user_properties
          ,COALESCE((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'User_Reg_KYCStatus'),
            CAST((SELECT value.int_value FROM UNNEST(user_properties) WHERE key = 'User_Reg_KYCStatus') AS STRING),
            CAST((SELECT value.float_value FROM UNNEST(user_properties) WHERE key = 'User_Reg_KYCStatus') AS STRING),
            CAST((SELECT value.double_value FROM UNNEST(user_properties) WHERE key = 'User_Reg_KYCStatus') AS STRING))
            AS User_Reg_KYCStatus_user_properties
          ,COALESCE( (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'User_UniqueID'),
            CAST((SELECT value.int_value FROM UNNEST(user_properties) WHERE key = 'User_UniqueID') AS STRING),
            CAST((SELECT value.float_value FROM UNNEST(user_properties) WHERE key = 'User_UniqueID') AS STRING),
            CAST((SELECT value.double_value FROM UNNEST(user_properties) WHERE key = 'User_UniqueID') AS STRING))
            AS User_UniqueID
          ,(SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'User_UniqueID_User') AS User_UniqueID_User              
      FROM `steam-mantis-108908.analytics_%s.events_*`
      
      --dynamic refresh
      WHERE
          _table_suffix = FORMAT_DATE('%%Y%%m%%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
      )
    """, property_id, property_id, property_id)
  );

  -- Increment index
  SET idx = idx + 1;
END LOOP;
