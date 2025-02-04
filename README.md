# GA4 to BQ - WPA Data Model  

This set of queries processes data exported from GA4 properties to build a data infrastructure in BigQuery, which serves as the foundation for generating dashboards and ad hoc reports.  

In BigQuery, you will find three datasets:  
- **WPA:** `steam-mantis-108908.WPA`  
  Contains tables where each table represents a GA4 property. The tables are named according to their respective property IDs, and the source data is pulled from `steam-mantis-108908.analytics_*`.  
- **WPA_Tables:** `steam-mantis-108908.WPA_Tables`  
  Includes tables for each GA4 event, as well as a table for sessions. These tables are derived from the `steam-mantis-108908.WPA` dataset, utilizing the data contained there.   

## Queries  
### **WPA:** `steam-mantis-108908.WPA`  
- **01_WPA: create tables**  
  This query processes a list of GA4 properties and generates a set of tables. The source data comes from `steam-mantis-108908.analytics_*`.  
  The tables preserve the original schema, except for `event_params` and `user_properties`, from which values are extracted using `UNNEST()` to create separate columns for each key. The `ecommerce` and `items` objects are excluded from the tables.  
  The resulting tables are **partitioned by date** and **clustered by `event_name` and `event_action`**. They can be found under `steam-mantis-108908.WPA.*`, i.e. `steam-mantis-108908.WPA.270382730` for Betsson.com.  

- **01_WPA: refresh tables**  
  This query is scheduled to run daily, updating the tables by appending new data for each day. The scheduled query can be found under `Scheduled Queries > WPA: properties refresh`.  

### **WPA_Tables:** `steam-mantis-108908.WPA_Tables`  
- **02_WPA: lnd traffic - create table**  
  This query processes the tables stored under `steam-mantis-108908.WPA.*` to obtain last non direct traffic source and last non direct traffic medium for each `session_id`. The resulting table is `steam-mantis-108908.WPA_Tables.00_LastNonDirectTraffic`.  

- **02_WPA: lnd traffic - refresh table**  
  This query is scheduled to run daily, updating the table `steam-mantis-108908.WPA_Tables.00_LastNonDirectTraffic`. The scheduled query can be found under `Scheduled Queries > WPA: traffic refresh`.

- **02_WPA: 00_Sessions**  
  This query creates a table for Sessions. 

- **02_WPA: 00_Sessions**  
  This query creates a table for Pageviews. 

- **02_WPA: CA_LoginFormOpen**  
  This query creates a table for the GA4 event "Login Form Open".  

- **02_WPA: CA_LoginSubmittedFailed**  
  This query creates a table for the GA4 event "Login Submitted & Login Failed".  

- **02_WPA: CA_LoginSuccess**  
  This query creates a table for the GA4 event "Login Success".  

- **02_WPA: CA_RegistrationFormOpen**  
  This query creates a table for the GA4 event "Registration Form Open".  

- **02_WPA: CA_RegistrationFormFields**  
  This query creates a table for the GA4 event "Registration Form Fields".  

- **02_WPA: CA_RegistrationSubmit**  
  This query creates a table for the GA4 event "Registration Submit".  

- **02_WPA: CA_RegistrationFailed**  
  This query creates a table for the GA4 event "Registration Failed".  

- **02_WPA: CA_NRC**  
  This query creates a table for the GA4 event "NRC".  

- **02_WPA: CA_NDCRDC**  
  This query creates a table for the GA4 event "NDC & RDC".

- **02_WPA: CA_CarouselCardClick**  
  This query creates a table for the GA4 event "Carousel Card Click".

- **02_WPA: CA_CarouselNavigationClick**  
  This query creates a table for the GA4 event "CarouselNavigation Click".

- **02_WPA: SB_SearchClick**  
  This query creates a table for the GA4 event "SB Search Click".  

- **02_WPA: SB_QuickLinksScroller**  
  This query creates a table for the GA4 event "SB Quick Links Scroller".  

- **02_WPA: SB_MarketSelector**  
  This query creates a table for the GA4 event "SB Market Selector".

- **02_WPA: SB_FavouriteClick**  
  This query creates a table for the GA4 event "SB Favourite Click". 

- **02_WPA: SB_PlaceBetClick**  
  This query creates a table for the GA4 event "SB Place Bet Click".

- **02_WPA: SB_BetConfirmed**  
  This query creates a table for the GA4 event "SB Bet Confirmed". 

- **02_WPA: SB_BannerClick**  
  This query creates a table for the GA4 event "SB Banner Click". 

- **02_WPA: SB_BetFailed**  
  This query creates a table for the GA4 event "SB Bet Failed". 

- **02_WPA: SB_BetHistoryClick**  
  This query creates a table for the GA4 event "SB Bet History Click". 

- **02_WPA: SB_LinkClick**  
  This query creates a table for the GA4 event "SB Bet Share". 

- **02_WPA: SB_BurgerMenuClick**  
  This query creates a table for the GA4 event "SB Burger Menu". 

- **02_WPA: SB_ContentLinksClick**  
  This query creates a table for the GA4 event "SB Content Links Click". 

- **02_WPA: SB_LiveEventVisit**  
  This query creates a table for the GA4 event "SB Live Event Visit".

- **02_WPA: SB_WidgetClick**  
  This query creates a table for the GA4 event "SB Widget Click". 

- **02_WPA: GX_GamePlayed**  
  This query creates a table for the GA4 event "GX Game Played".

- **02_WPA: GX_Searches**  
  This query creates a table for the GA4 event "GX Searches". 
