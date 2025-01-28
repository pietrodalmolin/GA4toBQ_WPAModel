# GA4 to BQ - WPA Data Model  

This set of queries processes data exported from GA4 properties to build a data infrastructure in BigQuery, which serves as the foundation for generating dashboards and ad hoc reports.  

In BigQuery, you will find three datasets:  
- **WPA:** `steam-mantis-108908.WPA`  
  Contains tables where each table represents a GA4 property. The tables are named according to their respective property IDs, and the source data is pulled from `steam-mantis-108908.analytics_*`.  
- **WPA_Tables:** `steam-mantis-108908.WPA_Tables`  
  Includes tables for each GA4 event, as well as a table for sessions. These tables are derived from the `steam-mantis-108908.WPA` dataset, utilizing the data contained there.  
- **WPA_Views:** `steam-mantis-108908.WPA_Views`  
  Consists of views that correspond to the event tables in `steam-mantis-108908.WPA_Tables`. These views filter data for the previous day only and serve as the data source for Power BI imports.  

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
  This query processes the tables stored under `steam-mantis-108908.WPA.*` to obtain last non direct traffic source and last non direct traffic medium for each session_id. The resulting table is `steam-mantis-108908.WPA_Tables.00_LastNonDirectTraffic`
- **02_WPA: lnd traffic - refresh table** 
  This query is scheduled to run daily, updating the tables `steam-mantis-108908.WPA_Tables.00_LastNonDirectTraffic`. The scheduled query can be found under `Scheduled Queries > WPA: traffic refresh`.
- **02_WPA: CA_LoginFormOpen**
  This query creates a table for the GA4 event "Login Form Open"
- **02_WPA: CA_LoginSubmittedFailed**
  This query creates a table for the GA4 event "Login Submitted & Login Failed"
- **02_WPA: CA_LoginSuccess**
  This query creates a table for the GA4 event "Login Success"
  
  
