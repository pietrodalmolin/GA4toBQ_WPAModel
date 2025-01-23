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
  The resulting tables are **partitioned by date** and **clustered by `event_name` and `event_action`**.
- **01_WPA: refresh tables**  
  This query is scheduled to run daily, refreshing the tables by appending the new data for each day. This query can be found 
