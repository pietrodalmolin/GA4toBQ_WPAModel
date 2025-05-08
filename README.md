# GA4 to BQ - WPA Data Model  

This set of queries processes data exported from GA4 properties to build a data infrastructure in BigQuery, which serves as the foundation for generating dashboards and ad hoc reports.  

In BigQuery, you will find two datasets:  
- **WPA:** `steam-mantis-108908.WPA`  
  Contains tables where each table represents a GA4 property. The tables are named according to their respective property IDs, and the source data is pulled from `steam-mantis-108908.analytics_*`.  
- **WPA_Tables:** `steam-mantis-108908.WPA_Events`  
  Includes tables for each GA4 event, as well as a table for sessions. These tables are derived from the `steam-mantis-108908.WPA` dataset, utilizing the data contained there. This set of tables, is then used as source of data for Power BI Models.

## Queries  
### **WPA:** `steam-mantis-108908.WPA`  
- **00_WPA: create properties**  
  This query processes a list of GA4 properties and generates a set of tables. The source data comes from `steam-mantis-108908.analytics_*`.  
  The tables preserve the original schema, except for `event_params` and `user_properties`, from which values are extracted using `UNNEST()` to create separate columns for each key. The `ecommerce` and `items` objects are excluded from the tables.  
  The resulting tables are **partitioned by date** and **clustered by `event_name` and `event_action`**. They can be found under `steam-mantis-108908.WPA.*`, i.e. `steam-mantis-108908.WPA.270382730` for Betsson.com.  

- **00_WPA: refresh properties**  
  This query is scheduled to run daily, updating the tables by appending new data for each day. The scheduled query can be found under `Scheduled Queries > WPA: properties refresh`.

- **01_WPA: lnd traffic**  
  This query is scheduled to run daily, querying the tables `steam-mantis-108908.WPA.*` to obtain last non direct traffic source and last non direct traffic medium for each `session_id`. The target table is `steam-mantis-108908.WPA_Tables.00_LastNonDirectTraffic`.  

### **WPA_Events:** `steam-mantis-108908.WPA_Events`  

  All tables inside this database are events, including a dedicated table for Pageviews and a dedicated table for Sessions. Inside this repository you will find all queries to refresh these tables, they are named: "02_WPA: ..." 
