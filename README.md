# GA4 to BQ - WPA Data Model  

This set of queries processes data exported from GA4 properties to build a data infrastructure in BigQuery, which serves as the foundation for generating dashboards and ad hoc reports.  

In BigQuery, you will find two datasets:  
- **WPA:** `steam-mantis-108908.WPA`  
  Contains tables where each table represents a GA4 property. The tables are named according to their respective property IDs, and the source data is pulled from `steam-mantis-108908.analytics_*`.  
- **WPA_Events:** `steam-mantis-108908.WPA_Events`  
  Includes tables for each GA4 event, as well as a table for sessions. These tables are derived from the `steam-mantis-108908.WPA` dataset, utilizing the data contained there. This set of tables, is then used as source of data for Power BI Models.
