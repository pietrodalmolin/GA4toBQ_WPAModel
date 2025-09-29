# GA4 to BQ - WPA Data Model  

This set of queries processes data exported from GA4 properties to build a data infrastructure in BigQuery, which serves as the foundation for generating dashboards and ad hoc reports.  

In BigQuery, you will find the following datasets and tables:  

- **WPA:** `steam-mantis-108908.WPA`  
  Contains tables where each table represents a GA4 property. The tables are named according to their respective property IDs, and the source data is pulled from `steam-mantis-108908.analytics_*`.

- **Master Table:** `steam-mantis-108908.WPA.00_MasterTable`  
  This table consolidates data from all property tables in the WPA dataset. It serves as a single unified source of truth for downstream processing.

- **WPA_Events:** `steam-mantis-108908.WPA_Events`  
  Includes tables for each GA4 event, as well as a table for sessions. These tables are derived from the `00_MasterTable`, which simplifies event table creation and ensures consistency. This dataset is then used as the source for Power BI models.

> 📄 **Table Contents Reference:** Detailed information about the columns and contents of each table can be found [here](https://docs.google.com/spreadsheets/d/1kbcn0me3V68KXtBomR1Jp0nyFWvZQt0M-SZTtPt2PZA/edit?gid=0#gid=0)
