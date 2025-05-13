# 📊 Event Table Setup Guide

This repository provides two templates to set up new **event tables** based on whether or not **traffic and channel grouping** columns are required.

---

## 🔧 Available Templates

| Template | Description |
|---------|-------------|
| **(A)** | Events **with** traffic and channel grouping columns  <br> ➤ Use: `000_A_create_event` & `000_A_refresh_event` |
| **(B)** | Events **without** traffic and channel grouping columns <br> ➤ Use: `000_B_create_event` & `000_B_refresh_event` |

> ⚠️ **Use template B** if traffic information is **not** needed for the event you want to add.

---

## ✅ Instructions: How to Create a New Event Table

### 1. **Choose the Appropriate Template**

Start with either template A or B depending on whether traffic information is required.

---

### 2. **Customize the Query**

You'll find numbered placeholders in the template SQL query. Modify them as follows:

---

### *(1) Table Name*
Update the `"TABLE NAME"` field to reflect the correct naming convention.  
Tables should start with a prefix for the relevant area:

| Area         | Prefix Example     |
|--------------|--------------------|
| Common Area  | `CA_`              |
| Sportsbook   | `SB_`              |
| Gaming       | `GX_`              |

**Examples:**
- `CA_NRC`
- `SB_Widget_Click`
- `GX_Searches`

---

### *(2) Event-Specific Dimensions*
Under the `-- EVENT SPECIFIC DIMENSIONS` section:
- Add only the **unique dimensions** for your event.
- **Do not include** dimensions already listed under `-- GLOBAL DIMENSIONS` (like `Content_Group`, `event_name`, `EventAction`, etc.) to avoid column duplication.

---

### *(3) Properties Tables*
Update the list of properties tables to ensure accuracy.  
To find the latest list, refer to the query: 00_List of Properties (found in this repository).

---

### *(4) Date Filter*
Set the appropriate **date range** for the data extraction.  
Update the date filter accordingly (e.g., for the last 30 days or a specific historical range).

---

### *(5) Event Filters*
Apply filters to accurately retrieve the desired event(s):
- Always include a filter on `event_name`.
- Add a filter on `event_action` if applicable.
- The more specific the filter, the better for performance and cost optimization.

> ✅ Efficient filtering = reduced consumption costs.

---

### *(6) Group By Clause*
In the `GROUP BY` section:
- Remove the example event-specific columns.
- Add the event-specific columns you defined in step *(2)* to ensure proper grouping.

---

## 🧠 Final Tip

Be methodical and double-check that all fields and filters align with your event logic. A clean and optimized query helps ensure efficient processing and scalable table maintenance.

---
