# 📊 Event Table Setup Guide

This repository provides a template to set up **new event tables**. The template allows you to **optionally include traffic and channel grouping columns** depending on your event’s requirements.

---

## 🔧 Available Template

| Template | Description |
|---------|-------------|
| **Default Template** | Events with optional traffic and channel grouping columns <br> ➤ Use: `000_create_event` & `000_refresh_event` |

> ⚠️ **Traffic and channel grouping columns are optional.** Only include them if your event requires this information.

---

## ✅ Instructions: How to Create a New Event Table

### 1. **Use the Template**
Start with the default template `000_create_event`.

---

### 2. **Customize the Query**

You'll find numbered placeholders in the template SQL query. Modify them as follows:

---

### *(1) Table Name*
Update the `"TABLE NAME"` field to reflect the correct naming convention.  
Tables should start with a prefix for the relevant area:

| Area         | Prefix Example     |
|--------------|------------------|
| Common Area  | `CA_`            |
| Sportsbook   | `SB_`            |
| Gaming       | `GX_`            |

**Examples:**
- `CA_NRC`
- `SB_Widget_Click`
- `GX_Searches`

---

### *(2) Event-Specific Dimensions*
Under the `-- EVENT SPECIFIC DIMENSIONS` section:
- Add only the **unique dimensions** for your event.
- **Do not include** dimensions already listed under `-- GLOBAL DIMENSIONS` (like `Content_Group`, `event_name`, `EventAction`, etc.) to avoid column duplication.
- Include traffic and channel grouping columns **only if needed**.

---

### *(3) Date Filter*
Set the appropriate **date range** for the data extraction.  
Update the date filter accordingly (e.g., for the last 30 days or a specific historical range).

---

### *(4) Event Filters*
Apply filters to accurately retrieve the desired event(s):
- Always include a filter on `event_name`.
- Add a filter on `event_action` if applicable.
- The more specific the filter, the better for performance and cost optimization.

> ✅ Efficient filtering = reduced consumption costs.

---

## 🔁 Instructions: How to Set Up the Refresh Table

Once your event table has been created, you'll also need to **schedule refreshes** to keep the data up to date.

### 1. **Use the Refresh Template**
Use the same template as for the creation: `000_refresh_event`.

---

### 2. **Copy the Original Query**
Start by copying the **same query** you used to create the event table.

---

### 3. **Apply the Following Changes**
- Replace the `CREATE OR REPLACE TABLE ... AS` statement with:

  ```sql
  INSERT INTO `steam-mantis-108908.WPA_Events.TABLE NAME`
Update the date filter to fetch only data for two days ago:
WHERE date = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
