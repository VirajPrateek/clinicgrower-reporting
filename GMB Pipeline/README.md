# GMB Metrics Pipeline (`gmb-pipeline.py`)

This script automates the ingestion of daily metrics from Google Business Profile (formerly Google My Business) into BigQuery using a scheduled Cloud Function.

---

## 🔧 Overview

* Pulls performance data via `fetchMultiDailyMetricsTimeSeries` from the [Business Profile Performance API](https://developers.google.com/my-business/reference/businessinformation/rest)
* Transforms and pivots the metrics
* Inserts them into a partitioned BigQuery table
* Supports both **daily incremental loads** and **historical backfills**

---

## ⚙️ Configuration Prerequisites

### 🔐 1. Secrets to Set in Secret Manager

Create the following secrets in **Secret Manager** under your GCP project (`clinicgrower-reporting` by default):

| Secret ID           | Description                                     |
| ------------------- | ----------------------------------------------- |
| `gmb-client-id`     | OAuth 2.0 Client ID (from Google Cloud Console) |
| `gmb-client-secret` | OAuth 2.0 Client Secret                         |
| `gmb-refresh-token` | Refresh token generated after user consent      |

> ✅ Tip: You can generate the refresh token using a one-time OAuth flow (see [OAuth Playground](https://developers.google.com/oauthplayground)).

---

### 📦 2. APIs to Enable in Google Cloud Console

Enable these APIs:

* [Secret Manager API](https://console.cloud.google.com/apis/library/secretmanager.googleapis.com)
* [BigQuery API](https://console.cloud.google.com/apis/library/bigquery.googleapis.com)
* [Cloud Functions API](https://console.cloud.google.com/apis/library/cloudfunctions.googleapis.com)
* [Cloud Scheduler API](https://console.cloud.google.com/apis/library/cloudscheduler.googleapis.com)
* [Cloud Logging API](https://console.cloud.google.com/apis/library/logging.googleapis.com)

> ⚠️ Required IAM roles:
>
> * Secret Manager Secret Accessor
> * BigQuery Data Editor (or higher)

---

### 🔑 3. Request Access to Business Profile APIs

1. [Request access](https://developers.google.com/my-business/content/prereqs) to the Business Profile APIs
2. Await Google approval (may take several days)
3. Use the following API endpoints:

   * `https://mybusinessbusinessinformation.googleapis.com/`
   * `https://businessprofileperformance.googleapis.com/`

> 📝 Note: Access is granted per GMB account.

---

## 🔁 Daily Schedule

* The Cloud Function is triggered daily at **4:00 AM UTC** via **Cloud Scheduler**
* It fetches data for the previous 3 days

**Example payload for daily run:**

```json
{}
```

---

## 📥 Backfill Support

To backfill historical data, send a POST request to the Cloud Function with this payload:

```json
{
  "backfill": true,
  "start_date": "2024-06-01",
  "end_date": "2024-06-30"
}
```

---

## 🧾 Metrics Collected

* `BUSINESS_IMPRESSIONS_DESKTOP_MAPS`
* `BUSINESS_IMPRESSIONS_DESKTOP_SEARCH`
* `BUSINESS_IMPRESSIONS_MOBILE_MAPS`
* `BUSINESS_IMPRESSIONS_MOBILE_SEARCH`
* `BUSINESS_CONVERSATIONS`
* `BUSINESS_DIRECTION_REQUESTS`
* `CALL_CLICKS`
* `WEBSITE_CLICKS`
* `BUSINESS_BOOKINGS`
* `BUSINESS_FOOD_ORDERS`
* `BUSINESS_FOOD_MENU_CLICKS`

---

## 🗃 BigQuery Table Schema

* Table: `gmb_data.daily_metrics`
* Partitioned by `date`
* Includes fields: store\_id, account\_id, location\_id, location\_title, store\_code, is\_verified, metrics (as columns), load\_timestamp

---

## 🧪 Local Testing

Test with curl:

```bash
curl -X POST https://<function-url> \
  -H "Content-Type: application/json" \
  -d '{
    "backfill": true,
    "start_date": "2024-06-01",
    "end_date": "2024-06-03"
  }'
```

---

## ✅ Error Handling

* Logs pushed to **Cloud Logging**
* Tracks retries, rate limits (429), and permission errors (403)
* Failed locations are returned in the response object
