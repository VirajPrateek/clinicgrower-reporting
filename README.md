# clinicgrower-reporting
clinicgrower-reporting project is a part of a larger data pipelining and visualization project including but not limited to GA4, Meta Ads, Google ads, GMB, GHL etc

### GHL to GA4 Mapping Documentation

This document defines the key-value mappings used in the `gahl-to-ga4-custom-code` script for sending data from GoHighLevel to GA4 via Measurement Protocol.

| **Key (JSON Field)** | **Value (Variable)** | **Example** |
|----------------------|----------------------|-------------|
| `measurement_id`     | *Get from GA4*   | `G-XXXXXXXXXX` |
| `mp_secret`          | *Get from GA4*   | `mn235nsd9034nfsd` |
| `event_name`         | *Use as per funnel*| `subscribe_form_meta_ghl` |
| `appointment_id`     | `{{appointment.id}}` | – |
| `utm_source`         | `{{contact.lastAttributionSource.utmSource}}` | – |
| `utm_medium`         | `{{contact.lastAttributionSource.utmMedium}}` | – |
| `utm_term`           | `{{contact.lastAttributionSource.utmTerm}}` | – |
| `utm_content`        | `{{contact.attributionSource.utmContent}}` | – |
| `ga_client_id`       | `{{contact.lastAttributionSource.gaClientId}}` | – |
| `page_location`      | `{{contact.lastAttributionSource.url}}` | – |
| `page_referrer`      | `{{contact.lastAttributionSource.referrer}}` | – |
| `campaign_id`        | `{{contact.lastAttributionSource.campaignId}}` | – |
| `fbclid`             | `{{contact.lastAttributionSource.fbclid}}` | – |
| `session_source`     | `{{contact.lastAttributionSource.sessionSource}}` | – |
| `ad_id`              | `{{contact.lastAttributionSource.adId}}` | – |
| `ad_group_id`        | `{{contact.lastAttributionSource.adGroupId}}` | – |
| `utm_campaign`       | `{{contact.lastAttributionSource.utmCampaign}}` | – |

> 🔧 **Note:** Make sure `measurement_id`, `mp_secret`, and `event_name` are configured correctly based on your use case.

---
---


### GMB Metrics Pipeline (`gmb-pipeline.py`)

This script automates the ingestion of daily metrics from Google Business Profile (formerly Google My Business) into BigQuery using a scheduled Cloud Function.


#### 🔧 Overview

- Pulls performance data via `fetchMultiDailyMetricsTimeSeries` from the [Business Profile Performance API](https://developers.google.com/my-business/reference/businessinformation/rest)
- Transforms and pivots the metrics
- Inserts them into a partitioned BigQuery table
- Supports both **daily incremental loads** and **historical backfills**


#### ⚙️ Configuration Prerequisites

##### 🔐 1. **Secrets to Set in Secret Manager**

Create the following secrets in **Secret Manager** under your GCP project (`clinicgrower-reporting` by default):

| Secret ID             | Description                                      |
|-----------------------|--------------------------------------------------|
| `gmb-client-id`       | OAuth 2.0 Client ID (from Google Cloud Console) |
| `gmb-client-secret`   | OAuth 2.0 Client Secret                          |
| `gmb-refresh-token`   | Refresh token generated after user consent      |

> ✅ **Tip:** You can generate the refresh token using a one-time OAuth flow (see [OAuth Playground](https://developers.google.com/oauthplayground)).


##### 📦 2. **APIs to Enable in Google Cloud Console**

Make sure these APIs are enabled:

- [Secret Manager API](https://console.cloud.google.com/apis/library/secretmanager.googleapis.com)
- [BigQuery API](https://console.cloud.google.com/apis/library/bigquery.googleapis.com)
- [Cloud Functions API](https://console.cloud.google.com/apis/library/cloudfunctions.googleapis.com)
- [Cloud Scheduler API](https://console.cloud.google.com/apis/library/cloudscheduler.googleapis.com)
- [Cloud Logging API](https://console.cloud.google.com/apis/library/logging.googleapis.com)

> ⚠️ **Also required:**  
> - `IAM` roles to access Secrets (`Secret Manager Secret Accessor`)
> - `BigQuery Data Editor` or higher on the target dataset


##### 🔑 3. **How to Get Access to GMB (Business Profile) APIs**

1. **Apply for access** at the [Google Business Profile APIs access request form](https://developers.google.com/my-business/content/prereqs)
2. Google will review your intended use case and may take several days to approve
3. Once approved, you’ll be able to make requests to:
   - `https://mybusinessbusinessinformation.googleapis.com/`
   - `https://businessprofileperformance.googleapis.com/`

> 📝 Note: Access is granted at the account-level (e.g., a brand's account managing multiple locations).


##### 🔁 4. Daily Schedule

- The Cloud Function is triggered **daily at 4:00 AM UTC** using **Cloud Scheduler**
- Scheduler sends an HTTP POST request to the function with no body (default: past 3-day window)

Example payload for daily run:

```json
{}

##### 📥 5. Backfill Support
To backfill historical data, send a POST request to the Cloud Function with the following payload:
```json
{
  "backfill": true,
  "start_date": "2024-06-01",
  "end_date": "2024-06-30"
}
