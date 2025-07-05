# GHL to GA4 Mapping (`ghl-to-ga4-custom-code.js`)

This script is used to send custom event data from GoHighLevel (GHL) to Google Analytics 4 (GA4) using the Measurement Protocol.

---

## 🔗 Purpose

This enables GHL contact or appointment events to be tracked in GA4 even if they originate outside of a traditional website or tag manager setup.

---

## 📄 Mapping Table

| **Key (JSON Field)** | **Value (Variable)**                              | **Example**               |
| -------------------- | ------------------------------------------------- | ------------------------- |
| `measurement_id`     | *Get from GA4*                                    | `G-XXXXXXXXXX`            |
| `mp_secret`          | *Get from GA4*                                    | `mn235nsd9034nfsd`        |
| `event_name`         | *Use as per funnel*                               | `subscribe_form_meta_ghl` |
| `appointment_id`     | `{{appointment.id}}`                              | –                         |
| `utm_source`         | `{{contact.lastAttributionSource.utmSource}}`     | –                         |
| `utm_medium`         | `{{contact.lastAttributionSource.utmMedium}}`     | –                         |
| `utm_term`           | `{{contact.lastAttributionSource.utmTerm}}`       | –                         |
| `utm_content`        | `{{contact.attributionSource.utmContent}}`        | –                         |
| `ga_client_id`       | `{{contact.lastAttributionSource.gaClientId}}`    | –                         |
| `page_location`      | `{{contact.lastAttributionSource.url}}`           | –                         |
| `page_referrer`      | `{{contact.lastAttributionSource.referrer}}`      | –                         |
| `campaign_id`        | `{{contact.lastAttributionSource.campaignId}}`    | –                         |
| `fbclid`             | `{{contact.lastAttributionSource.fbclid}}`        | –                         |
| `session_source`     | `{{contact.lastAttributionSource.sessionSource}}` | –                         |
| `ad_id`              | `{{contact.lastAttributionSource.adId}}`          | –                         |
| `ad_group_id`        | `{{contact.lastAttributionSource.adGroupId}}`     | –                         |
| `utm_campaign`       | `{{contact.lastAttributionSource.utmCampaign}}`   | –                         |

> 🔧 **Note:** Ensure that `measurement_id`, `mp_secret`, and `event_name` are properly configured for the receiving GA4 property.

---

## ✅ Requirements

* A valid GA4 Measurement ID
* A secret key created under the Measurement Protocol API in GA4
* Mapping context variables should be extracted dynamically from the GHL webhook or form source

---

## 🚀 Deployment

* This script can be hosted as a backend endpoint or used within a serverless function
* It should receive POST requests from GHL and forward them to:
  `https://www.google-analytics.com/mp/collect`

---

## 📌 Notes

* Use `event_name` strategically to separate actions like `lead_submission`, `appointment_booked`, etc.
* You may want to enrich or deduplicate events using custom server logic before forwarding to GA4.
