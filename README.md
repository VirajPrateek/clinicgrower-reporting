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

