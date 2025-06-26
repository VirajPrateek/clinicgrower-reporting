// Extract Client ID from ga_client_id, with a hardcoded dummy fallback
const rawClientId = inputData.ga_client_id; // Will be undefined if not available
const clientId = rawClientId && rawClientId.startsWith('GA1.1.') ? rawClientId.split('GA1.1.')[1] : (rawClientId || '999999999.1609459200'); // Hardcoded dummy in [randomNumber].[timestamp] format

// GA4 Measurement Protocol endpoint
const measurementId = inputData.measurement_id; // Will be undefined if not available
const apiSecret = inputData.mp_secret; // Will be undefined if not available
const url = `https://www.google-analytics.com/mp/collect?measurement_id=${measurementId}&api_secret=${apiSecret}`;

// Headers
const headers = {
  'Content-Type': 'application/json'
};

// Event data
const data = {
  client_id: clientId, // Will use hardcoded dummy if not available
  events: [{
    name: inputData.event_name, // Will be undefined if not available
    params: {
      source: inputData.utm_source !== undefined ? inputData.utm_source : undefined,
      medium: inputData.utm_medium !== undefined ? inputData.utm_medium : undefined,
      campaign: inputData.utm_campaign !== undefined ? inputData.utm_campaign : undefined,
      term: inputData.utm_term !== undefined ? inputData.utm_term : undefined,
      content: inputData.utm_content !== undefined ? inputData.utm_content : undefined,
      campaign_id: inputData.campaign_id !== undefined ? inputData.campaign_id : undefined,
      appointment_id: inputData.appointment_id !== undefined ? inputData.appointment_id : undefined,
      page_location: inputData.page_location !== undefined ? inputData.page_location : undefined,
      page_referrer: inputData.page_referrer !== undefined ? inputData.page_referrer : undefined,
      fbclid: inputData.fbclid !== undefined ? inputData.fbclid : undefined,
      session_source: inputData.session_source !== undefined ? inputData.session_source : undefined,
      ad_id: inputData.ad_id !== undefined ? inputData.ad_id : undefined,
      ad_group_id: inputData.ad_group_id !== undefined ? inputData.ad_group_id : undefined,
      engagement_time_msec: '100' // Required by GA4
    }
  }]
};

// Send POST request to GA4 using customRequest.post
try {
  const postResponse = await customRequest.post(url, {
    data,
    headers
  });

  // Check response status (treat 200 and 204 as success)
  if (postResponse.status === 200 || postResponse.status === 204) {
    return {
      status: 'success',
      message: 'Event sent to GA4'
    };
  } else {
    return {
      status: 'error',
      message: `Failed to send event: ${postResponse.status}`
    };
  }
} catch (error) {
  return {
    status: 'error',
    message: `Request failed: ${error.message}`
  };
}