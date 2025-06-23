import functions_framework
from google.cloud import secretmanager, bigquery
import requests
import logging
import json
import os
from urllib.parse import urlencode
from datetime import datetime, date, timedelta
from google.api_core.exceptions import NotFound

# Configure logging for Cloud Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
PROJECT_ID = os.environ.get('PROJECT_ID', 'clinicgrower-reporting')
DATASET_ID = os.environ.get('DATASET_ID', 'gmb_data')
TABLE_ID = os.environ.get('TABLE_ID', 'daily_metrics')
ACCOUNTS_URL = os.environ.get('ACCOUNTS_URL', 'https://mybusinessbusinessinformation.googleapis.com/v1/accounts')
LOCATIONS_URL_BASE = os.environ.get('LOCATIONS_URL_BASE', 'https://mybusinessbusinessinformation.googleapis.com/v1')
PERFORMANCE_URL_BASE = os.environ.get('PERFORMANCE_URL_BASE', 'https://businessprofileperformance.googleapis.com/v1')
STORE_ID = os.environ.get('STORE_ID', 'unknown_store')

# All available metrics
METRICS = [
    'BUSINESS_IMPRESSIONS_DESKTOP_MAPS',
    'BUSINESS_IMPRESSIONS_DESKTOP_SEARCH',
    'BUSINESS_IMPRESSIONS_MOBILE_MAPS',
    'BUSINESS_IMPRESSIONS_MOBILE_SEARCH',
    'BUSINESS_CONVERSATIONS',
    'BUSINESS_DIRECTION_REQUESTS',
    'CALL_CLICKS',
    'WEBSITE_CLICKS',
    'BUSINESS_BOOKINGS',
    'BUSINESS_FOOD_ORDERS',
    'BUSINESS_FOOD_MENU_CLICKS'
]

def get_secret(secret_id):
    """Retrieve a secret from Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(name=secret_name)
        return response.payload.data.decode('UTF-8')
    except Exception as e:
        logger.error(f"Error accessing secret {secret_id}: {e}")
        raise

def get_access_token():
    """Get access token using refresh token from Secret Manager."""
    try:
        client_id = get_secret('gmb-client-id')
        client_secret = get_secret('gmb-client-secret')
        refresh_token = get_secret('gmb-refresh-token')
        
        data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token'
        }
        response = requests.post('https://oauth2.googleapis.com/token', data=data)
        if response.status_code != 200:
            logger.error(f"Token refresh failed: {response.status_code} {response.text}")
            response.raise_for_status()
        access_token = response.json()['access_token']
        logger.info("Successfully refreshed access token")
        return access_token
    except requests.HTTPError as e:
        logger.error(f"HTTP error refreshing access token: {e}")
        raise
    except Exception as e:
        logger.error(f"Error refreshing access token: {e}")
        raise

def create_bigquery_table():
    """Create a date-partitioned BigQuery table with pivoted metric columns."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        dataset_ref = client.dataset(DATASET_ID)
        table_ref = dataset_ref.table(TABLE_ID)
        schema = [
            bigquery.SchemaField("store_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("location_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("location_title", "STRING"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        ]
        # Add a column for each metric
        for metric in METRICS:
            schema.append(bigquery.SchemaField(metric, "INTEGER", mode="NULLABLE"))
        schema.append(bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="REQUIRED"))
        
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date"
        )
        try:
            client.get_table(table_ref)
            logger.info(f"Table {DATASET_ID}.{TABLE_ID} already exists")
        except NotFound:
            client.create_table(table)
            logger.info(f"Created date-partitioned table {DATASET_ID}.{TABLE_ID}")
    except Exception as e:
        logger.error(f"Error creating BigQuery table: {e}")
        raise

def list_accounts(access_token):
    """List all GMB accounts."""
    try:
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(ACCOUNTS_URL, headers=headers)
        if response.status_code != 200:
            logger.error(f"Accounts request failed: {response.status_code} {response.text}")
            response.raise_for_status()
        data = response.json()
        accounts = data.get('accounts', [])
        if not accounts:
            logger.warning("No accounts returned")
        logger.info(f"Found {len(accounts)} accounts")
        return accounts
    except requests.HTTPError as e:
        logger.error(f"HTTP error listing accounts: {e}")
        return []
    except Exception as e:
        logger.error(f"Error listing accounts: {e}")
        return []

def list_locations(access_token, account_id):
    """List locations for a specific account."""
    try:
        locations_url = f"{LOCATIONS_URL_BASE}/accounts/{account_id}/locations?readMask=name,storeCode,title,websiteUri,relationshipData"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(locations_url, headers=headers)
        if response.status_code != 200:
            logger.error(f"Locations request failed for account {account_id}: {response.status_code} {response.text}")
            response.raise_for_status()
        data = response.json()
        locations = data.get('locations', [])
        if not locations:
            logger.warning(f"No locations returned for account {account_id}")
        logger.info(f"Found {len(locations)} locations for account {account_id}")
        return locations
    except requests.HTTPError as e:
        logger.error(f"HTTP error listing locations for account {account_id}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error listing locations for account {account_id}: {e}")
        return []

def fetch_performance_data(access_token, location_id, start_date, end_date):
    """Fetch performance data for a specific location and date range."""
    try:
        performance_url = f"{PERFORMANCE_URL_BASE}/locations/{location_id}:fetchMultiDailyMetricsTimeSeries"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        params = {
            'dailyRange.start_date.year': start_date.year,
            'dailyRange.start_date.month': start_date.month,
            'dailyRange.start_date.day': start_date.day,
            'dailyRange.end_date.year': end_date.year,
            'dailyRange.end_date.month': end_date.month,
            'dailyRange.end_date.day': end_date.day
        }
        for metric in METRICS:
            params[f'dailyMetrics'] = params.get('dailyMetrics', []) + [metric]
        query_string = urlencode(params, doseq=True)
        url = f"{performance_url}?{query_string}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.error(f"Performance request failed for location {location_id}: {response.status_code} {response.text}")
            response.raise_for_status()
        data = response.json()
        logger.info(f"Retrieved performance data for location {location_id}")
        return data
    except requests.HTTPError as e:
        logger.error(f"HTTP error fetching performance data for location {location_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error fetching performance data for location {location_id}: {e}")
        return None

def insert_metrics_to_bigquery(account_id, location_id, location_title, data):
    """Insert pivoted metrics into BigQuery."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        rows_to_insert = []
        load_timestamp = datetime.utcnow()
        
        # Group metrics by date
        metrics_by_date = {}
        for metric_series in data.get('multiDailyMetricTimeSeries', []):
            for daily_metric in metric_series.get('dailyMetricTimeSeries', []):
                metric_name = daily_metric.get('dailyMetric')
                time_series = daily_metric.get('timeSeries', {}).get('datedValues', [])
                for dated_value in time_series:
                    date_str = dated_value.get('date', {})
                    date_key = f"{date_str.get('year', 2025)}-{date_str.get('month', 3):02d}-{date_str.get('day', 1):02d}"
                    value = int(dated_value.get('value', 0))
                    if date_key not in metrics_by_date:
                        metrics_by_date[date_key] = {metric: 0 for metric in METRICS}
                    metrics_by_date[date_key][metric_name] = value
        
        # Create rows for each date
        for date_key, metric_values in metrics_by_date.items():
            row = {
                'store_id': STORE_ID,
                'account_id': account_id,
                'location_id': location_id,
                'location_title': location_title,
                'date': date_key,
                'load_timestamp': load_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            }
            for metric in METRICS:
                row[metric] = metric_values.get(metric, 0)
            rows_to_insert.append(row)
        
        if rows_to_insert:
            errors = client.insert_rows_json(table_ref, rows_to_insert)
            if errors:
                logger.error(f"Errors inserting rows to BigQuery: {errors}")
                return False
            logger.info(f"Inserted {len(rows_to_insert)} rows for location {location_id}")
            return True
        logger.warning(f"No data to insert for location {location_id}")
        return False
    except Exception as e:
        logger.error(f"Error inserting data to BigQuery for location {location_id}: {e}")
        return False

@functions_framework.http
def gmb_fetch_performance(request):
    """HTTP Cloud Run function to fetch GMB metrics and store in BigQuery."""
    try:
        # Initialize BigQuery table
        create_bigquery_table()
        
        # Get date range from request or default to March 2025
        request_json = request.get_json(silent=True) or {}
        if request_json.get('daily'):
            start_date = date.today() - timedelta(days=1)
            end_date = start_date
        else:
            start_date = date(2025, 3, 1)
            end_date = date(2025, 3, 31)
        
        access_token = get_access_token()
        
        # List all accounts
        accounts = list_accounts(access_token)
        if not accounts:
            logger.error("No accounts found")
            return {'status': 'error', 'message': 'No accounts found'}, 500
        
        # Process each account and its locations
        processed_locations = 0
        for account in accounts:
            account_id = account['name'].split('/')[-1]
            locations = list_locations(access_token, account_id)
            for location in locations:
                location_id = location['name'].split('/')[-1]
                location_title = location.get('title', 'Unknown')
                performance_data = fetch_performance_data(access_token, location_id, start_date, end_date)
                if performance_data:
                    if insert_metrics_to_bigquery(account_id, location_id, location_title, performance_data):
                        processed_locations += 1
                        logger.info(f"Successfully processed location {location_id} for account {account_id}")
                    else:
                        logger.warning(f"Failed to insert data for location {location_id}")
        
        return {
            'status': 'success',
            'message': f'Processed {processed_locations} locations for {len(accounts)} accounts from {start_date} to {end_date}'
        }, 200
    except Exception as e:
        logger.error(f"Function error: {str(e)}")
        return {'status': 'error', 'message': str(e)}, 500