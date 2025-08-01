import functions_framework
from google.cloud import secretmanager, bigquery
import requests
import logging
import json
import os
from urllib.parse import urlencode
from datetime import datetime, date, timedelta
from google.api_core.exceptions import NotFound

# Configure logging for Cloud Logging (minimal)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
PROJECT_ID = os.environ.get('PROJECT_ID', 'clinicgrower-reporting')
DATASET_ID = os.environ.get('DATASET_ID', 'gmb_data')
TABLE_ID = os.environ.get('TABLE_ID', 'daily_metrics')
ACCOUNTS_URL = os.environ.get('ACCOUNTS_URL', 'https://mybusinessbusinessinformation.googleapis.com/v1/accounts')
LOCATIONS_URL_BASE = os.environ.get('LOCATIONS_URL_BASE', 'https://mybusinessbusinessinformation.googleapis.com/v1')
PERFORMANCE_URL_BASE = os.environ.get('PERFORMANCE_URL_BASE', 'https://businessprofileperformance.googleapis.com/v1')

# Metrics from fetchMultiDailyMetricsTimeSeries
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
        return response.json()['access_token']
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
            bigquery.SchemaField("store_code", "STRING"),
            bigquery.SchemaField("is_verified", "BOOLEAN"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        ]
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
        except NotFound:
            client.create_table(table)
    except Exception as e:
        logger.error(f"Error creating BigQuery table: {e}")
        raise

def list_accounts(access_token, target_account_ids=None):
    """List all GMB accounts with pagination."""
    accounts = []
    page_token = None
    try:
        headers = {'Authorization': f'Bearer {access_token}'}
        while True:
            params = {'pageSize': 100}
            if page_token:
                params['pageToken'] = page_token
            response = requests.get(ACCOUNTS_URL, headers=headers, params=params)
            if response.status_code != 200:
                logger.error(f"Accounts request failed: {response.status_code} {response.text}")
                response.raise_for_status()
            data = response.json()
            new_accounts = data.get('accounts', [])
            if target_account_ids:
                new_accounts = [a for a in new_accounts if a['name'].split('/')[-1] in target_account_ids]
            accounts.extend(new_accounts)
            page_token = data.get('nextPageToken')
            if not page_token:
                break
        logger.info(f"Found {len(accounts)} accounts")
        return accounts
    except requests.HTTPError as e:
        logger.error(f"HTTP error listing accounts: {e}")
        return []
    except Exception as e:
        logger.error(f"Error listing accounts: {e}")
        return []

def list_locations(access_token, account_id):
    """List only verified locations for a specific account with pagination."""
    locations = []
    page_token = None
    try:
        headers = {'Authorization': f'Bearer {access_token}'}
        while True:
            params = {'readMask': 'name,storeCode,title,metadata', 'pageSize': 100}
            if page_token:
                params['pageToken'] = page_token
            locations_url = f"{LOCATIONS_URL_BASE}/accounts/{account_id}/locations"
            response = requests.get(locations_url, headers=headers, params=params)
            if response.status_code != 200:
                logger.error(f"Locations request failed for account {account_id}: {response.status_code} {response.text}")
                response.raise_for_status()
            data = response.json()
            new_locations = [loc for loc in data.get('locations', []) if loc.get('metadata', {}).get('hasVoiceOfMerchant', False)]
            locations.extend(new_locations)
            page_token = data.get('nextPageToken')
            if not page_token:
                break
        logger.info(f"Found {len(locations)} verified locations for account {account_id}")
        return locations
    except requests.HTTPError as e:
        logger.error(f"HTTP error listing locations for account {account_id}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error listing locations for account {account_id}: {e}")
        return []

def fetch_performance_data(access_token, location_id, location_title, store_code, is_verified, start_date, end_date, max_retries=3, delay=2):
    """Fetch performance data for a specific location and date range with retries."""
    performance_url = f"{PERFORMANCE_URL_BASE}/locations/{location_id}:fetchMultiDailyMetricsTimeSeries"
    headers = {'Authorization': f'Bearer {access_token}'}
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
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logger.error(f"Failed for location {location_id}: {response.status_code} {response.text}")
                response.raise_for_status()
            data = response.json()
            logger.info(f"Raw response for {location_id}: {json.dumps(data)}")
            return data
        except requests.HTTPError as e:
            if response.status_code == 403:
                logger.error(f"Permission denied for location {location_id}: {response.text}")
                return None
            if response.status_code == 429:
                if attempt == max_retries - 1:
                    logger.error(f"Max retries reached for location {location_id}")
                    return None
                from time import sleep
                sleep(delay * (2 ** attempt))
            else:
                logger.error(f"HTTP error for location {location_id}: {e}")
                return None
        except Exception as e:
            logger.error(f"Error fetching data for location {location_id}: {e}")
            return None
    return None

def insert_metrics_to_bigquery(account_id, location_id, location_title, store_code, is_verified, data, start_date, end_date, backfill=False):
    """Insert or update metrics into BigQuery using MERGE for backfill or daily overwrite."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        
        rows_to_insert = []
        load_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')  # Format as TIMESTAMP string
        metrics_by_date = {}

        if data and 'multiDailyMetricTimeSeries' in data:
            for metric_series in data.get('multiDailyMetricTimeSeries', []):
                for daily_metric in metric_series.get('dailyMetricTimeSeries', []):
                    metric_name = daily_metric.get('dailyMetric')
                    time_series = daily_metric.get('timeSeries', {}).get('datedValues', [])
                    for dated_value in time_series:
                        try:
                            date_key = date(
                                int(dated_value['date']['year']),
                                int(dated_value['date']['month']),
                                int(dated_value['date']['day'])
                            ).isoformat()  # Keep as ISO string for DATE casting
                            date_obj = date.fromisoformat(date_key)
                            # Filter dates for backfill
                            if backfill and (date_obj < start_date or date_obj > end_date):
                                logger.warning(f"Skipping date {date_key} outside backfill range {start_date} to {end_date} for location {location_id}")
                                continue
                        except Exception as e:
                            logger.warning(f"Invalid date format for location {location_id}: {e}")
                            continue
                        value = int(dated_value.get('value', 0))
                        if value == 0:
                            logger.warning(f"Zero value for {metric_name} on {date_key} for {location_id}")
                        if date_key not in metrics_by_date:
                            metrics_by_date[date_key] = {metric: 0 for metric in METRICS}
                        metrics_by_date[date_key][metric_name] = value

        for date_key, metric_values in metrics_by_date.items():
            row = {
                'store_id': store_code or 'unknown_store',
                'account_id': account_id,
                'location_id': location_id,
                'location_title': location_title,
                'store_code': store_code or 'unknown_store',
                'is_verified': bool(is_verified),  # Ensure boolean type
                'date': date_key,  # ISO string, will be cast to DATE
                'load_timestamp': load_timestamp  # TIMESTAMP string
            }
            for metric in METRICS:
                row[metric] = metric_values.get(metric, 0)
            rows_to_insert.append(row)

        if rows_to_insert:
            # Prepare rows as an array of structs for BigQuery
            struct_rows = [
                {
                    'store_id': row['store_id'],
                    'account_id': row['account_id'],
                    'location_id': row['location_id'],
                    'location_title': row['location_title'],
                    'store_code': row['store_code'],
                    'is_verified': row['is_verified'],
                    'date': row['date'],  # ISO string for DATE
                    **{metric: row[metric] for metric in METRICS},
                    'load_timestamp': row['load_timestamp']  # TIMESTAMP string
                }
                for row in rows_to_insert
            ]

            # Use MERGE with ArrayQueryParameter
            merge_query = f"""
                WITH rows AS (
                    SELECT * FROM UNNEST(@rows)
                )
                MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
                USING rows S
                ON T.location_id = S.location_id AND T.date = S.date
                WHEN MATCHED THEN
                    UPDATE SET
                        T.store_id = S.store_id,
                        T.account_id = S.account_id,
                        T.location_title = S.location_title,
                        T.store_code = S.store_code,
                        T.is_verified = S.is_verified,
                        T.load_timestamp = S.load_timestamp,
                        {', '.join([f'T.{metric} = S.{metric}' for metric in METRICS])}
                WHEN NOT MATCHED THEN
                    INSERT (store_id, account_id, location_id, location_title, store_code, is_verified, date, {', '.join(METRICS)}, load_timestamp)
                    VALUES (S.store_id, S.account_id, S.location_id, S.location_title, S.store_code, S.is_verified, S.date, {', '.join([f'S.{metric}' for metric in METRICS])}, S.load_timestamp)
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("rows", "STRUCT<store_id STRING, account_id STRING, location_id STRING, location_title STRING, store_code STRING, is_verified BOOLEAN, date DATE, " + ", ".join([f"{metric} INTEGER" for metric in METRICS]) + ", load_timestamp TIMESTAMP>", struct_rows)
                ],
                use_legacy_sql=False
            )
            query_job = client.query(merge_query, job_config=job_config)
            query_job.result()  # Wait for the job to complete
            logger.info(f"Updated/Inserted {len(rows_to_insert)} rows.")
            return len(rows_to_insert)
        return 0
    except Exception as e:
        logger.error(f"Error inserting/updating data for location {location_id}: {e}")
        return 0

@functions_framework.http
def gmb_fetch_performance(request):
    """HTTP Cloud Run function to fetch GMB metrics and store in BigQuery."""
    try:
        create_bigquery_table()
        request_json = request.get_json(silent=True) or {}
        
        # Determine date range
        backfill = request_json.get('backfill', False)
        if backfill:
            try:
                start_date = datetime.strptime(request_json['start_date'], '%Y-%m-%d').date()
                end_date = datetime.strptime(request_json['end_date'], '%Y-%m-%d').date()
                if start_date > end_date:
                    raise ValueError("start_date must be before or equal to end_date")
            except (KeyError, ValueError) as e:
                logger.error(f"Invalid backfill parameters: {e}")
                return {'status': 'error', 'message': f'Invalid backfill parameters: {e}', 'failed_locations': []}, 400
        else:
            end_date = (datetime.utcnow() - timedelta(days=2)).date()  # T-2 to allow data processing
            start_date = end_date - timedelta(days=6)  # T-8 to T-2 (7 days)
        logger.info(f"Starting {'backfill' if backfill else 'daily'} run for {start_date} to {end_date}")
        
        # Get target account IDs if specified
        target_account_ids = request_json.get('target_account_ids', None)
        
        access_token = get_access_token()
        all_accounts = list_accounts(access_token, target_account_ids)
        if not all_accounts:
            logger.error("No accounts found")
            return {'status': 'error', 'message': 'No accounts found', 'failed_locations': []}, 500
        
        # Process all accounts
        accounts = all_accounts
        
        processed_locations = 0
        failed_locations = []
        total_rows = 0
        for account in accounts:
            account_id = account['name'].split('/')[-1]
            locations = list_locations(access_token, account_id)
            for location in locations:
                location_id = location['name'].split('/')[-1]
                location_title = location.get('title', 'Unknown')
                store_code = location.get('storeCode', 'unknown_store')
                is_verified = location.get('metadata', {}).get('hasVoiceOfMerchant', True)
                performance_data = fetch_performance_data(access_token, location_id, location_title, store_code, is_verified, start_date, end_date)
                if performance_data:
                    rows_inserted = insert_metrics_to_bigquery(account_id, location_id, location_title, store_code, is_verified, performance_data, start_date, end_date)
                    if rows_inserted > 0:
                        processed_locations += 1
                        total_rows += rows_inserted
                    else:
                        failed_locations.append({
                            'account_id': account_id,
                            'location_id': location_id,
                            'location_title': location_title,
                            'store_code': store_code,
                            'is_verified': is_verified,
                            'error': 'Insert failed'
                        })
                else:
                    failed_locations.append({
                        'account_id': account_id,
                        'location_id': location_id,
                        'location_title': location_title,
                        'store_code': store_code,
                        'is_verified': is_verified,
                        'error': 'No performance data'
                    })
        
        # Log total rows inserted
        logger.info(f"Total rows inserted/updated: {total_rows}")
        
        response = {
            'status': 'success' if processed_locations > 0 else 'partial_success',
            'message': f'Processed {processed_locations} locations for {len(accounts)} accounts from {start_date} to {end_date}',
            'failed_locations': failed_locations
        }
        logger.info(f"Response: {json.dumps(response)}")
        return response, 200 if processed_locations > 0 else 500
    except Exception as e:
        logger.error(f"Function error: {str(e)}")
        return {'status': 'error', 'message': str(e), 'failed_locations': []}, 500