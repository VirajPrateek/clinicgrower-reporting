import requests
import json
import time
import logging
import os
from datetime import datetime, timedelta
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration from environment variables
MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_API_TOKEN = os.environ.get('MONDAY_API_TOKEN')
BOARD_ID = os.environ.get('BOARD_ID', '1981285971')

# BigQuery Configuration
PROJECT_ID = os.environ.get('GCP_PROJECT', 'clinicgrower-reporting')
DATASET_ID = "dashboard_views"
VIEW_NAME = "metaads_ga4_spend_leads_pivot"
FULL_VIEW_PATH = f"{PROJECT_ID}.{DATASET_ID}.{VIEW_NAME}"

# Group IDs to include (only process items in these groups)
INCLUDE_GROUPS = [
    'new_group55979',
    'duplicate_of_90_day_campaign',
    'new_group45032'
]

# Group IDs to exclude
EXCLUDE_GROUPS = [
    'group_title'
]

# Column Mapping
FB_METRICS_COLUMNS = {
    'fb_leads_2_days': 'numbers4',
    'fb_leads_7_days': 'numeric9',
    'fb_leads_30_days': 'numeric0',
    'fb_cpl_7_days': 'numbers01',
    'fb_cpl_30_days': 'numbers49',
    'fb_mtd_spend': 'numbers27',
    'fb_last_month_spend': 'numbers04',
    'fb_spend_30_days': 'numbers3',
    'fb_as_of_date': 'date_mkwars37'
}

# Lead source columns
LEAD_COLUMNS = [
    'subscribe_survey_meta_ghl',
    'subscribe_form_meta_ghl',
    'subscribe_chat_fbfunnel_ghl'
]

class ProductionPipeline:
    def __init__(self):
        if not MONDAY_API_TOKEN:
            raise ValueError("MONDAY_API_TOKEN environment variable not set")
        
        self.monday_headers = {"Authorization": MONDAY_API_TOKEN}
        self.bq_client = None
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'no_data': 0,
            'no_cgid': 0,
            'filtered_out': 0,
            'errors': []
        }
        
    def init_bigquery(self):
        """Initialize BigQuery client"""
        logging.info("Initializing BigQuery connection...")
        try:
            self.bq_client = bigquery.Client(project=PROJECT_ID)
            logging.info("✅ BigQuery client initialized")
            return True
        except Exception as e:
            logging.error(f"❌ Failed to initialize BigQuery: {str(e)}")
            return False
    
    def get_all_items(self):
        """Get all items from Monday board with pagination"""
        logging.info("Fetching all items from Monday board...")
        
        all_items = []
        cursor = None
        page = 1
        
        while True:
            # Build query with cursor for pagination
            if cursor:
                query = '''
                query ($board_id: ID!, $cursor: String) {
                    boards(ids: [$board_id]) {
                        items_page(limit: 100, cursor: $cursor) {
                            cursor
                            items {
                                id
                                name
                                group {
                                    id
                                    title
                                }
                                column_values {
                                    id
                                    value
                                }
                            }
                        }
                    }
                }
                '''
                variables = {"board_id": BOARD_ID, "cursor": cursor}
            else:
                query = '''
                query ($board_id: ID!) {
                    boards(ids: [$board_id]) {
                        items_page(limit: 100) {
                            cursor
                            items {
                                id
                                name
                                group {
                                    id
                                    title
                                }
                                column_values {
                                    id
                                    value
                                }
                            }
                        }
                    }
                }
                '''
                variables = {"board_id": BOARD_ID}
            
            try:
                response = requests.post(
                    MONDAY_API_URL,
                    json={"query": query, "variables": variables},
                    headers=self.monday_headers,
                    timeout=60
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if 'errors' in data:
                        logging.error(f"API error: {data['errors']}")
                        break
                    
                    items_page = data['data']['boards'][0]['items_page']
                    items = items_page['items']
                    
                    if not items:
                        break
                    
                    all_items.extend(items)
                    logging.info(f"  Page {page}: Fetched {len(items)} items (Total so far: {len(all_items)})")
                    
                    # Check if there are more pages
                    cursor = items_page.get('cursor')
                    if not cursor:
                        break
                    
                    page += 1
                    time.sleep(0.5)  # Rate limiting between pages
                    
                else:
                    logging.error(f"HTTP error: {response.status_code}")
                    break
                    
            except Exception as e:
                logging.error(f"Error fetching items: {str(e)}")
                break
        
        logging.info(f"✅ Total items fetched: {len(all_items)}")
        return all_items
    
    def should_process_item(self, item):
        """Check if item should be processed based on group filters"""
        group = item.get('group')
        if not group:
            return False
        
        group_id = group.get('id')
        
        # Exclude if in exclude list
        if group_id in EXCLUDE_GROUPS:
            return False
        
        # Include only if in include list
        return group_id in INCLUDE_GROUPS
    
    def extract_cgid(self, item):
        """Extract CGID from Monday item (from text25 column)"""
        for col in item['column_values']:
            if col['id'] == 'text25':  # CGID column
                if col['value']:
                    try:
                        cgid = json.loads(col['value'])
                        return cgid
                    except:
                        return None
        return None
    
    def get_date_range_data(self, cgid: str, start_date: str, end_date: str):
        """Get aggregated data for a specific CGID and date range"""
        
        try:
            query = f"""
            SELECT 
                cgid,
                client_name,
                SUM(subscribe_survey_meta_ghl) as survey_leads,
                SUM(subscribe_form_meta_ghl) as form_leads,
                SUM(subscribe_chat_fbfunnel_ghl) as chat_leads,
                SUM(total_spend) as total_spend
            FROM `{FULL_VIEW_PATH}`
            WHERE cgid = '{cgid}'
              AND report_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY cgid, client_name
            """
            
            df = self.bq_client.query(query).to_dataframe()
            
            if len(df) == 0:
                return None
            
            row = df.iloc[0]
            
            # Calculate total leads
            total_leads = (
                row['survey_leads'] + 
                row['form_leads'] + 
                row['chat_leads']
            )
            
            # Calculate CPL
            cpl = round(row['total_spend'] / total_leads, 2) if total_leads > 0 else 0
            
            return {
                'leads': int(total_leads),
                'spend': float(row['total_spend']),
                'cpl': cpl
            }
            
        except Exception as e:
            logging.error(f"Error getting data for {cgid}: {str(e)}")
            return None
    
    def calculate_metrics(self, cgid: str, as_of_date: str = None):
        """Calculate all metrics for a CGID"""
        
        if as_of_date is None:
            as_of_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        as_of = datetime.strptime(as_of_date, '%Y-%m-%d')
        
        # Calculate date ranges
        date_ranges = {
            '2_days': {
                'start': (as_of - timedelta(days=1)).strftime('%Y-%m-%d'),
                'end': as_of.strftime('%Y-%m-%d')
            },
            '7_days': {
                'start': (as_of - timedelta(days=6)).strftime('%Y-%m-%d'),
                'end': as_of.strftime('%Y-%m-%d')
            },
            '30_days': {
                'start': (as_of - timedelta(days=29)).strftime('%Y-%m-%d'),
                'end': as_of.strftime('%Y-%m-%d')
            },
            'mtd': {
                'start': as_of.replace(day=1).strftime('%Y-%m-%d'),
                'end': as_of.strftime('%Y-%m-%d')
            },
            'last_month': {
                'start': (as_of.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d'),
                'end': (as_of.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
            }
        }
        
        metrics = {
            'as_of_date': as_of_date,
            'fb_leads_2_days': 0,
            'fb_leads_7_days': 0,
            'fb_leads_30_days': 0,
            'fb_cpl_7_days': 0,
            'fb_cpl_30_days': 0,
            'fb_mtd_spend': 0,
            'fb_last_month_spend': 0,
            'fb_spend_30_days': 0
        }
        
        has_data = False
        
        # Get data for each range
        data_2d = self.get_date_range_data(cgid, date_ranges['2_days']['start'], date_ranges['2_days']['end'])
        if data_2d:
            metrics['fb_leads_2_days'] = data_2d['leads']
            has_data = True
        
        data_7d = self.get_date_range_data(cgid, date_ranges['7_days']['start'], date_ranges['7_days']['end'])
        if data_7d:
            metrics['fb_leads_7_days'] = data_7d['leads']
            metrics['fb_cpl_7_days'] = data_7d['cpl']
            has_data = True
        
        data_30d = self.get_date_range_data(cgid, date_ranges['30_days']['start'], date_ranges['30_days']['end'])
        if data_30d:
            metrics['fb_leads_30_days'] = data_30d['leads']
            metrics['fb_cpl_30_days'] = data_30d['cpl']
            metrics['fb_spend_30_days'] = data_30d['spend']
            has_data = True
        
        data_mtd = self.get_date_range_data(cgid, date_ranges['mtd']['start'], date_ranges['mtd']['end'])
        if data_mtd:
            metrics['fb_mtd_spend'] = data_mtd['spend']
            has_data = True
        
        data_lm = self.get_date_range_data(cgid, date_ranges['last_month']['start'], date_ranges['last_month']['end'])
        if data_lm:
            metrics['fb_last_month_spend'] = data_lm['spend']
            has_data = True
        
        return metrics if has_data else None
    
    def update_monday_cell(self, item_id: str, column_id: str, value):
        """Update a single cell in Monday"""
        mutation = '''
        mutation ($board_id: ID!, $item_id: ID!, $column_id: String!, $value: JSON!) {
            change_column_value(
                board_id: $board_id,
                item_id: $item_id,
                column_id: $column_id,
                value: $value
            ) {
                id
            }
        }
        '''
        
        variables = {
            "board_id": BOARD_ID,
            "item_id": item_id,
            "column_id": column_id,
            "value": json.dumps(str(value))
        }
        
        try:
            response = requests.post(
                MONDAY_API_URL,
                json={"query": mutation, "variables": variables},
                headers=self.monday_headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return 'errors' not in data
            return False
                
        except Exception as e:
            return False
    
    def update_date_column(self, item_id: str, column_id: str, date_value: str):
        """Update a date column in Monday"""
        mutation = '''
        mutation ($board_id: ID!, $item_id: ID!, $column_id: String!, $value: JSON!) {
            change_column_value(
                board_id: $board_id,
                item_id: $item_id,
                column_id: $column_id,
                value: $value
            ) {
                id
            }
        }
        '''
        
        variables = {
            "board_id": BOARD_ID,
            "item_id": item_id,
            "column_id": column_id,
            "value": json.dumps({"date": date_value})
        }
        
        try:
            response = requests.post(
                MONDAY_API_URL,
                json={"query": mutation, "variables": variables},
                headers=self.monday_headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return 'errors' not in data
            return False
                
        except Exception as e:
            return False
    
    def sync_client_to_monday(self, item_id: str, metrics: dict):
        """Update all metrics for a client in Monday"""
        
        success_count = 0
        total_count = 0
        
        # Update each metric
        for metric_name, value in metrics.items():
            if metric_name == 'as_of_date':
                total_count += 1
                if self.update_date_column(item_id, FB_METRICS_COLUMNS['fb_as_of_date'], value):
                    success_count += 1
                continue
            
            if metric_name in FB_METRICS_COLUMNS:
                column_id = FB_METRICS_COLUMNS[metric_name]
                total_count += 1
                
                if self.update_monday_cell(item_id, column_id, value):
                    success_count += 1
                
                time.sleep(0.15)  # Rate limiting
        
        return success_count == total_count
    
    def process_item(self, item, index, as_of_date):
        """Process a single Monday item"""
        
        item_id = item['id']
        item_name = item['name']
        group = item.get('group', {})
        group_name = group.get('title', 'Unknown')
        
        cgid = self.extract_cgid(item)
        
        logging.info(f"\n[{index}] {item_name[:50]}... | Group: {group_name}")
        
        if not cgid:
            logging.info(f"  ⊘ No CGID, skipping")
            self.stats['no_cgid'] += 1
            return False
        
        logging.info(f"  CGID: {cgid}")
        
        # Fetch metrics from BigQuery
        metrics = self.calculate_metrics(cgid, as_of_date)
        
        if not metrics:
            logging.info(f"  ⊘ No data in BQ")
            self.stats['no_data'] += 1
            return False
        
        # Display key metrics
        logging.info(f"  📊 7d: {metrics['fb_leads_7_days']} leads (${metrics['fb_cpl_7_days']} CPL) | 30d: {metrics['fb_leads_30_days']} leads")
        
        # Update Monday board
        if self.sync_client_to_monday(item_id, metrics):
            logging.info(f"  ✅ Updated")
            self.stats['success'] += 1
            return True
        else:
            logging.info(f"  ❌ Update failed")
            self.stats['failed'] += 1
            return False
    
    def run(self):
        """Run the production pipeline"""
        
        start_time = datetime.now()
        
        logging.info("="*70)
        logging.info("🚀 PRODUCTION RUN - FB Metrics Sync")
        logging.info(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("="*70)
        
        # Initialize BigQuery
        if not self.init_bigquery():
            logging.error("Failed to initialize BigQuery")
            return False
        
        # Get all items from Monday
        items = self.get_all_items()
        if not items:
            logging.error("Failed to fetch items from Monday")
            return False
        
        # Filter items by group
        logging.info(f"\nFiltering items by groups...")
        logging.info(f"  Include groups: {INCLUDE_GROUPS}")
        logging.info(f"  Exclude groups: {EXCLUDE_GROUPS}")
        
        filtered_items = [item for item in items if self.should_process_item(item)]
        self.stats['filtered_out'] = len(items) - len(filtered_items)
        self.stats['total'] = len(filtered_items)
        
        logging.info(f"  Total items: {len(items)}")
        logging.info(f"  Filtered out: {self.stats['filtered_out']}")
        logging.info(f"  To process: {len(filtered_items)}")
        
        if not filtered_items:
            logging.warning("No items to process after filtering")
            return False
        
        # Calculate as of date (yesterday)
        as_of_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        logging.info(f"\n📅 Using data as of: {as_of_date}")
        
        # Process each filtered item
        logging.info("\n" + "="*70)
        logging.info("Processing items...")
        logging.info("="*70)
        
        for index, item in enumerate(filtered_items, start=1):
            try:
                self.process_item(item, index, as_of_date)
            except Exception as e:
                logging.error(f"Unexpected error: {str(e)}")
                self.stats['failed'] += 1
            
            # Progress indicator every 10 items
            if index % 10 == 0:
                logging.info(f"\n--- Progress: {index}/{len(filtered_items)} items processed ---")
        
        # Print summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        self.print_summary(duration)
        
        return self.stats['failed'] == 0
    
    def print_summary(self, duration):
        """Print execution summary"""
        logging.info("\n" + "="*70)
        logging.info("📊 EXECUTION SUMMARY")
        logging.info("="*70)
        logging.info(f"Duration: {duration:.1f} seconds")
        logging.info(f"\nTotal Items on Board: {self.stats['total'] + self.stats['filtered_out']}")
        logging.info(f"Filtered Out (wrong groups): {self.stats['filtered_out']}")
        logging.info(f"Items Processed: {self.stats['total']}")
        logging.info(f"\n✅ Successfully Updated: {self.stats['success']}")
        logging.info(f"⊘ No CGID: {self.stats['no_cgid']}")
        logging.info(f"⊘ No Data in BQ: {self.stats['no_data']}")
        logging.info(f"❌ Failed: {self.stats['failed']}")
        
        if self.stats['total'] > 0:
            success_rate = (self.stats['success'] / self.stats['total'] * 100)
            logging.info(f"\n✨ Success Rate: {success_rate:.1f}%")
        
        logging.info("="*70)

def main(request=None):
    """
    Main function - compatible with Cloud Functions
    Args:
        request: Flask request object (for Cloud Functions)
    """
    try:
        pipeline = ProductionPipeline()
        success = pipeline.run()
        
        # Return response for Cloud Functions
        if request:
            return {
                'status': 'success' if success else 'partial_success',
                'stats': pipeline.stats
            }, 200 if success else 206
        
        return success
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        if request:
            return {'status': 'error', 'message': str(e)}, 500
        return False

if __name__ == "__main__":
    # For local testing
    main()