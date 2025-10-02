import requests
import json
import time
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Monday.com Configuration
MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_API_TOKEN = "monday_board_token_here"  # Replace with your actual token
BOARD_ID = "1981285971"

# BigQuery Configuration
PROJECT_ID = "clinicgrower-reporting"
DATASET_ID = "dashboard_views"
VIEW_NAME = "metaads_ga4_spend_leads_pivot"
FULL_VIEW_PATH = f"{PROJECT_ID}.{DATASET_ID}.{VIEW_NAME}"

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
        self.monday_headers = {"Authorization": MONDAY_API_TOKEN}
        self.bq_client = None
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'no_data': 0,
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
    
    def get_first_n_items(self, n=10):
        """Get the first N items from Monday board"""
        logging.info(f"Fetching first {n} items from Monday board...")
        
        query = '''
        query ($board_id: ID!, $limit: Int!) {
            boards(ids: [$board_id]) {
                items_page(limit: $limit) {
                    items {
                        id
                        name
                        column_values {
                            id
                            value
                        }
                    }
                }
            }
        }
        '''
        
        try:
            response = requests.post(
                MONDAY_API_URL,
                json={
                    "query": query, 
                    "variables": {"board_id": BOARD_ID, "limit": n}
                },
                headers=self.monday_headers
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'errors' not in data:
                    items = data['data']['boards'][0]['items_page']['items']
                    logging.info(f"✅ Found {len(items)} items")
                    return items
                else:
                    logging.error(f"API error: {data['errors']}")
                    return None
            else:
                logging.error(f"HTTP error: {response.status_code}")
                return None
                
        except Exception as e:
            logging.error(f"Error fetching items: {str(e)}")
            return None
    
    def extract_cgid(self, item):
        """Extract CGID from Monday item (from text25 column)"""
        for col in item['column_values']:
            if col['id'] == 'text25':  # CGID column
                if col['value']:
                    # Value is stored as JSON string like "\"CG374\""
                    cgid = json.loads(col['value'])
                    return cgid
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
                if 'errors' not in data:
                    return True
                else:
                    return False
            else:
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
                if 'errors' not in data:
                    return True
            return False
                
        except Exception as e:
            return False
    
    def sync_client_to_monday(self, item_id: str, cgid: str, client_name: str, metrics: dict):
        """Update all metrics for a client in Monday"""
        
        success_count = 0
        total_count = 0
        
        # Update each metric
        for metric_name, value in metrics.items():
            if metric_name == 'as_of_date':
                # Update the date column
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
        cgid = self.extract_cgid(item)
        
        logging.info(f"\n{'='*70}")
        logging.info(f"[{index}] Processing: {item_name}")
        logging.info(f"{'='*70}")
        
        if not cgid:
            logging.warning(f"⚠️  No CGID found for item {item_id}, skipping")
            self.stats['no_data'] += 1
            return False
        
        logging.info(f"CGID: {cgid} | Item ID: {item_id}")
        
        # Fetch metrics from BigQuery
        logging.info(f"📊 Fetching metrics from BigQuery...")
        metrics = self.calculate_metrics(cgid, as_of_date)
        
        if not metrics:
            logging.warning(f"⚠️  No data found in BigQuery for {cgid}")
            self.stats['no_data'] += 1
            return False
        
        # Display metrics
        logging.info(f"📈 Metrics Summary:")
        logging.info(f"  • Leads (7d): {metrics['fb_leads_7_days']} | CPL: ${metrics['fb_cpl_7_days']}")
        logging.info(f"  • Leads (30d): {metrics['fb_leads_30_days']} | CPL: ${metrics['fb_cpl_30_days']}")
        logging.info(f"  • Spend (30d): ${metrics['fb_spend_30_days']}")
        
        # Update Monday board
        logging.info(f"📤 Updating Monday board...")
        if self.sync_client_to_monday(item_id, cgid, item_name, metrics):
            logging.info(f"✅ Successfully updated {cgid}")
            self.stats['success'] += 1
            return True
        else:
            logging.error(f"❌ Failed to update {cgid}")
            self.stats['failed'] += 1
            self.stats['errors'].append({'cgid': cgid, 'name': item_name, 'reason': 'Monday update failed'})
            return False
    
    def run_production_rollout(self, num_clients=10):
        """Run the production rollout"""
        
        logging.info("="*70)
        logging.info("🚀 PRODUCTION ROLLOUT - First 10 Clients")
        logging.info("="*70)
        
        # Initialize BigQuery
        if not self.init_bigquery():
            logging.error("❌ Failed to initialize BigQuery. Stopping.")
            return False
        
        # Get items from Monday
        items = self.get_first_n_items(num_clients)
        if not items:
            logging.error("❌ Failed to fetch items from Monday. Stopping.")
            return False
        
        self.stats['total'] = len(items)
        
        # Calculate as of date (yesterday)
        as_of_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        logging.info(f"\n📅 Using data as of: {as_of_date}")
        
        # Process each item
        for index, item in enumerate(items, start=1):
            try:
                self.process_item(item, index, as_of_date)
            except Exception as e:
                logging.error(f"❌ Unexpected error processing item: {str(e)}")
                self.stats['failed'] += 1
        
        # Print summary
        self.print_summary()
        
        return self.stats['failed'] == 0
    
    def print_summary(self):
        """Print execution summary"""
        logging.info("\n" + "="*70)
        logging.info("📊 EXECUTION SUMMARY")
        logging.info("="*70)
        logging.info(f"Total Clients Processed: {self.stats['total']}")
        logging.info(f"✅ Successfully Updated: {self.stats['success']}")
        logging.info(f"⚠️  No Data in BQ: {self.stats['no_data']}")
        logging.info(f"❌ Failed: {self.stats['failed']}")
        
        if self.stats['errors']:
            logging.info(f"\n❌ Failed Items:")
            for error in self.stats['errors']:
                logging.info(f"  - {error['cgid']}: {error['name']} ({error['reason']})")
        
        success_rate = (self.stats['success'] / self.stats['total'] * 100) if self.stats['total'] > 0 else 0
        logging.info(f"\n✨ Success Rate: {success_rate:.1f}%")
        logging.info("="*70)

def main():
    """Main function"""
    print("\n" + "="*70)
    print("BigQuery → Monday.com Production Rollout")
    print("First 10 Clients")
    print("="*70 + "\n")
    
    # Check if token is set
    if MONDAY_API_TOKEN == "your_monday_api_token_here":
        print("❌ Please set your Monday API token in the script")
        return
    
    # Confirm rollout
    print("⚠️  This will update the first 10 items on your Monday board.")
    print("Make sure you've:")
    print("  1. Set the GOOGLE_APPLICATION_CREDENTIALS environment variable")
    print("  2. Updated the MONDAY_API_TOKEN in the script")
    print("  3. Tested with the Phase 3 integration test")
    
    confirm = input("\n🚀 Proceed with rollout? (yes/no): ").strip().lower()
    
    if confirm != 'yes':
        print("❌ Rollout cancelled.")
        return
    
    # Run rollout
    pipeline = ProductionPipeline()
    success = pipeline.run_production_rollout(num_clients=10)
    
    if success:
        print("\n✅ Production rollout completed successfully!")
        print("Check your Monday board to verify the updates.")
    else:
        print("\n⚠️  Production rollout completed with some issues.")
        print("Review the logs above for details.")

if __name__ == "__main__":
    main()