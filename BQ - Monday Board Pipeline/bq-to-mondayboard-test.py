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

# Test Clients Configuration
TEST_CLIENTS = {
    'CG374': {
        'monday_item_id': '9079236558',
        'bq_cgid': 'CG374',
        'name': 'Breeze Med Spa'
    },
    'CG376': {
        'monday_item_id': '9312483762',
        'bq_cgid': 'CG376',
        'name': 'Vanish Advanced Vein Treatment'
    }
}

# Column Mapping (from Phase 1)
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

class IntegrationPipeline:
    def __init__(self):
        self.monday_headers = {"Authorization": MONDAY_API_TOKEN}
        self.bq_client = None
        
    def get_fb_as_of_date_column_id(self):
        """Find the column ID for 'FB As of Date' field"""
        logging.info("Looking for 'FB As of Date' column...")
        
        query = '''
        query ($board_id: ID!) {
            boards(ids: [$board_id]) {
                columns {
                    id
                    title
                    type
                }
            }
        }
        '''
        
        try:
            response = requests.post(
                MONDAY_API_URL,
                json={"query": query, "variables": {"board_id": BOARD_ID}},
                headers=self.monday_headers
            )
            
            if response.status_code == 200:
                data = response.json()
                columns = data['data']['boards'][0]['columns']
                
                # Look for the date column
                for col in columns:
                    if 'FB As of Date' in col['title'] or 'fb_as_of_date' in col['id'].lower():
                        logging.info(f"Found 'FB As of Date' column: {col['title']} (ID: {col['id']})")
                        return col['id']
                
                logging.warning("Could not find 'FB As of Date' column. Will skip updating it.")
                return None
            else:
                logging.error(f"Error fetching columns: {response.status_code}")
                return None
                
        except Exception as e:
            logging.error(f"Error finding column: {str(e)}")
            return None
    
    def init_bigquery(self):
        """Initialize BigQuery client"""
        logging.info("Initializing BigQuery connection...")
        try:
            self.bq_client = bigquery.Client(project=PROJECT_ID)
            logging.info("BigQuery client initialized")
            return True
        except Exception as e:
            logging.error(f"Failed to initialize BigQuery: {str(e)}")
            return False
    
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
        
        # Get data for each range
        data_2d = self.get_date_range_data(cgid, date_ranges['2_days']['start'], date_ranges['2_days']['end'])
        if data_2d:
            metrics['fb_leads_2_days'] = data_2d['leads']
        
        data_7d = self.get_date_range_data(cgid, date_ranges['7_days']['start'], date_ranges['7_days']['end'])
        if data_7d:
            metrics['fb_leads_7_days'] = data_7d['leads']
            metrics['fb_cpl_7_days'] = data_7d['cpl']
        
        data_30d = self.get_date_range_data(cgid, date_ranges['30_days']['start'], date_ranges['30_days']['end'])
        if data_30d:
            metrics['fb_leads_30_days'] = data_30d['leads']
            metrics['fb_cpl_30_days'] = data_30d['cpl']
            metrics['fb_spend_30_days'] = data_30d['spend']
        
        data_mtd = self.get_date_range_data(cgid, date_ranges['mtd']['start'], date_ranges['mtd']['end'])
        if data_mtd:
            metrics['fb_mtd_spend'] = data_mtd['spend']
        
        data_lm = self.get_date_range_data(cgid, date_ranges['last_month']['start'], date_ranges['last_month']['end'])
        if data_lm:
            metrics['fb_last_month_spend'] = data_lm['spend']
        
        return metrics
    
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
                    logging.error(f"Monday API error: {data['errors']}")
                    return False
            else:
                logging.error(f"HTTP error: {response.status_code}")
                return False
                
        except Exception as e:
            logging.error(f"Error updating Monday: {str(e)}")
            return False
    
    def update_date_column(self, item_id: str, column_id: str, date_value: str):
        """Update a date column in Monday (requires different format)"""
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
        
        # Date columns expect format: {"date": "YYYY-MM-DD"}
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
                else:
                    logging.error(f"Monday API error: {data['errors']}")
                    return False
            else:
                logging.error(f"HTTP error: {response.status_code}")
                return False
                
        except Exception as e:
            logging.error(f"Error updating date: {str(e)}")
            return False
    
    def sync_client_to_monday(self, monday_cgid: str, client_info: dict, metrics: dict):
        """Update all metrics for a client in Monday"""
        
        item_id = client_info['monday_item_id']
        success_count = 0
        total_count = 0
        
        logging.info(f"\nUpdating Monday board for {monday_cgid} - {client_info['name']}")
        
        # Update each metric
        for metric_name, value in metrics.items():
            if metric_name == 'as_of_date':
                # Update the date column
                if FB_METRICS_COLUMNS['fb_as_of_date']:
                    total_count += 1
                    if self.update_date_column(item_id, FB_METRICS_COLUMNS['fb_as_of_date'], value):
                        logging.info(f"  ✓ Updated {metric_name}: {value}")
                        success_count += 1
                    else:
                        logging.error(f"  ✗ Failed to update {metric_name}")
                continue
            
            if metric_name in FB_METRICS_COLUMNS:
                column_id = FB_METRICS_COLUMNS[metric_name]
                total_count += 1
                
                if self.update_monday_cell(item_id, column_id, value):
                    logging.info(f"  ✓ Updated {metric_name}: {value}")
                    success_count += 1
                else:
                    logging.error(f"  ✗ Failed to update {metric_name}")
                
                time.sleep(0.2)  # Rate limiting
        
        logging.info(f"Update complete: {success_count}/{total_count} successful")
        return success_count == total_count
    
    def run_integration_test(self):
        """Run the complete integration pipeline"""
        logging.info("=" * 70)
        logging.info("🚀 Starting Integration Test - Phase 3")
        logging.info("=" * 70)
        
        # Step 1: Initialize BigQuery
        if not self.init_bigquery():
            logging.error("Failed to initialize BigQuery. Stopping.")
            return False
        
        # Step 2: Process each test client
        all_success = True
        
        for monday_cgid, client_info in TEST_CLIENTS.items():
            logging.info("\n" + "=" * 70)
            logging.info(f"Processing: {client_info['name']}")
            logging.info("=" * 70)
            
            bq_cgid = client_info['bq_cgid']
            
            # Fetch metrics from BigQuery
            logging.info(f"\n📊 Fetching metrics from BigQuery for {bq_cgid}...")
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            metrics = self.calculate_metrics(bq_cgid, yesterday)
            
            if not metrics:
                logging.error(f"Failed to calculate metrics for {bq_cgid}")
                all_success = False
                continue
            
            # Display metrics
            logging.info("\n📈 Calculated Metrics:")
            logging.info(f"  As of Date: {metrics['as_of_date']}")
            logging.info(f"  Leads (2d): {metrics['fb_leads_2_days']}")
            logging.info(f"  Leads (7d): {metrics['fb_leads_7_days']}")
            logging.info(f"  Leads (30d): {metrics['fb_leads_30_days']}")
            logging.info(f"  CPL (7d): ${metrics['fb_cpl_7_days']}")
            logging.info(f"  CPL (30d): ${metrics['fb_cpl_30_days']}")
            logging.info(f"  Spend (MTD): ${metrics['fb_mtd_spend']}")
            logging.info(f"  Spend (Last Month): ${metrics['fb_last_month_spend']}")
            logging.info(f"  Spend (30d): ${metrics['fb_spend_30_days']}")
            
            # Update Monday board
            logging.info(f"\n📤 Updating Monday board...")
            if not self.sync_client_to_monday(monday_cgid, client_info, metrics):
                all_success = False
        
        # Final summary
        logging.info("\n" + "=" * 70)
        if all_success:
            logging.info("✅ Integration test completed successfully!")
            logging.info("=" * 70)
            logging.info("\n🎉 Phase 3 Complete!")
            logging.info("\nNext steps:")
            logging.info("1. Check your Monday board to verify all values updated correctly")
            logging.info("2. Compare the values with what you see in BigQuery")
            logging.info("3. If everything looks good, we can proceed to production rollout")
            return True
        else:
            logging.warning("⚠️ Integration test completed with some failures")
            logging.info("=" * 70)
            return False

def main():
    """Main function"""
    print("\n" + "=" * 70)
    print("BigQuery → Monday.com Integration Pipeline")
    print("Phase 3: Integration Testing")
    print("=" * 70 + "\n")
    
    # Check if token is set
    if MONDAY_API_TOKEN == "your_monday_api_token_here":
        print("❌ Please set your Monday API token in the script")
        return
    
    pipeline = IntegrationPipeline()
    success = pipeline.run_integration_test()
    
    if success:
        print("\n✅ All systems working! Ready for production rollout.")
    else:
        print("\n❌ Some issues detected. Review logs and fix before production.")

if __name__ == "__main__":
    main()