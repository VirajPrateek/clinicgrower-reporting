import logging
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# BigQuery Configuration
PROJECT_ID = "clinicgrower-reporting"
DATASET_ID = "dashboard_views"
VIEW_NAME = "metaads_ga4_spend_leads_pivot"
FULL_VIEW_PATH = f"{PROJECT_ID}.{DATASET_ID}.{VIEW_NAME}"

# Test Clients - Map Monday CGID to BQ CGID (confirmed they match)
TEST_CLIENTS_MAPPING = {
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

# Lead source columns to sum
LEAD_COLUMNS = [
    'subscribe_survey_meta_ghl',
    'subscribe_form_meta_ghl',
    'subscribe_chat_fbfunnel_ghl'
]

class BigQueryTester:
    def __init__(self):
        self.client = None
        
    def test_connection(self):
        """Test BigQuery connection"""
        logging.info("Testing BigQuery connection...")
        
        try:
            self.client = bigquery.Client(project=PROJECT_ID)
            
            # Simple query to test connection
            query = "SELECT 1 as test"
            result = self.client.query(query).result()
            
            logging.info(f"✅ BigQuery connection successful! Project: {self.client.project}")
            return True
            
        except Exception as e:
            logging.error(f"❌ BigQuery connection failed: {str(e)}")
            logging.error("Make sure you have:")
            logging.error("1. Set GOOGLE_APPLICATION_CREDENTIALS environment variable")
            logging.error("2. Or run 'gcloud auth application-default login'")
            return False
    
    def test_view_access(self):
        """Test access to the specific view"""
        logging.info(f"Testing access to view: {FULL_VIEW_PATH}")
        
        try:
            query = f"""
            SELECT COUNT(*) as row_count
            FROM `{FULL_VIEW_PATH}`
            LIMIT 1
            """
            
            result = self.client.query(query).result()
            row_count = list(result)[0].row_count
            
            logging.info(f"✅ View access successful! View has data.")
            return True
            
        except Exception as e:
            logging.error(f"❌ View access failed: {str(e)}")
            return False
    
    def explore_data_for_test_clients(self):
        """Check what CGIDs are available in the data"""
        logging.info("Exploring available CGIDs in BigQuery data...")
        
        try:
            query = f"""
            SELECT DISTINCT cgid, client_name
            FROM `{FULL_VIEW_PATH}`
            WHERE cgid IN ('CG374', 'CG376', 'CG026')
            ORDER BY cgid
            """
            
            df = self.client.query(query).to_dataframe()
            
            if len(df) > 0:
                logging.info(f"✅ Found {len(df)} matching CGIDs:")
                for _, row in df.iterrows():
                    logging.info(f"  - {row['cgid']}: {row['client_name']}")
                return df
            else:
                logging.warning("⚠️ No matching CGIDs found in BQ data")
                logging.info("Let's check what CGIDs exist in the data...")
                
                # Get sample of CGIDs
                query = f"""
                SELECT DISTINCT cgid, client_name
                FROM `{FULL_VIEW_PATH}`
                LIMIT 10
                """
                df = self.client.query(query).to_dataframe()
                logging.info("Sample CGIDs in the view:")
                for _, row in df.iterrows():
                    logging.info(f"  - {row['cgid']}: {row['client_name']}")
                
                return None
                
        except Exception as e:
            logging.error(f"❌ Error exploring data: {str(e)}")
            return None
    
    def get_date_range_data(self, cgid: str, start_date: str, end_date: str) -> Optional[Dict]:
        """Get aggregated data for a specific CGID and date range"""
        
        try:
            query = f"""
            SELECT 
                cgid,
                client_name,
                SUM(subscribe_survey_meta_ghl) as survey_leads,
                SUM(subscribe_form_meta_ghl) as form_leads,
                SUM(subscribe_chat_fbfunnel_ghl) as chat_leads,
                SUM(total_spend) as total_spend,
                MIN(report_date) as start_date,
                MAX(report_date) as end_date
            FROM `{FULL_VIEW_PATH}`
            WHERE cgid = '{cgid}'
              AND report_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY cgid, client_name
            """
            
            df = self.client.query(query).to_dataframe()
            
            if len(df) == 0:
                logging.warning(f"No data found for {cgid} between {start_date} and {end_date}")
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
            
            result = {
                'cgid': row['cgid'],
                'client_name': row['client_name'],
                'leads': int(total_leads),
                'spend': float(row['total_spend']),
                'cpl': cpl,
                'date_range': f"{row['start_date']} to {row['end_date']}"
            }
            
            return result
            
        except Exception as e:
            logging.error(f"Error getting data for {cgid}: {str(e)}")
            return None
    
    def calculate_all_metrics(self, cgid: str, as_of_date: Optional[str] = None) -> Dict:
        """Calculate all required metrics for a specific CGID"""
        
        if as_of_date is None:
            as_of_date = datetime.now().strftime('%Y-%m-%d')
        
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
            'cgid': cgid,
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
        logging.info(f"\nCalculating metrics for {cgid} as of {as_of_date}...")
        
        # 2 days
        data_2d = self.get_date_range_data(cgid, date_ranges['2_days']['start'], date_ranges['2_days']['end'])
        if data_2d:
            metrics['fb_leads_2_days'] = data_2d['leads']
            logging.info(f"  2 days: {data_2d['leads']} leads, ${data_2d['spend']:.2f} spend")
        
        # 7 days
        data_7d = self.get_date_range_data(cgid, date_ranges['7_days']['start'], date_ranges['7_days']['end'])
        if data_7d:
            metrics['fb_leads_7_days'] = data_7d['leads']
            metrics['fb_cpl_7_days'] = data_7d['cpl']
            logging.info(f"  7 days: {data_7d['leads']} leads, ${data_7d['spend']:.2f} spend, ${data_7d['cpl']:.2f} CPL")
        
        # 30 days
        data_30d = self.get_date_range_data(cgid, date_ranges['30_days']['start'], date_ranges['30_days']['end'])
        if data_30d:
            metrics['fb_leads_30_days'] = data_30d['leads']
            metrics['fb_cpl_30_days'] = data_30d['cpl']
            metrics['fb_spend_30_days'] = data_30d['spend']
            logging.info(f"  30 days: {data_30d['leads']} leads, ${data_30d['spend']:.2f} spend, ${data_30d['cpl']:.2f} CPL")
        
        # MTD
        data_mtd = self.get_date_range_data(cgid, date_ranges['mtd']['start'], date_ranges['mtd']['end'])
        if data_mtd:
            metrics['fb_mtd_spend'] = data_mtd['spend']
            logging.info(f"  MTD: ${data_mtd['spend']:.2f} spend")
        
        # Last month
        data_lm = self.get_date_range_data(cgid, date_ranges['last_month']['start'], date_ranges['last_month']['end'])
        if data_lm:
            metrics['fb_last_month_spend'] = data_lm['spend']
            logging.info(f"  Last month: ${data_lm['spend']:.2f} spend")
        
        return metrics
    
    def test_metrics_calculation(self):
        """Test calculating metrics for test clients"""
        logging.info("\n📊 Testing metrics calculation for test clients...")
        
        results = {}
        
        for monday_cgid, client_info in TEST_CLIENTS_MAPPING.items():
            bq_cgid = client_info['bq_cgid']
            logging.info(f"\n{'='*60}")
            logging.info(f"Client: {client_info['name']} (Monday: {monday_cgid}, BQ: {bq_cgid})")
            logging.info(f"{'='*60}")
            
            # Calculate metrics using yesterday as the "as of" date
            # (since today's data might not be complete yet)
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            metrics = self.calculate_all_metrics(bq_cgid, yesterday)
            
            if metrics:
                results[monday_cgid] = metrics
                
                # Display summary
                logging.info(f"\n📋 Summary for {monday_cgid}:")
                logging.info(f"  Leads (2d): {metrics['fb_leads_2_days']}")
                logging.info(f"  Leads (7d): {metrics['fb_leads_7_days']}")
                logging.info(f"  Leads (30d): {metrics['fb_leads_30_days']}")
                logging.info(f"  CPL (7d): ${metrics['fb_cpl_7_days']}")
                logging.info(f"  CPL (30d): ${metrics['fb_cpl_30_days']}")
                logging.info(f"  Spend (MTD): ${metrics['fb_mtd_spend']}")
                logging.info(f"  Spend (Last Month): ${metrics['fb_last_month_spend']}")
                logging.info(f"  Spend (30d): ${metrics['fb_spend_30_days']}")
        
        return results
    
    def run_comprehensive_test(self):
        """Run all BigQuery tests"""
        logging.info("🚀 Starting BigQuery Comprehensive Test")
        
        # Test 1: Connection
        if not self.test_connection():
            logging.error("❌ Connection test failed. Stopping tests.")
            return False
        
        # Test 2: View access
        if not self.test_view_access():
            logging.error("❌ View access test failed. Stopping tests.")
            return False
        
        # Test 3: Explore data
        self.explore_data_for_test_clients()
        
        # Test 4: Calculate metrics
        results = self.test_metrics_calculation()
        
        if results:
            logging.info("\n" + "="*60)
            logging.info("✅ All BigQuery tests completed successfully!")
            logging.info("="*60)
            logging.info(f"\nMetrics calculated for {len(results)} clients")
            return results
        else:
            logging.warning("⚠️ Tests completed but no metrics were calculated")
            return None

def main():
    """Main function to run BigQuery tests"""
    print("BigQuery Data Fetcher - Phase 2")
    print("================================\n")
    
    tester = BigQueryTester()
    results = tester.run_comprehensive_test()
    
    if results:
        print("\n✅ Phase 2 testing completed successfully!")
        print("\nNext steps:")
        print("1. Verify the calculated metrics match your expectations")
        print("2. Update TEST_CLIENTS_MAPPING if CGID mappings are incorrect")
        print("3. Proceed to Phase 3 (Integration testing)")
    else:
        print("\n❌ Phase 2 testing had issues. Review logs and fix.")

if __name__ == "__main__":
    main()