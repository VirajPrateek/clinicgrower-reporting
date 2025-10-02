import requests
import json
import time
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Monday.com Configuration
MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_API_TOKEN = "monday_board_token_here"  # Replace with your actual token
BOARD_ID = 1981285971  # Your board ID from the JSON

# Test Clients Mapping (from your JSON data)
TEST_CLIENTS = {
    'CG374': {
        'item_id': '9079236558',
        'name': 'Breeze Med Spa | Natalie Romney Call, RN | Chandler, AZ'
    },
    'CG376': {
        'item_id': '9312483762', 
        'name': 'Vanish Advanced Vein Treatment | Dr. Denise Abernethy | Mequon, WI'
    }
}

# Column Mapping - Actual column IDs from the board
FB_METRICS_COLUMNS = {
    'fb_leads_2_days': 'numbers4',       # FB Leads Past 2 Days
    'fb_leads_7_days': 'numeric9',       # FB Leads Past 7 Days
    'fb_leads_30_days': 'numeric0',      # FB Leads Past 30 Days
    'fb_cpl_7_days': 'numbers01',        # FB 7 day cpl
    'fb_cpl_30_days': 'numbers49',       # FB 30 day cpl
    'fb_mtd_spend': 'numbers27',         # FB MTD Spend
    'fb_last_month_spend': 'numbers04',  # FB Last Month Spend
    'fb_spend_30_days': 'numbers3'       # FB Spend Last 30 Days
}

# Dummy Test Data
DUMMY_DATA = {
    'CG374': {
        'fb_leads_2_days': 88,
        'fb_leads_7_days': 238,
        'fb_leads_30_days': 889,
        'fb_cpl_7_days': 125.50,
        'fb_cpl_30_days': 132.75,
        'fb_mtd_spend': 2850.00,
        'fb_last_month_spend': 3200.00,
        'fb_spend_30_days': 11800.00
    },
    'CG376': {
        'fb_leads_2_days': 58,
        'fb_leads_7_days': 818,
        'fb_leads_30_days': 67,
        'fb_cpl_7_days': 95.25,
        'fb_cpl_30_days': 98.50,
        'fb_mtd_spend': 1750.00,
        'fb_last_month_spend': 2100.00,
        'fb_spend_30_days': 8500.00
    }
}

class MondayAPITester:
    def __init__(self):
        self.headers = {"Authorization": MONDAY_API_TOKEN}
        self.test_results = []

    def test_connection(self):
        """Test basic connection to Monday API"""
        logging.info("Testing Monday API connection...")
        
        query = '''
        query {
            me {
                id
                name
            }
        }
        '''
        
        try:
            response = requests.post(
                MONDAY_API_URL,
                json={"query": query},
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'errors' in data:
                    logging.error(f"API Error: {data['errors']}")
                    return False
                else:
                    user = data['data']['me']
                    logging.info(f"✅ Connection successful! User: {user['name']} (ID: {user['id']})")
                    return True
            else:
                logging.error(f"HTTP Error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logging.error(f"Connection failed: {str(e)}")
            return False

    def get_board_columns(self):
        """Get all column information for the board"""
        logging.info("Fetching board column information...")
        
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
                json={"query": query, "variables": {"board_id": str(BOARD_ID)}},
                headers=self.headers
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'errors' not in data:
                    columns = data['data']['boards'][0]['columns']
                    logging.info("📋 Board Columns:")
                    for col in columns:
                        if col['type'] in ['numeric', 'numbers']:
                            logging.info(f"  - {col['title']} (ID: {col['id']}, Type: {col['type']})")
                    return columns
                else:
                    logging.error(f"API Error: {data['errors']}")
                    return None
            else:
                logging.error(f"HTTP Error: {response.status_code}")
                return None
                
        except Exception as e:
            logging.error(f"Error fetching columns: {str(e)}")
            return None

    def verify_test_items(self):
        """Verify that our test client items exist on the board"""
        logging.info("Verifying test client items...")
        
        query = '''
        query ($board_id: ID!) {
            boards(ids: [$board_id]) {
                items_page {
                    items {
                        id
                        name
                    }
                }
            }
        }
        '''
        
        try:
            response = requests.post(
                MONDAY_API_URL,
                json={"query": query, "variables": {"board_id": str(BOARD_ID)}},
                headers=self.headers
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'errors' not in data:
                    items = data['data']['boards'][0]['items_page']['items']
                    found_items = {}
                    
                    for item in items:
                        for client_code, client_info in TEST_CLIENTS.items():
                            if item['id'] == client_info['item_id']:
                                found_items[client_code] = item
                                logging.info(f"✅ Found {client_code}: {item['name']}")
                    
                    missing = set(TEST_CLIENTS.keys()) - set(found_items.keys())
                    if missing:
                        logging.warning(f"❌ Missing items: {missing}")
                        return False
                    
                    return True
                else:
                    logging.error(f"API Error: {data['errors']}")
                    return False
            else:
                logging.error(f"HTTP Error: {response.status_code}")
                return False
                
        except Exception as e:
            logging.error(f"Error verifying items: {str(e)}")
            return False

    def update_single_cell(self, item_id, column_id, value):
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
        
        # For numbers columns, Monday expects just the number as a string
        # The value needs to be JSON-serializable
        variables = {
            "board_id": str(BOARD_ID),
            "item_id": str(item_id),
            "column_id": column_id,
            "value": json.dumps(str(value))  # Monday expects the number as a JSON-encoded string
        }
        
        try:
            response = requests.post(
                MONDAY_API_URL,
                json={"query": mutation, "variables": variables},
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'errors' not in data:
                    return {'success': True, 'data': data}
                else:
                    return {'success': False, 'error': data['errors']}
            else:
                return {'success': False, 'error': f"HTTP {response.status_code}"}
                
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def test_single_update(self, client_code, metric_name, test_value):
        """Test updating a single metric for a single client"""
        logging.info(f"Testing update: {client_code} - {metric_name} = {test_value}")
        
        if client_code not in TEST_CLIENTS:
            logging.error(f"Client {client_code} not in test clients")
            return False
        
        if metric_name not in FB_METRICS_COLUMNS:
            logging.error(f"Metric {metric_name} not in column mapping")
            return False
        
        item_id = TEST_CLIENTS[client_code]['item_id']
        column_id = FB_METRICS_COLUMNS[metric_name]
        
        result = self.update_single_cell(item_id, column_id, test_value)
        
        if result['success']:
            logging.info(f"✅ Successfully updated {client_code} - {metric_name}")
            self.test_results.append({
                'client': client_code,
                'metric': metric_name,
                'value': test_value,
                'success': True
            })
            return True
        else:
            logging.error(f"❌ Failed to update {client_code} - {metric_name}: {result['error']}")
            self.test_results.append({
                'client': client_code,
                'metric': metric_name,
                'value': test_value,
                'success': False,
                'error': result['error']
            })
            return False

    def test_bulk_update_single_client(self, client_code):
        """Test updating all metrics for a single client"""
        logging.info(f"Testing bulk update for {client_code}")
        
        if client_code not in DUMMY_DATA:
            logging.error(f"No dummy data for {client_code}")
            return False
        
        client_data = DUMMY_DATA[client_code]
        success_count = 0
        
        for metric_name, value in client_data.items():
            if self.test_single_update(client_code, metric_name, value):
                success_count += 1
            time.sleep(0.2)  # Rate limiting
        
        logging.info(f"Bulk update for {client_code}: {success_count}/{len(client_data)} successful")
        return success_count == len(client_data)

    def run_comprehensive_test(self):
        """Run all tests in sequence"""
        logging.info("🚀 Starting Monday API Comprehensive Test")
        
        # Test 1: Connection
        if not self.test_connection():
            logging.error("❌ Connection test failed. Stopping tests.")
            return False
        
        # Test 2: Get columns (for reference)
        self.get_board_columns()
        
        # Test 3: Verify test items exist
        if not self.verify_test_items():
            logging.error("❌ Test items verification failed. Stopping tests.")
            return False
        
        # Test 4: Single cell update test
        logging.info("\n📝 Testing single cell updates...")
        self.test_single_update('CG374', 'fb_leads_7_days', 99)
        
        # Test 5: Bulk updates for both clients
        logging.info("\n📊 Testing bulk updates...")
        cg374_success = self.test_bulk_update_single_client('CG374')
        cg376_success = self.test_bulk_update_single_client('CG376')
        
        # Summary
        logging.info("\n📋 TEST SUMMARY:")
        logging.info(f"Total tests run: {len(self.test_results)}")
        successful_tests = sum(1 for test in self.test_results if test['success'])
        logging.info(f"Successful: {successful_tests}")
        logging.info(f"Failed: {len(self.test_results) - successful_tests}")
        
        if successful_tests == len(self.test_results):
            logging.info("🎉 All tests passed!")
            return True
        else:
            logging.warning("⚠️ Some tests failed. Check logs above.")
            return False

def main():
    """Main function to run the tests"""
    print("Monday.com API Tester - Phase 1")
    print("================================")
    
    # Check if token is set
    if MONDAY_API_TOKEN == "your_monday_api_token_here":
        print("❌ Please set your Monday API token in the script")
        return
    
    tester = MondayAPITester()
    success = tester.run_comprehensive_test()
    
    if success:
        print("\n✅ Phase 1 testing completed successfully!")
        print("Next steps:")
        print("1. Verify the updates are visible in your Monday board")
        print("2. Proceed to Phase 2 (BigQuery testing)")
    else:
        print("\n❌ Phase 1 testing had failures. Review logs and fix issues.")

if __name__ == "__main__":
    main()