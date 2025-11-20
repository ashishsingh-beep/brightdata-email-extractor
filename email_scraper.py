"""
Email Scraper Script - Brightdata API Integration with Supabase Storage
This script processes search queries, sends them to Brightdata API in batches,
and stores the snapshot IDs in Supabase.
"""

import requests
import json
import time
import os
import csv
from typing import List, Dict, Optional
from supabase import create_client, Client
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables or defaults
BRIGHTDATA_URL = os.getenv(
    'BRIGHTDATA_URL',
    "https://api.brightdata.com/datasets/v3/trigger?dataset_id=gd_mfz5x93lmsjjjylob&notify=false&include_errors=true"
)
BRIGHTDATA_API_KEY = os.getenv('BRIGHTDATA_API_KEY', "")

# Supabase Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL', "https://fjrysnhleratybutzvkt.supabase.co" \
"")
SUPABASE_KEY = os.getenv('SUPABASE_KEY', "")


class BrightdataClient:
    """Client for interacting with Brightdata API"""
    
    def __init__(self, api_key: str, url: str):
        self.api_key = api_key
        self.url = url
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
    
    def get_snapshot_data(self, snapshot_id: str) -> Optional[Dict]:
        """
        Retrieve data for a specific snapshot ID
        
        Args:
            snapshot_id: The snapshot ID to retrieve
            
        Returns:
            JSON response data or None if failed
        """
        try:
            url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}?format=json"
            
            response = requests.get(
                url,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully retrieved data for snapshot: {snapshot_id}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error retrieving snapshot {snapshot_id}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding response for snapshot {snapshot_id}: {e}")
            return None
    
    def create_payload(self, keywords: List[str]) -> str:
        """
        Create payload for Brightdata API request
        
        Args:
            keywords: List of search keywords/queries
            
        Returns:
            JSON string payload
        """
        input_data = []
        for keyword in keywords:
            input_data.append({
                "url": "https://www.google.com/",
                "keyword": keyword,
                "language": "",
                "uule": "",
                "brd_mobile": ""
            })
        
        payload_dict = {"input": input_data}
        payload = json.dumps(payload_dict)
        return payload
    
    def send_request(self, keywords: List[str]) -> Optional[Dict]:
        """
        Send request to Brightdata API
        
        Args:
            keywords: List of search keywords to process
            
        Returns:
            Response JSON containing snapshot_id or None if failed
        """
        try:
            payload = self.create_payload(keywords)
            response = requests.post(
                self.url,
                headers=self.headers,
                data=payload,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Successfully received snapshot: {result.get('snapshot_id')}")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending request to Brightdata: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding response: {e}")
            return None


class SupabaseClient:
    """Client for interacting with Supabase"""
    
    def __init__(self, url: str, key: str):
        self.client: Client = create_client(url, key)
    
    def save_snapshot(self, snapshot_id: str) -> bool:
        """
        Save snapshot ID to Supabase
        
        Args:
            snapshot_id: The snapshot ID from Brightdata
            
        Returns:
            True if successful, False otherwise
        """
        try:
            data = {
                'snapshot_id': snapshot_id,
                'processed': False
            }
            
            response = self.client.table('snapshot_table').insert(data).execute()
            logger.info(f"Snapshot {snapshot_id} saved to Supabase")
            return True
            
        except Exception as e:
            logger.error(f"Error saving snapshot to Supabase: {e}")
            return False
    
    def get_unprocessed_snapshots(self) -> List[str]:
        """
        Get all snapshot IDs where processed = false
        
        Returns:
            List of unprocessed snapshot IDs
        """
        try:
            response = self.client.table('snapshot_table').select('snapshot_id').eq('processed', False).execute()
            
            snapshot_ids = [row['snapshot_id'] for row in response.data]
            logger.info(f"Found {len(snapshot_ids)} unprocessed snapshots")
            return snapshot_ids
            
        except Exception as e:
            logger.error(f"Error fetching unprocessed snapshots: {e}")
            return []
    
    def mark_as_processed(self, snapshot_id: str) -> bool:
        """
        Mark a snapshot as processed in Supabase
        
        Args:
            snapshot_id: The snapshot ID to mark as processed
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = self.client.table('snapshot_table').update({'processed': True}).eq('snapshot_id', snapshot_id).execute()
            logger.info(f"Marked snapshot {snapshot_id} as processed")
            return True
            
        except Exception as e:
            logger.error(f"Error marking snapshot as processed: {e}")
            return False
    
    def save_email(self, email: str) -> tuple[bool, str]:
        """
        Save a single email to Supabase email_table
        
        Args:
            email: Email address to save
            
        Returns:
            Tuple of (success: bool, error_type: str)
            error_type can be: '' (success), 'duplicate', 'error'
        """
        try:
            data = {'email': email}
            
            response = self.client.table('email_table').insert(data).execute()
            logger.info(f"Saved email {email} to Supabase")
            return True, ''
            
        except Exception as e:
            error_str = str(e)
            # Check if it's a duplicate key error
            if 'duplicate' in error_str.lower() or 'unique' in error_str.lower():
                logger.warning(f"Duplicate email {email}")
                return False, 'duplicate'
            else:
                logger.error(f"Error saving email to Supabase: {e}")
                return False, 'error'
    
    def save_response(self, snapshot_id: str, response_data: dict) -> tuple[bool, str]:
        """
        Save snapshot response to Supabase response_table
        
        Args:
            snapshot_id: The snapshot ID
            response_data: The JSON response data to save
            
        Returns:
            Tuple of (success: bool, error_type: str)
            error_type can be: '' (success), 'duplicate', 'error'
        """
        try:
            data = {
                'snapshot_id': snapshot_id,
                'response': response_data,
                'is_email_extracted': False
            }
            
            response = self.client.table('response_table').insert(data).execute()
            logger.info(f"Saved response for snapshot {snapshot_id} to Supabase")
            return True, ''
            
        except Exception as e:
            error_str = str(e)
            # Check if it's a duplicate key error
            if 'duplicate' in error_str.lower() or 'unique' in error_str.lower():
                logger.warning(f"Duplicate snapshot {snapshot_id}")
                return False, 'duplicate'
            else:
                logger.error(f"Error saving response to Supabase: {e}")
                return False, 'error'
    
    def get_unextracted_responses(self) -> List[Dict]:
        """
        Get all responses where is_email_extracted = false
        
        Returns:
            List of dictionaries with snapshot_id and response data
        """
        try:
            response = self.client.table('response_table').select('snapshot_id, response').eq('is_email_extracted', False).execute()
            
            rows = response.data if response.data else []
            logger.info(f"Found {len(rows)} unextracted responses")
            return rows
            
        except Exception as e:
            logger.error(f"Error fetching unextracted responses: {e}")
            return []
    
    def mark_email_extracted(self, snapshot_id: str) -> bool:
        """
        Mark a response row as email extracted
        
        Args:
            snapshot_id: The snapshot_id (primary key) in response_table
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = self.client.table('response_table').update({'is_email_extracted': True}).eq('snapshot_id', snapshot_id).execute()
            logger.info(f"Marked snapshot {snapshot_id} as email extracted")
            return True
            
        except Exception as e:
            logger.error(f"Error marking snapshot as extracted: {e}")
            return False
    
    def get_emails_by_date(self, start_date: str | None = None, end_date: str | None = None) -> List[Dict]:
        """
        Get all emails from email_table with optional date filtering
        
        Args:
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            
        Returns:
            List of dictionaries with email data
        """
        try:
            query = self.client.table('email_table').select('*')
            
            # Apply date filters if provided
            if start_date:
                query = query.gte('created_at', start_date)
            if end_date:
                # Add one day to include the entire end_date
                from datetime import datetime, timedelta
                end_datetime = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
                query = query.lt('created_at', end_datetime.strftime('%Y-%m-%d'))
            
            # Order by created_at descending
            query = query.order('created_at', desc=True)
            
            response = query.execute()
            rows = response.data if response.data else []
            logger.info(f"Found {len(rows)} emails")
            return rows
            
        except Exception as e:
            logger.error(f"Error fetching emails: {e}")
            return []


class EmailScraperEngine:
    """Main engine for orchestrating the scraping process"""
    
    def __init__(self, brightdata_client: BrightdataClient, supabase_client: SupabaseClient):
        self.brightdata = brightdata_client
        self.supabase = supabase_client
    
    def process_queries(self, queries: List[str], batch_size: int = 2) -> Dict[str, int]:
        """
        Process search queries in batches and save snapshots to Supabase
        
        Args:
            queries: List of search queries to process
            batch_size: Number of queries per batch (default: 2)
            
        Returns:
            Dictionary with statistics about processed queries
        """
        total_queries = len(queries)
        successful_snapshots = 0
        failed_batches = 0
        batch_count = 0
        submitted_ids = []
        
        logger.info(f"Starting to process {total_queries} queries with batch size {batch_size}")
        
        # Process queries in batches
        for i in range(0, total_queries, batch_size):
            batch_count += 1
            batch = queries[i:i + batch_size]
            batch_size = len(batch)
            
            logger.info(f"Processing batch {batch_count} ({batch_size} queries)")
            
            # Send request to Brightdata
            response = self.brightdata.send_request(batch)
            
            if response and 'snapshot_id' in response:
                snapshot_id = response['snapshot_id']
                
                # Save to Supabase
                if self.supabase.save_snapshot(snapshot_id):
                    successful_snapshots += 1
                    submitted_ids.append(snapshot_id)
                else:
                    failed_batches += 1
            else:
                logger.warning(f"Batch {batch_count} failed: No snapshot_id in response")
                failed_batches += 1
            
            # Add delay between requests to avoid rate limiting
            if i + batch_size < total_queries:
                time.sleep(2)
        
        statistics = {
            'total_queries': total_queries,
            'successful_snapshots': successful_snapshots,
            'failed_batches': failed_batches,
            'total_batches': batch_count,
            'submitted_ids': submitted_ids
        }
        
        return statistics


def main():
    """Main entry point"""
    
    # Example queries - User should provide these
    queries = [
        "pizza restaurants near me",
        "coffee shops downtown",
        "best sushi in the city",
        "italian restaurants",
        "breakfast cafes",
        # Add more queries as needed
    ]
    
    try:
        # Initialize clients
        brightdata_client = BrightdataClient(BRIGHTDATA_API_KEY, BRIGHTDATA_URL)
        supabase_client = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        
        # Create engine and process queries
        engine = EmailScraperEngine(brightdata_client, supabase_client)
        stats = engine.process_queries(queries)
        
        # Log final statistics
        logger.info("=" * 50)
        logger.info("PROCESSING COMPLETE")
        logger.info(f"Total queries: {stats['total_queries']}")
        logger.info(f"Successful snapshots: {stats['successful_snapshots']}")
        logger.info(f"Failed batches: {stats['failed_batches']}")
        logger.info(f"Total batches processed: {stats['total_batches']}")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Fatal error in main execution: {e}")
        raise


if __name__ == "__main__":
    main()
