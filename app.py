"""
Streamlit UI for Email Scraper - Brightdata API Integration
"""

import streamlit as st
import csv
import os
import re
from io import StringIO
from dotenv import load_dotenv
from email_scraper import (
    BrightdataClient,
    SupabaseClient,
    EmailScraperEngine,
    logger
)

# Load environment variables
load_dotenv()

# Page Configuration
st.set_page_config(
    page_title="Email Scraper",
    page_icon="📧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 2rem;
    }
    .stTitle {
        color: #1f77b4;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)


def initialize_session_state():
    """Initialize Streamlit session state"""
    if 'queries_loaded' not in st.session_state:
        st.session_state.queries_loaded = False
        st.session_state.queries = []
        st.session_state.processing_started = False
        st.session_state.processing_complete = False
        st.session_state.results = None
        st.session_state.stage2_results = None


def validate_environment():
    """Validate that all required environment variables are set"""
    required_vars = {
        'BRIGHTDATA_URL': os.getenv('BRIGHTDATA_URL'),
        'SUPABASE_URL': os.getenv('SUPABASE_URL'),
        'SUPABASE_KEY': os.getenv('SUPABASE_KEY')
    }
    
    missing = [key for key, value in required_vars.items() if not value]
    
    if missing:
        return False, f"Missing environment variables: {', '.join(missing)}"
    
    return True, "All environment variables configured"


def load_csv_queries(uploaded_file) -> list:
    """
    Load queries from uploaded CSV file
    
    Args:
        uploaded_file: Streamlit uploaded file object
        
    Returns:
        List of queries from the first column
    """
    try:
        stringio = StringIO(uploaded_file.getvalue().decode("utf8"))
        csv_reader = csv.reader(stringio)
        
        # Skip header
        next(csv_reader)
        
        queries = []
        for row in csv_reader:
            if row and len(row) > 0:
                query = row[0].strip()
                if query:
                    queries.append(query)
        
        return queries
    except Exception as e:
        st.error(f"Error reading CSV file: {str(e)}")
        return []


def display_header():
    """Display application header"""
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.title("📧 Email Scraper")
        st.markdown("*Powered by Bright Data & Supabase*")
    
    with col2:
        st.info("v1.0", icon="ℹ️")


def display_sidebar():
    """Display sidebar with configuration"""
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Bright Data API Key input
        st.subheader("Bright Data API Key")
        api_key = st.text_input(
            "Enter API Key",
            type="password",
            help="Your Bright Data API key"
        )
        
        st.divider()
        
        # Environment status
        is_valid, message = validate_environment()
        
        if is_valid:
            st.success(message, icon="✅")
        else:
            st.error(message, icon="❌")
            st.markdown("""
            ### Setup Required
            Please configure your `.env` file with:
            - `BRIGHTDATA_URL`
            - `SUPABASE_URL`
            - `SUPABASE_KEY`
            """)
            return False, 2, None
        
        st.divider()
        
        # Settings
        st.subheader("Settings")
        batch_size = st.slider(
            "Batch Size",
            min_value=1,
            max_value=5,
            value=2,
            help="Number of queries per API request"
        )
        
        request_delay = st.slider(
            "Request Delay (seconds)",
            min_value=0,
            max_value=10,
            value=2,
            help="Delay between API requests"
        )
        
        return True, batch_size, api_key


def display_upload_section():
    """Display CSV file upload section"""
    st.header("📁 Upload CSV File")
    
    uploaded_file = st.file_uploader(
        "Choose a CSV file",
        type="csv"
    )
    
    return uploaded_file


def display_queries_preview(queries):
    """Display preview of loaded queries"""
    st.subheader("📋 Loaded Queries")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Queries", len(queries))
    with col2:
        st.metric("Batches (÷2)", (len(queries) + 1) // 2)
    with col3:
        st.metric("API Requests", (len(queries) + 1) // 2)
    
    # Show preview
    with st.expander("👁️ Preview Queries", expanded=False):
        preview_cols = st.columns([1, 4])
        
        with preview_cols[0]:
            st.write("**#**")
            for i in range(1, min(len(queries) + 1, 21)):
                st.write(i)
        
        with preview_cols[1]:
            st.write("**Query**")
            for query in queries[:20]:
                st.write(query)
        
        if len(queries) > 20:
            st.info(f"... and {len(queries) - 20} more queries")


def display_processing_section():
    """Display processing controls and results"""
    st.header("🚀 Processing")
    
    start_button = st.button(
        "Process",
        use_container_width=True,
        type="primary",
        key="stage1_process"
    )
    
    return start_button


def process_queries(queries):
    """
    Process queries using the scraper engine
    
    Args:
        queries: List of search queries
        
    Returns:
        Statistics dictionary from processing
    """
    try:
        # Get API key from session state
        api_key = st.session_state.get('api_key', '')
        
        if not api_key:
            st.error("Please enter Bright Data API Key in the sidebar")
            return None
        
        # Initialize clients
        brightdata_url = os.getenv('BRIGHTDATA_URL') or ""
        supabase_url = os.getenv('SUPABASE_URL') or ""
        supabase_key = os.getenv('SUPABASE_KEY') or ""
        
        brightdata_client = BrightdataClient(api_key, brightdata_url)
        supabase_client = SupabaseClient(supabase_url, supabase_key)
        
        # Create engine and process
        engine = EmailScraperEngine(brightdata_client, supabase_client)
        
        # Create progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Process queries with batch size from session state
        batch_size = st.session_state.get('batch_size', 2)
        stats = engine.process_queries(queries, batch_size)
        
        progress_bar.progress(1.0)
        status_text.text("Processing complete!")
        
        return stats
        
    except Exception as e:
        st.error(f"Error during processing: {str(e)}")
        return None


def display_results(stats):
    """Display processing results"""
    st.header("✅ Results")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Queries",
            stats['total_queries'],
            delta=None
        )
    
    with col2:
        st.metric(
            "Successful",
            stats['successful_snapshots'],
            delta=f"{(stats['successful_snapshots']/stats['total_batches']*100):.0f}%"
        )
    
    with col3:
        st.metric(
            "Failed",
            stats['failed_batches'],
            delta=f"{(stats['failed_batches']/stats['total_batches']*100):.0f}%"
        )
    
    with col4:
        st.metric(
            "Batches",
            stats['total_batches'],
            delta="2 per batch"
        )
    
    # Status indicator
    if stats['failed_batches'] == 0:
        st.success("Processing complete")
    elif stats['successful_snapshots'] > 0:
        st.warning(f"{stats['failed_batches']} batches failed")
    else:
        st.error("Processing failed")


def process_unprocessed_snapshots():
    """
    Process all unprocessed snapshots from Supabase
    
    Returns:
        Dictionary with statistics about processed snapshots
    """
    try:
        # Get API key from session state
        api_key = st.session_state.get('api_key', '')
        
        if not api_key:
            st.error("Please enter Bright Data API Key in the sidebar")
            return {
                'total': 0,
                'successful': 0,
                'failed': 0,
                'skipped': 0,
                'db_errors': 0,
                'duplicate_snapshots': 0,
                'message': 'API key not provided'
            }
        
        # Initialize clients
        brightdata_url = os.getenv('BRIGHTDATA_URL') or ""
        supabase_url = os.getenv('SUPABASE_URL') or ""
        supabase_key = os.getenv('SUPABASE_KEY') or ""
        
        brightdata_client = BrightdataClient(api_key, brightdata_url)
        supabase_client = SupabaseClient(supabase_url, supabase_key)
        
        # Get unprocessed snapshots
        snapshot_ids = supabase_client.get_unprocessed_snapshots()
        
        if not snapshot_ids:
            return {
                'total': 0,
                'successful': 0,
                'failed': 0,
                'skipped': 0,
                'message': 'No unprocessed snapshots found'
            }
        
        # Initialize counters
        total = len(snapshot_ids)
        successful = 0
        failed = 0
        skipped = 0
        db_errors = 0
        duplicate_snapshots = 0
        running_snapshots = 0
        
        # Process each snapshot with progress
        progress_bar = st.progress(0)
        status_text = st.empty()
        error_text = st.empty()
        
        for idx, snapshot_id in enumerate(snapshot_ids):
            # Update progress
            progress = (idx + 1) / total
            progress_bar.progress(progress)
            status_text.text(f"Processing {idx + 1}/{total}: {snapshot_id}")
            
            # Retrieve snapshot data
            data, is_running = brightdata_client.get_snapshot_data(snapshot_id)
            
            if is_running:
                # Skip snapshots that are still running
                running_snapshots += 1
                logger.warning(f"Snapshot {snapshot_id} is still running - skipping")
            elif data:
                # Save response to Supabase response_table
                save_success, error_type = supabase_client.save_response(snapshot_id, data)
                if save_success:
                    # Mark as processed in snapshot_table
                    if supabase_client.mark_as_processed(snapshot_id):
                        successful += 1
                        logger.info(f"Successfully processed snapshot {snapshot_id}")
                    else:
                        failed += 1
                        db_errors += 1
                        error_msg = f"Database error: Failed to mark {snapshot_id} as processed"
                        logger.error(error_msg)
                        error_text.error(error_msg)
                elif error_type == 'duplicate':
                    duplicate_snapshots += 1
                    # Still mark as processed since response already exists
                    supabase_client.mark_as_processed(snapshot_id)
                else:
                    failed += 1
                    db_errors += 1
                    error_msg = f"Database error: Failed to save response for {snapshot_id}"
                    logger.error(error_msg)
                    error_text.error(error_msg)
            else:
                # Skip this snapshot
                skipped += 1
                logger.warning(f"Skipped snapshot {snapshot_id} - no data received")
            
            # Add small delay
            import time
            time.sleep(0.5)
        
        progress_bar.progress(1.0)
        status_text.text("Processing complete!")
        
        return {
            'total': total,
            'successful': successful,
            'failed': failed,
            'skipped': skipped,
            'db_errors': db_errors,
            'duplicate_snapshots': duplicate_snapshots,
            'running_snapshots': running_snapshots,
            'message': f'Processed {successful}/{total} snapshots successfully'
        }
        
    except Exception as e:
        logger.error(f"Error in process_unprocessed_snapshots: {e}")
        st.error(f"Critical error: {str(e)}")
        return {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'db_errors': 0,
            'duplicate_snapshots': 0,
            'running_snapshots': 0,
            'message': f'Error: {str(e)}'
        }


def display_stage2_tab():
    """Display Stage 2 tab for retrieving snapshot data"""
    st.header("📥 Stage 2: Retrieve Snapshot Data")
    
    # Initialize clients to get count
    supabase_url = os.getenv('SUPABASE_URL') or ""
    supabase_key = os.getenv('SUPABASE_KEY') or ""
    supabase_client = SupabaseClient(supabase_url, supabase_key)
    
    # Get unprocessed count
    snapshot_ids = supabase_client.get_unprocessed_snapshots()
    eligible_count = len(snapshot_ids)
    
    st.metric("Eligible Rows", eligible_count)
    
    st.divider()
    
    retrieve_button = st.button(
        "Process",
        use_container_width=True,
        type="primary",
        key="stage2_process"
    )
    
    # Process all unprocessed snapshots when button is clicked
    if retrieve_button:
        if eligible_count == 0:
            st.info("No unprocessed snapshots found")
        else:
            with st.spinner("Processing..."):
                result = process_unprocessed_snapshots()
            
            st.divider()
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total", result['total'])
            
            with col2:
                st.metric("Successful", result['successful'])
            
            with col3:
                st.metric("Skipped", result['skipped'])
            
            with col4:
                st.metric("Failed", result['failed'])
            
            # Show duplicate info if any
            if result.get('duplicate_snapshots', 0) > 0:
                st.info(f"ℹ️ {result['duplicate_snapshots']} duplicate snapshots skipped (already exist in database)")
            
            # Show running snapshots info if any
            if result.get('running_snapshots', 0) > 0:
                st.warning(f"⚠️ {result['running_snapshots']} snapshots still running (will be processed later)")
            
            # Show database errors if any
            if result.get('db_errors', 0) > 0:
                st.error(f"⚠️ {result['db_errors']} database errors occurred")
            
            if result['successful'] == result['total']:
                st.success("Processing complete")
            elif result['successful'] > 0:
                st.warning(f"{result['skipped']} skipped, {result['failed']} failed")
            else:
                st.error("Processing failed")


def extract_emails_from_text(text: str) -> list:
    """
    Extract email addresses from text using regex
    
    Args:
        text: Text content to extract emails from
        
    Returns:
        List of unique email addresses
    """
    # Email regex pattern
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    
    # Find all emails
    emails = re.findall(email_pattern, text)
    
    # Return unique emails
    return list(set(emails))


def extract_emails_from_json(json_data):
    """
    Extract email addresses from JSON data using regex
    
    Args:
        json_data: JSON data (dict, list, or any JSON structure)
        
    Returns:
        List of unique email addresses
    """
    import json
    
    # Convert JSON to string to search through it
    json_string = json.dumps(json_data)
    
    # Extract emails from the JSON string
    return extract_emails_from_text(json_string)


def process_all_responses_for_emails(total_count: int):
    """
    Process all unextracted responses sequentially in batches
    
    Args:
        total_count: Total number of eligible rows to process
        
    Returns:
        Dictionary with extraction statistics
    """
    try:
        # Initialize Supabase client
        supabase_url = os.getenv('SUPABASE_URL') or ""
        supabase_key = os.getenv('SUPABASE_KEY') or ""
        supabase_client = SupabaseClient(supabase_url, supabase_key)
        
        BATCH_SIZE = 20
        num_batches = (total_count + BATCH_SIZE - 1) // BATCH_SIZE
        
        # Initialize overall counters
        total_processed = 0
        total_successful = 0
        total_failed = 0
        total_emails_extracted = 0
        total_db_errors = 0
        total_duplicate_emails = 0
        
        # Create progress containers
        overall_progress_bar = st.progress(0)
        batch_status = st.empty()
        current_batch_progress = st.empty()
        email_log = st.empty()
        
        # Process in batches
        for batch_num in range(num_batches):
            batch_status.info(f"📦 Processing Batch {batch_num + 1}/{num_batches}")
            
            # Fetch current batch (always from offset 0 since we mark as extracted)
            rows = supabase_client.get_unextracted_responses(limit=BATCH_SIZE, offset=0)
            
            if not rows:
                batch_status.warning(f"Batch {batch_num + 1}: No more rows to process")
                break
            
            # Process each row in current batch
            batch_total = len(rows)
            batch_emails = 0
            batch_duplicates = 0
            batch_errors = 0
            
            for idx, row in enumerate(rows):
                snapshot_id = row.get('snapshot_id')
                response_data = row.get('response')
                
                if not snapshot_id or not response_data:
                    total_failed += 1
                    batch_errors += 1
                    continue
                
                # Extract emails from response
                emails = extract_emails_from_json(response_data)
                
                if emails:
                    # Save each email
                    for email in emails:
                        success, error_type = supabase_client.save_email(email)
                        if success:
                            batch_emails += 1
                            email_log.success(f"✅ Batch {batch_num + 1} [{idx + 1}/{batch_total}]: Saved {email}")
                        elif error_type == 'duplicate':
                            batch_duplicates += 1
                            email_log.info(f"ℹ️ Batch {batch_num + 1} [{idx + 1}/{batch_total}]: Duplicate {email}")
                        else:
                            batch_errors += 1
                            email_log.error(f"❌ Batch {batch_num + 1} [{idx + 1}/{batch_total}]: Failed {email}")
                
                # Mark as extracted
                if supabase_client.mark_email_extracted(snapshot_id):
                    total_successful += 1
                else:
                    total_failed += 1
                    batch_errors += 1
                
                total_processed += 1
            
            # Update overall progress
            overall_progress = min(total_processed / total_count, 1.0)
            overall_progress_bar.progress(overall_progress)
            current_batch_progress.text(f"Batch {batch_num + 1} Complete: {batch_emails} emails saved, {batch_duplicates} duplicates, {batch_errors} errors")
            
            # Update totals
            total_emails_extracted += batch_emails
            total_duplicate_emails += batch_duplicates
            total_db_errors += batch_errors
        
        # Clear progress displays
        overall_progress_bar.progress(1.0)
        batch_status.success(f"✅ All {num_batches} batches processed!")
        
        return {
            'total': total_processed,
            'successful': total_successful,
            'failed': total_failed,
            'total_emails': total_emails_extracted,
            'duplicate_emails': total_duplicate_emails,
            'db_errors': total_db_errors,
            'message': f'Processed {total_successful}/{total_processed} responses successfully'
        }
        
    except Exception as e:
        logger.error(f"Error in process_all_responses_for_emails: {e}")
        st.error(f"Critical error: {str(e)}")
        return {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'total_emails': 0,
            'duplicate_emails': 0,
            'db_errors': 0,
            'message': f'Error: {str(e)}'
        }


def process_responses_for_emails(batch_size: int = 20):
    """
    Process unextracted responses from Supabase and extract emails
    DEPRECATED: Use process_all_responses_for_emails instead
    
    Args:
        batch_size: Total number of rows to process (fetched in sub-batches of 20)
        
    Returns:
        Dictionary with extraction statistics
    """
    try:
        # Initialize Supabase client
        supabase_url = os.getenv('SUPABASE_URL') or ""
        supabase_key = os.getenv('SUPABASE_KEY') or ""
        supabase_client = SupabaseClient(supabase_url, supabase_key)
        
        # Fetch in sub-batches of 20 to avoid timeout
        SUB_BATCH_SIZE = 20
        all_rows = []
        
        for offset in range(0, batch_size, SUB_BATCH_SIZE):
            current_limit = min(SUB_BATCH_SIZE, batch_size - offset)
            rows_batch = supabase_client.get_unextracted_responses(limit=current_limit, offset=offset)
            all_rows.extend(rows_batch)
            if len(rows_batch) < current_limit:
                # No more rows available
                break
        
        rows = all_rows
        
        if not rows:
            return {
                'total': 0,
                'successful': 0,
                'failed': 0,
                'total_emails': 0,
                'message': 'No unextracted responses found'
            }
        
        # Initialize counters
        total = len(rows)
        successful = 0
        failed = 0
        total_emails_extracted = 0
        db_errors = 0
        duplicate_emails = 0
        
        # Process each response with progress
        progress_bar = st.progress(0)
        status_text = st.empty()
        email_log = st.empty()
        
        for idx, row in enumerate(rows):
            # Update progress
            progress = (idx + 1) / total
            progress_bar.progress(progress)
            status_text.text(f"Processing {idx + 1}/{total}: Snapshot {row['snapshot_id']}")
            
            # Extract emails from response JSON
            response_data = row.get('response', {})
            emails = extract_emails_from_json(response_data)
            
            email_save_failed = False
            if emails:
                # Save each email individually
                for email in emails:
                    success, error_type = supabase_client.save_email(email)
                    if success:
                        total_emails_extracted += 1
                        email_log.success(f"✅ Saved: {email}")
                    elif error_type == 'duplicate':
                        duplicate_emails += 1
                        email_log.info(f"ℹ️ Duplicate: {email} (already exists)")
                    else:
                        email_save_failed = True
                        db_errors += 1
                        email_log.error(f"❌ Failed: {email} (database error)")
                        logger.error(f"Database error: Failed to save email {email}")
                logger.info(f"Extracted {len(emails)} emails from snapshot {row['snapshot_id']}")
            
            # Mark as extracted regardless of whether emails were found
            if supabase_client.mark_email_extracted(row['snapshot_id']):
                successful += 1
            else:
                failed += 1
                db_errors += 1
                error_msg = f"Database error: Failed to mark {row['snapshot_id']} as extracted"
                logger.error(error_msg)
                email_log.error(f"❌ {error_msg}")
        
        progress_bar.progress(1.0)
        status_text.text("Processing complete!")
        
        return {
            'total': total,
            'successful': successful,
            'failed': failed,
            'total_emails': total_emails_extracted,
            'db_errors': db_errors,
            'duplicate_emails': duplicate_emails,
            'message': f'Processed {successful}/{total} responses, extracted {total_emails_extracted} emails'
        }
        
    except Exception as e:
        logger.error(f"Error processing responses for emails: {e}")
        st.error(f"Critical error: {str(e)}")
        return {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'total_emails': 0,
            'db_errors': 0,
            'duplicate_emails': 0,
            'message': f'Error: {str(e)}'
        }


def display_stage3_tab():
    """Display Stage 3 tab for extracting emails from response_table"""
    st.header("📧 Stage 3: Extract Emails")
    
    # Initialize clients to get count
    supabase_url = os.getenv('SUPABASE_URL') or ""
    supabase_key = os.getenv('SUPABASE_KEY') or ""
    supabase_client = SupabaseClient(supabase_url, supabase_key)
    
    # Get unextracted count
    eligible_count = supabase_client.count_unextracted_responses()
    
    # Calculate number of batches
    BATCH_SIZE = 20
    num_batches = (eligible_count + BATCH_SIZE - 1) // BATCH_SIZE if eligible_count > 0 else 0
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.metric("Total Eligible Rows", eligible_count)
    
    with col2:
        st.metric("Batches (20 rows each)", num_batches)
    
    st.divider()
    
    process_button = st.button(
        "Process All",
        use_container_width=True,
        type="primary",
        key="stage3_process",
        help=f"Process all {eligible_count} rows in {num_batches} sequential batches"
    )
    
    # Process when button clicked
    if process_button:
        if eligible_count == 0:
            st.info("No unextracted responses found")
        else:
            result = process_all_responses_for_emails(eligible_count)
            
            st.divider()
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total", result['total'])
            
            with col2:
                st.metric("Processed", result['successful'])
            
            with col3:
                st.metric("Failed", result['failed'])
            
            with col4:
                st.metric("Emails", result['total_emails'])
            
            # Show duplicate info if any
            if result.get('duplicate_emails', 0) > 0:
                st.info(f"ℹ️ {result['duplicate_emails']} duplicate emails skipped (already exist in database)")
            
            # Show database errors if any
            if result.get('db_errors', 0) > 0:
                st.error(f"⚠️ {result['db_errors']} database errors occurred")
            
            if result['successful'] == result['total']:
                st.success("Processing complete")
            elif result['successful'] > 0:
                st.warning(f"{result['failed']} failed")
            else:
                st.error("Processing failed")


def display_stage4_tab():
    """Display Stage 4 tab for viewing emails by date"""
    st.header("📊 Stage 4: View Emails")
    
    # Date filter inputs
    col1, col2 = st.columns(2)
    
    with col1:
        start_date = st.date_input("Start Date", value=None)
    
    with col2:
        end_date = st.date_input("End Date", value=None)
    
    st.divider()
    
    fetch_button = st.button(
        "Fetch Emails",
        use_container_width=True,
        type="primary",
        key="stage4_fetch"
    )
    
    if fetch_button:
        # Initialize Supabase client
        supabase_url = os.getenv('SUPABASE_URL') or ""
        supabase_key = os.getenv('SUPABASE_KEY') or ""
        supabase_client = SupabaseClient(supabase_url, supabase_key)
        
        # Convert dates to string format if provided
        start_date_str = None
        end_date_str = None
        
        if start_date:
            # Handle both single date and tuple
            if isinstance(start_date, tuple):
                start_date_str = start_date[0].strftime('%Y-%m-%d') if start_date[0] else None
            else:
                start_date_str = start_date.strftime('%Y-%m-%d')
        
        if end_date:
            # Handle both single date and tuple
            if isinstance(end_date, tuple):
                end_date_str = end_date[0].strftime('%Y-%m-%d') if end_date[0] else None
            else:
                end_date_str = end_date.strftime('%Y-%m-%d')
        
        with st.spinner("Fetching emails..."):
            emails = supabase_client.get_emails_by_date(start_date_str, end_date_str)
        
        st.divider()
        
        if emails:
            st.metric("Total Emails", len(emails))
            
            st.divider()
            
            # Display emails in a table
            st.subheader("📮 Email List")
            
            # Create DataFrame for better display
            import pandas as pd
            df = pd.DataFrame(emails)
            
            # Display only email column if exists, otherwise show all
            if 'email' in df.columns:
                if 'created_at' in df.columns:
                    df_display = df[['email', 'created_at']]
                    df_display['created_at'] = pd.to_datetime(df_display['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    df_display = df[['email']]
                
                st.dataframe(df_display, use_container_width=True, height=400)
                
                # Download button
                st.divider()
                csv = df_display.to_csv(index=False)
                st.download_button(
                    label="💾 Download as CSV",
                    data=csv,
                    file_name=f"emails_{start_date_str or 'all'}_{end_date_str or 'all'}.csv",
                    mime="text/csv"
                )
            else:
                st.dataframe(df, use_container_width=True)
        else:
            st.info("No emails found for the selected date range")


def main():
    """Main Streamlit application"""
    initialize_session_state()
    
    # Display header
    display_header()
    
    # Display sidebar and validate environment
    is_configured, batch_size, api_key = display_sidebar()
    
    if not is_configured:
        st.stop()
    
    # Store batch_size and api_key in session state
    st.session_state.batch_size = batch_size
    st.session_state.api_key = api_key
    
    st.divider()
    
    # Create tabs for Stage 1, Stage 2, Stage 3, and Stage 4
    tab1, tab2, tab3, tab4 = st.tabs(["📤 Stage 1: Upload & Process", "📥 Stage 2: Retrieve Data", "📧 Stage 3: Extract Emails", "📊 Stage 4: View Emails"])
    
    with tab1:
        # Upload section
        uploaded_file = display_upload_section()
    
        # Process uploaded file
        if uploaded_file is not None:
            queries = load_csv_queries(uploaded_file)
            
            if queries:
                st.session_state.queries_loaded = True
                st.session_state.queries = queries
                
                st.divider()
                
                # Display queries preview
                display_queries_preview(queries)
                
                st.divider()
                
                # Processing section
                start_button = display_processing_section()
                
                if start_button:
                    if queries:
                        st.session_state.processing_started = True
                        
                        with st.spinner("Processing queries..."):
                            results = process_queries(queries)
                        
                        if results:
                            st.session_state.processing_complete = True
                            st.session_state.results = results
                            
                            st.divider()
                            display_results(results)
                            
                            # Download report option
                            st.divider()
                            st.subheader("📥 Export Results")
                            
                            report = f"""
Email Scraper - Processing Report
==================================

Date: {pd.Timestamp.now()}
Total Queries: {results['total_queries']}
Total Batches: {results['total_batches']}
Successful Snapshots: {results['successful_snapshots']}
Failed Batches: {results['failed_batches']}
Success Rate: {(results['successful_snapshots']/results['total_batches']*100):.1f}%
"""
                            
                            st.download_button(
                                label="📊 Download Report",
                                data=report,
                                file_name="scraper_report.txt",
                                mime="text/plain"
                            )
                    else:
                        st.error("No queries loaded")
            else:
                st.error("No queries found in CSV file")

    
    with tab2:
        display_stage2_tab()
    
    with tab3:
        display_stage3_tab()
    
    with tab4:
        display_stage4_tab()


if __name__ == "__main__":
    try:
        import pandas as pd
    except ImportError:
        st.error("Please install pandas: pip install pandas")
        st.stop()
    
    main()
