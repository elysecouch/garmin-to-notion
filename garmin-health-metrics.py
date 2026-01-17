#!/usr/bin/env python3
"""
Garmin Health Metrics to Notion Sync
Syncs HRV, Resting Heart Rate, and VO2 Max data from Garmin Connect to Notion
"""

import os
import sys
from datetime import datetime, timedelta
from garminconnect import Garmin, GarminConnectAuthenticationError
from notion_client import Client
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
GARMIN_EMAIL = os.getenv('GARMIN_EMAIL')
GARMIN_PASSWORD = os.getenv('GARMIN_PASSWORD')
NOTION_TOKEN = os.getenv('NOTION_TOKEN')
NOTION_HEALTH_DATABASE_ID = os.getenv('NOTION_HEALTH_DATABASE_ID')

# Number of days to fetch (default: yesterday only, change to fetch more history)
DAYS_BACK = int(os.getenv('HEALTH_DAYS_BACK', 1))


def check_credentials():
    """Check if all required credentials are set."""
    if not all([GARMIN_EMAIL, GARMIN_PASSWORD, NOTION_TOKEN, NOTION_HEALTH_DATABASE_ID]):
        logger.error("Missing required environment variables!")
        logger.error("Required: GARMIN_EMAIL, GARMIN_PASSWORD, NOTION_TOKEN, NOTION_HEALTH_DATABASE_ID")
        sys.exit(1)


def connect_garmin():
    """Connect to Garmin and return client."""
    try:
        logger.info("Connecting to Garmin Connect...")
        client = Garmin(GARMIN_EMAIL, GARMIN_PASSWORD)
        client.login()
        logger.info("✓ Successfully connected to Garmin")
        return client
    except GarminConnectAuthenticationError as e:
        logger.error(f"Failed to authenticate with Garmin: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error connecting to Garmin: {e}")
        sys.exit(1)


def entry_exists(notion_client, database_id, date_str):
    """Check if an entry for this date already exists in Notion."""
    try:
        # Query the database for entries with this date (matching original pattern)
        query = notion_client.databases.query(
            database_id=database_id,
            filter={
                "property": "Date",
                "date": {
                    "equals": date_str
                }
            }
        )
        return query.get('results', [])
    except Exception as e:
        logger.error(f"Error checking existing entries: {e}")
        return []


def create_notion_entry(notion_client, database_id, date_str, hrv_data, rhr_data, vo2_data):
    """Create a new entry in Notion database."""
    try:
        # Prepare properties - start with the Date
        properties = {
            "Date": {
                "date": {
                    "start": date_str
                }
            }
        }
        
        # Add HRV data if available (nested under hrvSummary)
        if hrv_data and 'hrvSummary' in hrv_data:
            hrv_summary = hrv_data['hrvSummary']
            logger.info(f"  HRV summary: {hrv_summary}")
            
            if 'lastNightAvg' in hrv_summary and hrv_summary['lastNightAvg'] is not None:
                properties["Last Night HRV"] = {
                    "number": hrv_summary['lastNightAvg']
                }
            if 'weeklyAvg' in hrv_summary and hrv_summary['weeklyAvg'] is not None:
                properties["Weekly Avg HRV"] = {
                    "number": hrv_summary['weeklyAvg']
                }
            if 'status' in hrv_summary and hrv_summary['status'] is not None:
                properties["HRV Status"] = {
                    "rich_text": [{
                        "text": {
                            "content": str(hrv_summary['status'])
                        }
                    }]
                }
        
        # Add Resting Heart Rate if available (nested structure)
        if rhr_data and 'allMetrics' in rhr_data:
            logger.info(f"  RHR metrics: {rhr_data.get('allMetrics', {})}")
            metrics_map = rhr_data.get('allMetrics', {}).get('metricsMap', {})
            rhr_list = metrics_map.get('WELLNESS_RESTING_HEART_RATE', [])
            
            if rhr_list and len(rhr_list) > 0 and 'value' in rhr_list[0]:
                rhr_value = rhr_list[0]['value']
                properties["Resting Heart Rate"] = {
                    "number": int(rhr_value)
                }
        
        # Add VO2 Max data if available
        if vo2_data:
            logger.info(f"  VO2 data: {vo2_data}")
            if 'vo2MaxValue' in vo2_data and vo2_data['vo2MaxValue'] is not None:
                properties["VO2 Max"] = {
                    "number": vo2_data['vo2MaxValue']
                }
            if 'fitnessAge' in vo2_data and vo2_data['fitnessAge'] is not None:
                properties["Fitness Age"] = {
                    "number": vo2_data['fitnessAge']
                }
        
        # Create the page (matching original pattern with ** unpacking)
        page = {
            "parent": {"database_id": database_id},
            "properties": properties
        }
        
        notion_client.pages.create(**page)
        
        logger.info(f"✓ Created entry for {date_str}")
        return True
        
    except Exception as e:
        logger.error(f"Error creating Notion entry for {date_str}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def update_notion_entry(notion_client, page_id, hrv_data, rhr_data, vo2_data):
    """Update an existing entry in Notion database."""
    try:
        # Prepare properties
        properties = {}
        
        # Add HRV data if available (nested under hrvSummary)
        if hrv_data and 'hrvSummary' in hrv_data:
            hrv_summary = hrv_data['hrvSummary']
            
            if 'lastNightAvg' in hrv_summary and hrv_summary['lastNightAvg'] is not None:
                properties["Last Night HRV"] = {
                    "number": hrv_summary['lastNightAvg']
                }
            if 'weeklyAvg' in hrv_summary and hrv_summary['weeklyAvg'] is not None:
                properties["Weekly Avg HRV"] = {
                    "number": hrv_summary['weeklyAvg']
                }
            if 'status' in hrv_summary and hrv_summary['status'] is not None:
                properties["HRV Status"] = {
                    "rich_text": [{
                        "text": {
                            "content": str(hrv_summary['status'])
                        }
                    }]
                }
        
        # Add Resting Heart Rate if available (nested structure)
        if rhr_data and 'allMetrics' in rhr_data:
            metrics_map = rhr_data.get('allMetrics', {}).get('metricsMap', {})
            rhr_list = metrics_map.get('WELLNESS_RESTING_HEART_RATE', [])
            
            if rhr_list and len(rhr_list) > 0 and 'value' in rhr_list[0]:
                rhr_value = rhr_list[0]['value']
                properties["Resting Heart Rate"] = {
                    "number": int(rhr_value)
                }
        
        # Add VO2 Max data if available
        if vo2_data:
            if 'vo2MaxValue' in vo2_data and vo2_data['vo2MaxValue'] is not None:
                properties["VO2 Max"] = {
                    "number": vo2_data['vo2MaxValue']
                }
            if 'fitnessAge' in vo2_data and vo2_data['fitnessAge'] is not None:
                properties["Fitness Age"] = {
                    "number": vo2_data['fitnessAge']
                }
        
        # Update the page (matching original pattern with ** unpacking)
        update = {
            "page_id": page_id,
            "properties": properties
        }
        
        notion_client.pages.update(**update)
        
        return True
        
    except Exception as e:
        logger.error(f"Error updating Notion entry: {e}")
        return False


def main():
    """Main function to sync Garmin health metrics to Notion."""
    logger.info("=== Starting Garmin Health Metrics Sync ===")
    
    # Check credentials
    check_credentials()
    
    # Connect to services
    garmin_client = connect_garmin()
    notion_client = Client(auth=NOTION_TOKEN)
    
    # Process each day
    success_count = 0
    error_count = 0
    
    for days_ago in range(DAYS_BACK):
        date = datetime.now() - timedelta(days=days_ago)
        date_str = date.strftime('%Y-%m-%d')
        
        logger.info(f"\nProcessing {date_str}...")
        
        try:
            # Fetch HRV data
            hrv_data = None
            try:
                hrv_data = garmin_client.get_hrv_data(date_str)
                logger.info(f"  ✓ HRV data retrieved")
            except Exception as e:
                logger.warning(f"  ⚠ Could not fetch HRV data: {e}")
            
            # Fetch Resting Heart Rate
            rhr_data = None
            try:
                rhr_data = garmin_client.get_rhr_day(date_str)
                logger.info(f"  ✓ RHR data retrieved")
            except Exception as e:
                logger.warning(f"  ⚠ Could not fetch RHR data: {e}")
            
            # Fetch VO2 Max / Max Metrics
            vo2_data = None
            try:
                vo2_data = garmin_client.get_max_metrics(date_str)
                logger.info(f"  ✓ VO2 Max data retrieved")
            except Exception as e:
                logger.warning(f"  ⚠ Could not fetch VO2 Max data: {e}")
            
            # Check if we have any data to sync
            if not any([hrv_data, rhr_data, vo2_data]):
                logger.info(f"  ⊘ No health data available for {date_str}")
                continue
            
            # Check if entry exists in Notion
            existing_entries = entry_exists(notion_client, NOTION_HEALTH_DATABASE_ID, date_str)
            
            if existing_entries:
                # Update existing entry
                page_id = existing_entries[0]['id']
                if update_notion_entry(notion_client, page_id, hrv_data, rhr_data, vo2_data):
                    logger.info(f"  ✓ Updated existing entry for {date_str}")
                    success_count += 1
                else:
                    error_count += 1
            else:
                # Create new entry
                if create_notion_entry(notion_client, NOTION_HEALTH_DATABASE_ID, date_str, 
                                     hrv_data, rhr_data, vo2_data):
                    success_count += 1
                else:
                    error_count += 1
                    
        except Exception as e:
            logger.error(f"  ✗ Error processing {date_str}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            error_count += 1
    
    # Summary
    logger.info("\n=== Sync Complete ===")
    logger.info(f"✓ Successfully synced: {success_count} days")
    if error_count > 0:
        logger.info(f"✗ Errors: {error_count} days")
    
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
