# scripts/update_timestamps_lstm.py
"""
Script to update voting timestamps with realistic temporal patterns for LSTM analysis
Based on academic research on voting behavior and blockchain systems
Supports configurable time windows and voting distributions
"""

import random
import time
from datetime import datetime, timedelta
import pytz
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError


# MongoDB Configuration
MONGO_URI = "mongodb://admin:votaciones2025@mongodb:27017/"
DATABASE_NAME = "votaciones_usa"
COLLECTION_NAME = "registros_votacion"
BATCH_SIZE = 10000


def create_hourly_distribution(total_hours, end_weight_percentage=30):
    """
    Creates hourly voting distribution with configurable end concentration
    
    Args:
        total_hours (int): Total voting window hours (e.g., 12, 16, 24)
        end_weight_percentage (float): Percentage of votes in final 2 hours
    
    Returns:
        dict: Hour-to-weight mapping for temporal distribution
    """
    # Base distribution - early surge, steady middle, end spike
    if total_hours <= 12:
        # Short voting window - morning peak and evening rush
        base_weights = {
            0: 0.02, 1: 0.08, 2: 0.12, 3: 0.10, 4: 0.08, 5: 0.06,
            6: 0.04, 7: 0.03, 8: 0.05, 9: 0.10, 10: 0.15, 11: 0.17
        }
    elif total_hours <= 16:
        # Extended hours - distributed with peaks
        base_weights = {
            0: 0.02, 1: 0.06, 2: 0.10, 3: 0.08, 4: 0.06, 5: 0.05,
            6: 0.04, 7: 0.04, 8: 0.05, 9: 0.06, 10: 0.07, 11: 0.08,
            12: 0.10, 13: 0.08, 14: 0.10, 15: 0.11
        }
    else:
        # 24-hour voting - realistic round-the-clock pattern
        base_weights = {
            0: 0.005, 1: 0.003, 2: 0.002, 3: 0.001, 4: 0.001, 5: 0.002,
            6: 0.008, 7: 0.025, 8: 0.045, 9: 0.055, 10: 0.050, 11: 0.045,
            12: 0.035, 13: 0.030, 14: 0.040, 15: 0.050, 16: 0.060, 17: 0.070,
            18: 0.085, 19: 0.070, 20: 0.045, 21: 0.025, 22: 0.015, 23: 0.008
        }
    
    # Adjust for specified hours
    hourly_weights = {}
    for hour in range(total_hours):
        if hour < len(base_weights):
            hourly_weights[hour] = base_weights[hour]
        else:
            # Extend pattern for longer windows
            hourly_weights[hour] = base_weights[hour % len(base_weights)]
    
    # Apply end concentration
    total_base_weight = sum(hourly_weights.values())
    remaining_weight = 1.0 - (end_weight_percentage / 100.0)
    
    # Scale base weights
    for hour in hourly_weights:
        hourly_weights[hour] = (hourly_weights[hour] / total_base_weight) * remaining_weight
    
    # Concentrate weight in final 2 hours
    final_hours = min(2, total_hours)
    end_weight_per_hour = (end_weight_percentage / 100.0) / final_hours
    
    for i in range(final_hours):
        final_hour = total_hours - 1 - i
        hourly_weights[final_hour] += end_weight_per_hour
    
    return hourly_weights


def create_weighted_hour_choices(hourly_weights):
    """
    Creates weighted list for random hour selection
    
    Args:
        hourly_weights (dict): Hour-to-weight mapping
        
    Returns:
        list: Weighted hour choices for random.choice()
    """
    hours = []
    for hour, weight in hourly_weights.items():
        count = int(weight * 10000)  # Scale for granularity
        hours.extend([hour] * count)
    return hours


def generate_voting_timeframe(election_date, voting_hours):
    """
    Generates voting timeframe based on specified date and duration
    
    Args:
        election_date (datetime): Base election date
        voting_hours (int): Total voting window hours
        
    Returns:
        tuple: (voting_start, voting_end) datetime objects
    """
    # Standard voting starts at 6:00 AM local time
    voting_start = election_date.replace(hour=6, minute=0, second=0, microsecond=0)
    voting_end = voting_start + timedelta(hours=voting_hours)
    
    return voting_start, voting_end


def generate_realistic_timestamp(voting_start, voting_end, weighted_hours, 
                                early_voting_percentage=15):
    """
    Generates realistic voting timestamp within specified window
    
    Args:
        voting_start (datetime): Voting period start
        voting_end (datetime): Voting period end
        weighted_hours (list): Weighted hour choices
        early_voting_percentage (float): Percentage of early voting
        
    Returns:
        datetime: Realistic voting timestamp
    """
    # Determine if early voting or election day
    if random.random() < (early_voting_percentage / 100.0):
        # Early voting: up to 14 days before
        days_before = random.randint(1, 14)
        target_date = (voting_start - timedelta(days=days_before)).date()
        # Early voting has more uniform distribution
        target_hour = random.randint(8, 17)  # Business hours
    else:
        # Election day voting with weighted distribution
        target_date = voting_start.date()
        target_hour = random.choice(weighted_hours)
    
    # Create base timestamp
    base_time = datetime.combine(target_date, datetime.min.time())
    if voting_start.tzinfo:
        base_time = voting_start.tzinfo.localize(base_time)
    
    # Add hour and random minutes/seconds
    target_time = base_time.replace(
        hour=target_hour,
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=random.randint(0, 999999)
    )
    
    # Ensure within bounds for election day
    if target_date == voting_start.date():
        target_time = max(voting_start, min(voting_end, target_time))
    
    return target_time


def add_blockchain_delay(timestamp, min_delay=1, max_delay=30):
    """
    Simulates blockchain processing delay
    
    Args:
        timestamp (datetime): Original voting timestamp
        min_delay (int): Minimum delay in seconds
        max_delay (int): Maximum delay in seconds
        
    Returns:
        datetime: Timestamp with blockchain delay added
    """
    delay_seconds = random.uniform(min_delay, max_delay)
    return timestamp + timedelta(seconds=delay_seconds)


def update_voting_timestamps(election_date_str, voting_hours=12, 
                           end_concentration_pct=30, early_voting_pct=15):
    """
    Main function to update voting timestamps with realistic patterns
    
    Args:
        election_date_str (str): Election date in YYYY-MM-DD format
        voting_hours (int): Total voting window hours
        end_concentration_pct (float): Percentage of votes in final hours
        early_voting_pct (float): Percentage of early voting
    """
    print(f"Starting timestamp update process...")
    print(f"Election Date: {election_date_str}")
    print(f"Voting Window: {voting_hours} hours")
    print(f"End Concentration: {end_concentration_pct}%")
    print(f"Early Voting: {early_voting_pct}%")
    
    # Parse election date
    try:
        election_date = datetime.strptime(election_date_str, "%Y-%m-%d")
        # Use EST timezone for US elections
        est = pytz.timezone('US/Eastern')
        election_date = est.localize(election_date)
    except ValueError:
        raise ValueError("Election date must be in YYYY-MM-DD format")
    
    # Connect to MongoDB
    print("\nConnecting to MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    
    # Get total record count
    total_records = collection.count_documents({})
    print(f"Total records to update: {total_records:,}")
    
    # Generate temporal patterns
    voting_start, voting_end = generate_voting_timeframe(election_date, voting_hours)
    hourly_weights = create_hourly_distribution(voting_hours, end_concentration_pct)
    weighted_hours = create_weighted_hour_choices(hourly_weights)
    
    print(f"Voting Period: {voting_start} to {voting_end}")
    print(f"Hourly Distribution: {len(set(weighted_hours))} unique hours")
    
    # Process documents in batches
    updated_count = 0
    start_time = time.time()
    
    print("\nUpdating timestamps...")
    cursor = collection.find({}, {"_id": 1}).batch_size(BATCH_SIZE)
    
    batch_operations = []
    
    for doc in cursor:
        # Generate realistic timestamp
        realistic_time = generate_realistic_timestamp(
            voting_start, voting_end, weighted_hours, early_voting_pct
        )
        
        # Add blockchain processing delay
        final_timestamp = add_blockchain_delay(realistic_time)
        
        # Convert to UTC for MongoDB storage
        utc_timestamp = final_timestamp.astimezone(pytz.UTC).replace(tzinfo=None)
        
        # Prepare update operation
        update_doc = {
            "_id": doc["_id"],
            "loaded_at": utc_timestamp,
            "voting_hour": realistic_time.hour,
            "voting_date": realistic_time.date().isoformat(),
            "is_early_vote": realistic_time.date() < voting_start.date(),
            "blockchain_delay_seconds": (final_timestamp - realistic_time).total_seconds()
        }
        
        batch_operations.append(update_doc)
        
        # Execute batch when full
        if len(batch_operations) >= BATCH_SIZE:
            execute_batch_update(collection, batch_operations)
            updated_count += len(batch_operations)
            
            # Progress reporting
            elapsed = time.time() - start_time
            rate = updated_count / elapsed if elapsed > 0 else 0
            progress = (updated_count / total_records) * 100
            
            print(f"Progress: {updated_count:,}/{total_records:,} ({progress:.1f}%) "
                  f"- Rate: {rate:,.0f} docs/sec")
            
            batch_operations = []
    
    # Process remaining batch
    if batch_operations:
        execute_batch_update(collection, batch_operations)
        updated_count += len(batch_operations)
    
    # Final statistics
    total_time = time.time() - start_time
    print(f"\nUpdate completed successfully!")
    print(f"Records updated: {updated_count:,}")
    print(f"Total time: {total_time/60:.2f} minutes")
    print(f"Average rate: {updated_count/total_time:,.0f} docs/sec")
    
    # Create temporal indexes
    create_temporal_indexes(collection)
    
    # Generate distribution report
    generate_distribution_report(collection, hourly_weights)
    
    client.close()


def execute_batch_update(collection, batch_operations):
    """
    Executes batch update operations
    
    Args:
        collection: MongoDB collection object
        batch_operations (list): List of update documents
        
    Returns:
        int: Number of successfully updated documents
    """
    operations = []
    for doc in batch_operations:
        doc_id = doc.pop("_id")
        operations.append(
            UpdateOne(
                {"_id": doc_id},
                {"$set": doc}
            )
        )
    
    try:
        result = collection.bulk_write(operations, ordered=False)
        return result.modified_count
    except BulkWriteError as bwe:
        print(f"Bulk write error: {len(bwe.details.get('writeErrors', []))} errors")
        return bwe.details.get('nModified', 0)
    except Exception as e:
        print(f"Batch update error: {e}")
        return 0


def create_temporal_indexes(collection):
    """
    Creates indexes for efficient temporal analysis
    
    Args:
        collection: MongoDB collection object
    """
    print("\nCreating temporal indexes...")
    
    try:
        # Individual field indexes
        collection.create_index("loaded_at")
        collection.create_index("voting_hour")
        collection.create_index("voting_date")
        collection.create_index("is_early_vote")
        
        # Compound indexes for LSTM analysis
        collection.create_index([
            ("voting_date", 1),
            ("voting_hour", 1)
        ])
        
        collection.create_index([
            ("state", 1),
            ("voting_hour", 1)
        ])
        
        print("Temporal indexes created successfully")
        
    except Exception as e:
        print(f"Index creation error: {e}")


def generate_distribution_report(collection, expected_weights):
    """
    Generates distribution analysis report
    
    Args:
        collection: MongoDB collection object
        expected_weights (dict): Expected hourly distribution
    """
    print("\n" + "="*60)
    print("TEMPORAL DISTRIBUTION ANALYSIS")
    print("="*60)
    
    try:
        # Analyze hourly distribution
        pipeline = [
            {"$group": {
                "_id": "$voting_hour",
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        hourly_results = list(collection.aggregate(pipeline))
        total_votes = sum(doc["count"] for doc in hourly_results)
        
        print(f"\nTotal votes analyzed: {total_votes:,}")
        print("\nHourly Distribution:")
        print("Hour | Actual Count | Actual % | Expected % | Variance")
        print("-" * 55)
        
        for doc in hourly_results:
            hour = doc["_id"]
            count = doc["count"]
            actual_pct = (count / total_votes) * 100
            expected_pct = expected_weights.get(hour, 0) * 100
            variance = actual_pct - expected_pct
            
            print(f"{hour:2d}   | {count:11,} | {actual_pct:7.2f}% | "
                  f"{expected_pct:8.2f}% | {variance:+7.2f}%")
        
        # Early voting analysis
        early_vote_count = collection.count_documents({"is_early_vote": True})
        early_vote_pct = (early_vote_count / total_votes) * 100
        
        print(f"\nEarly Voting Analysis:")
        print(f"Early votes: {early_vote_count:,} ({early_vote_pct:.1f}%)")
        print(f"Election day votes: {total_votes - early_vote_count:,} "
              f"({100 - early_vote_pct:.1f}%)")
        
    except Exception as e:
        print(f"Distribution analysis error: {e}")


def main():
    """
    Main execution function with configurable parameters
    """
    # Configuration parameters - modify as needed
    ELECTION_DATE = "2024-11-05"      # Election date (YYYY-MM-DD)
    VOTING_HOURS = 12                 # Total voting window hours
    END_CONCENTRATION = 30            # Percentage of votes in final hours
    EARLY_VOTING_PERCENTAGE = 15     # Percentage of early voting
    
    print("Voting Timestamp Update Tool")
    print("================================")
    print(f"Configuration:")
    print(f"  Election Date: {ELECTION_DATE}")
    print(f"  Voting Hours: {VOTING_HOURS}")
    print(f"  End Concentration: {END_CONCENTRATION}%")
    print(f"  Early Voting: {EARLY_VOTING_PERCENTAGE}%")
    
    try:
        update_voting_timestamps(
            election_date_str=ELECTION_DATE,
            voting_hours=VOTING_HOURS,
            end_concentration_pct=END_CONCENTRATION,
            early_voting_pct=EARLY_VOTING_PERCENTAGE
        )
        
    except Exception as e:
        print(f"Error during timestamp update: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
