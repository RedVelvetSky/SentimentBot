import asyncio
import os
import logging
import pandas as pd
import clickhouse_connect
from datetime import datetime, timedelta

if os.name == "nt":  # Windows
    from dotenv import load_dotenv
    load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Database Connection Function
def get_clickhouse_client(host, port, database, username, password):
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=int(port),
            username=username,
            password=password,
            database=database,
            secure=False,
            verify=False  # Set to True if SSL is required
        )
        logger.info(f"Connected to ClickHouse database '{database}' at {host}:{port}.")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse database '{database}'. Error: {e}")
        return None

# Fetch User IDs from Database 2, Table 1
def fetch_user_ids(db2_client, table_name):
    query = f"SELECT user_id FROM {table_name}"
    try:
        result = db2_client.query(query)
        data = result.result_rows
        user_ids = [row[0] for row in data]
        logger.info(f"Fetched {len(user_ids)} user IDs from {table_name}.")
        return user_ids
    except Exception as e:
        logger.error(f"Error fetching user IDs from {table_name}: {e}")
        return []

# Fetch Sentiment Data from Database 1, Table 1
def fetch_sentiment_data(db1_client, table_name):
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=1)
    start_time_formatted = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_formatted = end_time.strftime("%Y-%m-%d %H:%M:%S")

    query = f"SELECT sender_id, sentiment_score, sentiment_intensity, emotion FROM {table_name} WHERE created_at >= '{start_time_formatted}' AND created_at < '{end_time_formatted}'"
    try:
        result = db1_client.query(query)
        data = result.result_rows
        columns = result.column_names
        sentiment_df = pd.DataFrame(data, columns=columns)
        logger.info(f"Fetched {len(sentiment_df)} records from {table_name}.")
        return sentiment_df
    except Exception as e:
        logger.error(f"Error fetching sentiment data from {table_name}: {e}")
        return pd.DataFrame()

# Calculate Metrics
def calculate_metrics(sentiment_df, user_ids):
    # Filter data to exclude specified user IDs
    filtered_data = sentiment_df[~sentiment_df['sender_id'].isin(user_ids)]

    if filtered_data.empty:
        logger.warning("No data available after filtering with the provided user IDs.")
        return pd.DataFrame()

    # Calculate average sentiment score and intensity
    average_sentiment_score = round(filtered_data['sentiment_score'].mean(), 2)
    average_sentiment_intensity = round(filtered_data['sentiment_intensity'].mean(), 2)
    total_messages = len(filtered_data)

    # Calculate emotion distribution
    emotion_distribution = filtered_data['emotion'].value_counts(normalize=True).round(4)

    # Select top three emotions
    top_emotions = emotion_distribution.head(3)

    # Prepare the metrics dictionary
    metrics = {
        'average_sentiment_score': average_sentiment_score,
        'average_sentiment_intensity': average_sentiment_intensity,
        'total_messages': total_messages,
        'created_at': datetime.utcnow()  # Add the current UTC timestamp
    }

    # Add top three emotions to the metrics
    for i in range(1, 4):
        if i <= len(top_emotions):
            emotion = top_emotions.index[i-1]
            score = top_emotions.iloc[i-1]
            metrics[f'emotion{i}'] = emotion
            metrics[f'emotion{i}_score'] = score
        else:
            # If fewer than three emotions, fill with None
            metrics[f'emotion{i}'] = None
            metrics[f'emotion{i}_score'] = None

    return pd.DataFrame([metrics])

# Insert Metrics into Database 2, Table 2
import traceback

def prepare_metrics_df(metrics_df):
    # Define the expected data types
    dtype_mapping = {
        'average_sentiment_score': 'float64',
        'average_sentiment_intensity': 'float64',
        'total_messages': 'UInt64',
        'emotion1': 'string',
        'emotion1_score': 'float64',
        'emotion2': 'string',
        'emotion2_score': 'float64',
        'emotion3': 'string',
        'emotion3_score': 'float64',
        'created_at': 'datetime64'
    }

    # Cast columns to the correct types
    for column, dtype in dtype_mapping.items():
        if column in metrics_df.columns:
            metrics_df[column] = metrics_df[column].astype(dtype)
        else:
            # Handle missing columns if necessary
            logger.warning(f"Column {column} is missing in metrics_df. Filling with default values.")
            if 'score' in column:
                metrics_df[column] = 0.0
            elif 'emotion' in column:
                metrics_df[column] = 'Unknown'
            elif column == 'created_at':
                metrics_df[column] = pd.to_datetime('now')
            else:
                metrics_df[column] = 0

    return metrics_df


def insert_metrics(db2_client, table_name, metrics_df):
    if metrics_df.empty:
        logger.info("No metrics to insert.")
        return

    try:
        db2_client.insert_df(table_name, metrics_df)
        logger.info(f"Inserted metrics into {table_name} successfully.")
    except Exception as e:
        logger.error(f"Error inserting metrics into {table_name}: {e}")
        logger.error(traceback.format_exc())

# Combined Processing Function
def process_sentiment_and_calculate_metrics():
    logger.info("Starting sentiment processing and metrics calculation.")

    # Connection details for Database 1 (Sentiment Data)
    db1_host = os.getenv("SLURP_HOST")
    db1_port = os.getenv("SLURP_PORT")
    db1_database = os.getenv("SLURP_DATABASE")
    db1_username = os.getenv("SLURP_USERNAME")
    db1_password = os.getenv("SLURP_PASSWORD")

    # Connection details for Database 2 (User IDs and Metrics)
    db2_host = os.getenv("SPELL_DS_HOST")
    db2_port = os.getenv("SPELL_DS_PORT")
    db2_database = os.getenv("SPELL_DS_DATABASE")
    db2_username = os.getenv("SPELL_DS_USERNAME")
    db2_password = os.getenv("SPELL_DS_PASSWORD")

    # Table names
    user_ids_table = "moderator_metrics"
    sentiment_table = "sentiment_analysis"
    metrics_table = "moderator_sentiment"

    # Connect to Database 2 to fetch user IDs
    db2_client = get_clickhouse_client(db2_host, db2_port, db2_database, db2_username, db2_password)
    if not db2_client:
        logger.error("Cannot proceed without Database 2 connection.")
        return

    user_ids = fetch_user_ids(db2_client, user_ids_table)
    if not user_ids:
        logger.warning("No user IDs fetched. Proceeding with empty user ID list.")

    # Connect to Database 1 to fetch sentiment data
    db1_client = get_clickhouse_client(db1_host, db1_port, db1_database, db1_username, db1_password)
    if not db1_client:
        logger.error("Cannot proceed without Database 1 connection.")
        return

    sentiment_df = fetch_sentiment_data(db1_client, sentiment_table)
    if sentiment_df.empty:
        logger.warning("No sentiment data fetched. Exiting.")
        return

    # Calculate metrics
    metrics_df = calculate_metrics(sentiment_df, user_ids)
    if metrics_df.empty:
        logger.warning("No metrics calculated. Exiting.")
        return

    # Insert metrics into Database 2, Table 2
    insert_metrics(db2_client, metrics_table, metrics_df)

    logger.info("Sentiment processing and metrics calculation completed successfully.")

async def schedule_daily_sentiment_task():
    loop = asyncio.get_running_loop()
    while True:
        logger.info("Starting daily sentiment processing and metrics calculation.")
        # Run the processing in a separate thread to avoid blocking
        await loop.run_in_executor(None, process_sentiment_and_calculate_metrics)
        logger.info("Metrics calculation task completed. Sleeping for 24 hours.")
        await asyncio.sleep((24 * 60 * 60) + 60)  # sleep for 24 hours