import os
import asyncio
import logging
from datetime import datetime, timedelta

import pandas as pd
import clickhouse_connect

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
def get_clickhouse_client(host, port, database, username, password, server_host_name):
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=int(port),
            database=database,
            username=username,
            password=password,
            connect_timeout=30,
            secure=False,
            server_host_name=server_host_name,
            settings={'session_timeout': 300}
        )
        logger.info(f"Connected to ClickHouse database '{os.getenv('CLICKHOUSE_DATABASE')}' at {os.getenv('CLICKHOUSE_HOST')}:{os.getenv('CLICKHOUSE_PORT')}.")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse database. Error: {e}")
        return None

# Data Fetching Functions
def fetch_messages(client, start_time, end_time):
    # Format timestamps to exclude microseconds
    start_time_formatted = start_time.split(".")[0]
    end_time_formatted = end_time.split(".")[0]

    query = f"""
        SELECT *
        FROM messages
        WHERE created_at >= '{start_time_formatted}' AND created_at < '{end_time_formatted}'
    """
    try:
        result = client.query(query)
        data = result.result_rows
        columns = result.column_names
        df = pd.DataFrame(data, columns=columns)
        logger.info(f"Fetched {len(df)} messages from the last day.")
        return df
    except Exception as e:
        logger.error(f"Error fetching messages: {e}")
        return pd.DataFrame()

def fetch_user_lookup(client):
    query = "SELECT user_id, username FROM moderator_lookup"
    try:
        result = client.query(query)
        data = result.result_rows
        columns = result.column_names
        df = pd.DataFrame(data, columns=columns)
        logger.info(f"Fetched {len(df)} records from user_lookup table.")
        return df
    except Exception as e:
        logger.error(f"Error fetching user lookup data: {e}")
        return pd.DataFrame()


# Helper Function to Get Username
def get_username(user_lookup_df, user_id):
    username_series = user_lookup_df.loc[user_lookup_df['user_id'] == user_id, 'username']
    if not username_series.empty:
        return username_series.values[0]
    return "Unknown"

# Data Processing Function
def calculate_metrics(messages_df, user_lookup_df, user_ids, session_threshold):
    logger.info("Starting metrics calculation.")
    metrics = []

    # Precompute a mapping from message_id to sender_id for quick lookups
    message_id_to_sender = messages_df.set_index('message_id')['sender_id'].to_dict()

    for user_id in user_ids:
        logger.info(f"Processing user: {user_id}")
        user_replies = messages_df[
            (messages_df['sender_id'] == user_id) &
            (messages_df['reply_to_message_id'].notnull())
        ]

        paired_messages = user_replies.merge(
            messages_df,
            left_on='reply_to_message_id',
            right_on='message_id',
            suffixes=('_reply', '_original')
        )

        if paired_messages.empty:
            logger.info(f"No replies found for user {user_id}.")
            metrics.append({
                'user_id': user_id,
                'username': get_username(user_lookup_df, user_id),
                'total_sessions': 0,
                'max_session_length': 0,
                'min_session_length': 0,
                'avg_session_length': 0.0,
                'total_message_pairs': 0,
                'communicated_user_ids': [],
                'created_at': datetime.utcnow().date()
            })
            continue

        # Parse datetime columns
        paired_messages['message_created_at_original'] = pd.to_datetime(
            paired_messages['message_created_at_original'], errors='coerce'
        )
        paired_messages['message_created_at_reply'] = pd.to_datetime(
            paired_messages['message_created_at_reply'], errors='coerce'
        )

        # Combine original messages and replies
        interactions = pd.concat([
            paired_messages.rename(columns={
                'sender_id_original': 'sender_id',
                'message_original': 'message',
                'message_created_at_original': 'message_time'
            })[['sender_id', 'message', 'message_time']],
            paired_messages.rename(columns={
                'sender_id_reply': 'sender_id',
                'message_reply': 'message',
                'message_created_at_reply': 'message_time'
            })[['sender_id', 'message', 'message_time']]
        ], ignore_index=True)

        interactions.sort_values(by=['sender_id', 'message_time'], inplace=True)

        # Calculate session breaks
        interactions['time_diff'] = interactions.groupby('sender_id')['message_time'].diff()
        interactions['new_session'] = interactions['time_diff'] > session_threshold
        interactions['session_id'] = interactions.groupby('sender_id')['new_session'].cumsum()

        # Aggregate session data
        sessions = interactions.groupby(['sender_id', 'session_id']).agg(
            total_messages=('message', 'count')
        ).reset_index()

        sessions['message_pairs'] = (sessions['total_messages'] / 2).apply(lambda x: max(1, int(x)))

        total_pairs = sessions['message_pairs'].sum()

        # Collect communicated_user_ids
        # 1. Users who replied to the moderator's messages
        replied_user_ids = paired_messages['sender_id_original'].unique().tolist()

        # 2. Users whose messages were replied to by the moderator
        # Find messages sent by the moderator
        moderator_messages = messages_df[messages_df['sender_id'] == user_id]
        # Find messages that are replies to moderator's messages
        replies_to_moderator = messages_df[messages_df['reply_to_message_id'].isin(moderator_messages['message_id'])]
        replied_to_user_ids = replies_to_moderator['sender_id'].unique().tolist()

        # Combine and deduplicate
        communicated_user_ids = list(set(replied_user_ids + replied_to_user_ids))
        # Remove the moderator's own user_id if present
        communicated_user_ids = [uid for uid in communicated_user_ids if uid != user_id]

        metrics.append({
            'user_id': user_id,
            'username': get_username(user_lookup_df, user_id),
            'total_sessions': sessions['session_id'].nunique(),
            'max_session_length': sessions['message_pairs'].max(),
            'min_session_length': sessions['message_pairs'].min(),
            'avg_session_length': round(sessions['message_pairs'].mean(), 4),
            'total_message_pairs': total_pairs,
            'communicated_user_ids': communicated_user_ids,
            'created_at': datetime.utcnow().date()
        })

        logger.info(f"Metrics for user {user_id}: {metrics[-1]}")

    metrics_df = pd.DataFrame(metrics)
    logger.info("Completed metrics calculation.")
    return metrics_df

from datetime import date

def insert_metrics(client, metrics_df):
    if metrics_df.empty:
        logger.info("No metrics to insert.")
        return

    try:
        # Convert DataFrame to a list of tuples with type normalization
        records = [
            (
                int(record['user_id']),
                int(record['total_sessions']),
                int(record['max_session_length']),
                int(record['min_session_length']),
                float(record['avg_session_length']),
                int(record['total_message_pairs']),
                record['username'],
                list(map(int, record['communicated_user_ids'])),  # Ensure list of ints
                datetime.combine(record['created_at'], datetime.min.time()) if isinstance(record['created_at'], date) else datetime.strptime(record['created_at'], '%Y-%m-%d')
            )
            for record in metrics_df.to_dict('records')
        ]

        # Insert data into the database
        client.insert(
            'moderator_metrics',
            records,
            column_names=[
                'user_id',
                'total_sessions',
                'max_session_length',
                'min_session_length',
                'avg_session_length',
                'total_message_pairs',
                'username',
                'communicated_user_ids',
                'created_at'
            ]
        )
        logger.info(f"Inserted {len(records)} records into moderator_metrics table.")
    except Exception as e:
        logger.error(f"Error inserting metrics into moderator_metrics table: {e}")


# Main Processing Function
async def run_metrics_calculation():
    logger.info("Starting daily metrics calculation.")

    # Define session threshold
    session_threshold = timedelta(minutes=15)

    # Calculate the time range for the last day
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=1)

    # Connect to ClickHouse
    client_spell_tg = get_clickhouse_client(os.getenv("SLURP_HOST"),
                                            os.getenv("SLURP_PORT"),
                                            os.getenv("SLURP_DATABASE"),
                                            os.getenv("SLURP_USERNAME"),
                                            os.getenv("SLURP_PASSWORD"),
                                            os.getenv("SLURP_HOST"))

    if not client_spell_tg:
        logger.error("ClickHouse client not available. Exiting.")
        return

    # Fetch data
    messages_df = fetch_messages(client_spell_tg, start_time.isoformat(), end_time.isoformat())
    if messages_df.empty:
        logger.warning("No messages fetched for the last day.")
        return

    # Connect to ClickHouse
    client_spell_ds = get_clickhouse_client(os.getenv("SPELL_DS_HOST"),
                                            os.getenv("SPELL_DS_PORT"),
                                            os.getenv("SPELL_DS_DATABASE"),
                                            os.getenv("SPELL_DS_USERNAME"),
                                            os.getenv("SPELL_DS_PASSWORD"),
                                            os.getenv("SPELL_DS_HOST"))

    user_lookup_df = fetch_user_lookup(client_spell_ds)
    if user_lookup_df.empty:
        logger.warning("User lookup data is empty.")
        return

    user_ids = user_lookup_df['user_id'].tolist()
    logger.info(f"Analyzing metrics for user IDs: {user_ids}")

    # Calculate metrics
    metrics_df = calculate_metrics(messages_df, user_lookup_df, user_ids, session_threshold)

    # Insert metrics into Target Database
    insert_metrics(client_spell_ds, metrics_df)

    logger.info("Daily metrics calculation completed.")

# Scheduler Function
async def schedule_daily_task():
    while True:
        await run_metrics_calculation()
        logger.info("Metrics calculation task completed. Sleeping for 24 hours.")
        await asyncio.sleep(24 * 60 * 60)  # sleep for 24 hours