# import asyncio
# import os
# import logging
# import pandas as pd
# import clickhouse_connect
# from datetime import datetime, timedelta
#
# if os.name == "nt":  # Windows
#     from dotenv import load_dotenv
#     load_dotenv()
#
# # Configure logging
# logging.basicConfig(
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     level=logging.INFO
# )
# logger = logging.getLogger(__name__)
#
# # Database Connection Function
# def get_clickhouse_client(host, port, database, username, password):
#     try:
#         client = clickhouse_connect.get_client(
#             host=host,
#             port=int(port),
#             username=username,
#             password=password,
#             database=database,
#             secure=False,
#             verify=False  # Set to True if SSL is required
#         )
#         logger.info(f"Connected to ClickHouse database '{database}' at {host}:{port}.")
#         return client
#     except Exception as e:
#         logger.error(f"Failed to connect to ClickHouse database '{database}'. Error: {e}")
#         return None
#
# def prepare_metrics_df(metrics_df):
#     # Define the expected data types
#     dtype_mapping = {
#         'average_sentiment_score': 'float64',
#         'average_sentiment_intensity': 'float64',
#         'total_messages': 'UInt64',
#         'emotion1': 'string',
#         'emotion1_score': 'float64',
#         'emotion2': 'string',
#         'emotion2_score': 'float64',
#         'emotion3': 'string',
#         'emotion3_score': 'float64',
#         'created_at': 'DateTime'
#     }
#
#     # Cast columns to the correct types
#     for column, dtype in dtype_mapping.items():
#         if column in metrics_df.columns:
#             metrics_df[column] = metrics_df[column].astype(dtype)
#         else:
#             # Handle missing columns if necessary
#             logger.warning(f"Column {column} is missing in metrics_df. Filling with default values.")
#             if 'score' in column:
#                 metrics_df[column] = 0.0
#             elif 'emotion' in column:
#                 metrics_df[column] = 'Unknown'
#             elif column == 'created_at':
#                 metrics_df[column] = pd.to_datetime('now')
#             else:
#                 metrics_df[column] = 0
#
#     return metrics_df
#
#
# def insert_metrics(db2_client, table_name, metrics_df):
#     if metrics_df.empty:
#         logger.info("No metrics to insert.")
#         return
#
#     try:
#         # # Reorder columns to match the table schema
#         # ordered_columns = [
#         #     'average_sentiment_score',
#         #     'average_sentiment_intensity',
#         #     'total_messages',
#         #     'emotion1',
#         #     'emotion1_score',
#         #     'emotion2',
#         #     'emotion2_score',
#         #     'emotion3',
#         #     'emotion3_score',
#         #     'created_at'  # Ensure created_at is last
#         # ]
#         # metrics_df = metrics_df[ordered_columns]
#
#         # Convert DataFrame to list of dictionaries
#         # records = metrics_df.to_dict('records')
#         #
#         # print(metrics_df)
#         #
#         # column_names = metrics_df.columns.tolist()
#         #
#         # logger.debug(f"Reordered records: {records}")
#         # logger.debug(f"Column names: {column_names}")
#
#         # Insert records into the target table
#         # db2_client.insert(table_name, records, column_names=column_names)
#         db2_client.insert_df(table_name, metrics_df)
#         logger.info(f"Inserted metrics into {table_name} successfully.")
#     except Exception as e:
#         logger.error(f"Error inserting metrics into {table_name}: {e}")
#
#
# # Combined Processing Function
# def process_sentiment_and_calculate_metrics():
#     logger.info("Starting sentiment processing and metrics calculation.")
#
#     # Connection details for Database 1 (Sentiment Data)
#     db1_host = os.getenv("SLURP_HOST")
#     db1_port = os.getenv("SLURP_PORT")
#     db1_database = os.getenv("SLURP_DATABASE")
#     db1_username = os.getenv("SLURP_USERNAME")
#     db1_password = os.getenv("SLURP_PASSWORD")
#
#     # Connection details for Database 2 (User IDs and Metrics)
#     db2_host = os.getenv("SPELL_DS_HOST")
#     db2_port = os.getenv("SPELL_DS_PORT")
#     db2_database = os.getenv("SPELL_DS_DATABASE")
#     db2_username = os.getenv("SPELL_DS_USERNAME")
#     db2_password = os.getenv("SPELL_DS_PASSWORD")
#
#     metrics_table = "moderator_sentiment"
#
#     # Connect to Database 2 to fetch user IDs
#     db2_client = get_clickhouse_client(db2_host, db2_port, db2_database, db2_username, db2_password)
#     if not db2_client:
#         logger.error("Cannot proceed without Database 2 connection.")
#         return
#
#     # Connect to Database 1 to fetch sentiment data
#     db1_client = get_clickhouse_client(db1_host, db1_port, db1_database, db1_username, db1_password)
#     if not db1_client:
#         logger.error("Cannot proceed without Database 1 connection.")
#         return
#
#     data = {
#         "average_sentiment_score": [0.85],
#         "average_sentiment_intensity": [0.72],
#         "total_messages": [150],
#         "emotion1": ["happy"],
#         "emotion1_score": [0.6],
#         "emotion2": ["neutral"],
#         "emotion2_score": [0.3],
#         "emotion3": ["sad"],
#         "emotion3_score": [0.1],
#         "created_at": ["2024-12-22 15:20:00"]
#     }
#
#     metrics_df = pd.DataFrame(data)
#     metrics_df['created_at'] = pd.to_datetime(metrics_df['created_at'])
#
#     # query = 'SELECT * FROM moderator_sentiment LIMIT 10'
#     # result = db2_client.query(query)
#     # df = pd.DataFrame(result.result_rows, columns=result.column_names)
#     # print(df)
#
#     # Insert metrics into Database 2, Table 2
#     insert_metrics(db2_client, metrics_table, metrics_df)
#
#     logger.info("Sentiment processing and metrics calculation completed successfully.")
#
# if __name__ == '__main__':
#     process_sentiment_and_calculate_metrics()