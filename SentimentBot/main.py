import json
import logging
import os
import asyncio
from datetime import datetime, timedelta

import clickhouse_connect
import pandas as pd
from openai import OpenAI, OpenAIError
from telegram import Update
from telegram.error import TelegramError
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

if os.name == "nt":  # Windows
    from dotenv import load_dotenv
    load_dotenv()

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants from Environment Variables
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", 120))  # Default to 2 minutes
CHAT_IDS = [-1002240327148, -1002167264676]
EXCLUDED_SENDERS_ID = [609517172, 6441628071, 839584406, 607662689, 696267355, 210944655]
SENTIMENT_THRESHOLD_BASE = float(os.getenv("SENTIMENT_THRESHOLD", "-0.5"))
SENTIMENT_THRESHOLD_URGENT = float(os.getenv("SENTIMENT_THRESHOLD_URGENT", "-0.8"))

# Telegram Configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
# TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_CHAT_ID = -1002455147403


class SentimentAnalyzer:
    def __init__(self, bot_context: ContextTypes.DEFAULT_TYPE):
        self.bot_context = bot_context
        # Initialize OpenAI Client
        self.clientai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        # Initialize ClickHouse Client
        self.client = clickhouse_connect.get_client(
            host=os.getenv('SLURP_HOST'),
            port=int(os.getenv('SLURP_PORT')),
            database=os.getenv('SLURP_DATABASE'),
            username=os.getenv('SLURP_USERNAME'),
            password=os.getenv('SLURP_PASSWORD'),
            connect_timeout=30,
            secure=False,
            server_host_name=os.getenv('SLURP_HOST'),
            settings={'session_timeout': 300}
        )

        # Initialize Timestamps
        self.latest_created_at = self.get_latest_created_at() - timedelta(minutes=5)
        self.last_daily_notification_date = datetime.utcnow().date()

    def get_latest_created_at(self):
        query = "SELECT MAX(created_at) AS latest_created_at FROM messages"
        try:
            result = self.client.query(query)
            if result.result_rows and result.result_rows[0][0]:
                latest = result.result_rows[0][0]
                logger.info(f"Latest created_at retrieved: {latest}")
                return latest
            else:
                logger.info("No existing messages found in the database.")
                return datetime.utcnow() - timedelta(hours=1)
        except Exception as e:
            logger.error(f"Error fetching latest_created_at: {e}")
            return datetime.utcnow() - timedelta(hours=1)

    @staticmethod
    def convert_timestamp_to_string(messages):
        for message in messages:
            created_at = message.get('created_at')
            if isinstance(created_at, (pd.Timestamp, datetime)):
                message['created_at'] = created_at.isoformat()
        return messages

    def get_batch_sentiment(self, messages):
        try:
            messages = self.convert_timestamp_to_string(messages)
            batch_text = "\n\n".join(
                [f"Message ID {msg['message_id']} in Chat ID {msg['chat_id']} with Sender ID {msg['sender_id']}: {msg['message']}" for msg in messages]
            )
            logger.info(f"Batch text for API: {batch_text[:500]}...")

            function_schema = {
                "name": "analyzeSentiment",
                "description": "Analyze messages for sentiment and provide structured data, including intensity, emotion, and polarity.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "evaluations": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "message_id": {"type": "number"},
                                    "chat_id": {"type": "number"},
                                    "sender_id": {"type": "number"},
                                    "sentiment": {"type": "string"},
                                    "sentiment_intensity": {"type": "number"},
                                    "emotion": {
                                        "type": "string",
                                        "enum": [
                                            "Curiosity", "Excitement", "Frustration", "Joy", "Trust", "Anticipation",
                                            "Disappointment", "Neutral", "Confusion", "Anger", "Satisfaction", "Enthusiasm",
                                            "Sadness", "Hopefulness", "Gratefulness", "Skepticism", "Anxiety", "Relief",
                                            "Surprise", "Indifference"
                                        ]
                                    },
                                    "subjectivity": {"type": "number"},
                                    "sentiment_score": {"type": "number"},
                                    "sentiment_reason": {"type": "string"},
                                    "emotion_confidence": {"type": "number"}
                                },
                                "required": [
                                    "message_id", "chat_id", "sender_id", "sentiment",
                                    "sentiment_intensity", "emotion",
                                    "subjectivity", "sentiment_score",
                                    "sentiment_reason", "emotion_confidence"
                                ]
                            }
                        }
                    },
                    "required": ["evaluations"]
                }
            }

            logger.info("Waiting for OpenAI to process sentiment analysis...")
            completion = self.clientai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a sentiment analysis model for Spell - Crypto Airdrop Wallet messages. "
                            "Classify each message as positive, negative, or neutral with scores:\n"
                            "• **0.8-1.0**: Very Positive (e.g., 'Fucking Love it, amazing!!!')\n"
                            "• **0.3-0.7**: Positive\n"
                            "• **0.1-0.2**: Slightly Positive\n"
                            "• **0**: Neutral\n"
                            "• **-0.1 to -0.2**: Slightly Negative\n"
                            "• **-0.3 to -0.6**: Negative\n"
                            "• **-0.7 to -1.0**: (e.g., 'You scummed us 😡😡😡 where our money') Very Negative\n\n"
                            "**Guidelines:**\n"
                            "- Use scores ≥0.8 or ≤-0.8 for strong sentiments to flag for moderation.\n"
                            "- Emoji-only: use **0.2-0.4** for positive emojis unless paired with enthusiastic text.\n"
                            "- messages where only /daily_puzzle has sentiment of 0.05"
                            "- High emoji scores (≥0.7) require clear enthusiastic language.\n\n"
                            "**Examples:**\n"
                            "- 'Love it!!!' → 0.8, Enthusiasm\n"
                            "- '🚀' → 0.2, Excitement\n"
                            "- 'I hate this.' → -0.8, Anger\n"
                            "- 'Hello!' → 0, Neutral\n\n"
                            "Return the analysis as JSON."
                        )
                    },
                    {"role": "user", "content": batch_text}
                ],
                functions=[function_schema],
                function_call={"name": "analyzeSentiment"}
            )

            function_call = completion.choices[0].message.function_call
            function_arguments = function_call.arguments
            evaluation_data = json.loads(function_arguments)

            logger.info(f"API response: {json.dumps(evaluation_data, indent=2)}")

            if isinstance(evaluation_data.get('evaluations'), list):
                return evaluation_data['evaluations']
            else:
                logger.error("Unexpected data format in API response.")
                return None

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            return None
        except OpenAIError as e:
            logger.error(f"OpenAI API error: {e}")
            return None
        except Exception as e:
            logger.error(f"An error occurred during sentiment analysis: {e}")
            return None

    def fetch_data_by_id(self, message_id, chat_id):
        query = f"""
            SELECT message, sender_id FROM messages 
            WHERE message_id = {message_id} AND chat_id = {chat_id}
            AND created_at > '2024-10-19'
            LIMIT 1
        """
        try:
            result = self.client.query(query)
            if result.result_rows:
                return result.result_rows[0]
            else:
                logger.warning(f"Message not found for ID: {message_id}, Chat ID: {chat_id}")
                return None
        except Exception as e:
            logger.error(f"Error fetching message by ID: {e}")
            return None

    async def send_to_external_api(self, messages_df):
        loop = asyncio.get_event_loop()
        messages = messages_df.to_dict(orient='records')
        sentiment_results = await loop.run_in_executor(None, self.get_batch_sentiment, messages)
        return sentiment_results

    def fetch_messages_after_timestamp(self, latest_created_at, chat_ids, excluded_sender_ids):
        chat_ids_str = ', '.join(map(str, chat_ids))
        excluded_sender_ids_str = ', '.join(map(str, excluded_sender_ids))

        query = f"""
            SELECT * FROM messages 
            WHERE created_at > '{latest_created_at}'
            AND chat_id IN ({chat_ids_str})
            AND sender_id NOT IN ({excluded_sender_ids_str})
            ORDER BY created_at
        """
        try:
            result = self.client.query(query)
            data = result.result_rows
            columns = result.column_names
            df = pd.DataFrame(data, columns=columns)
            logger.info(
                f"Fetched {len(df)} new messages for chat_ids {chat_ids}, excluding sender_ids {excluded_sender_ids}.")
            return df
        except Exception as e:
            logger.error(f"Error fetching messages: {e}")
            return pd.DataFrame()

    async def send_telegram_notification(self, message, notification=True):
        try:
            await self.bot_context.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='Markdown',
                                                    disable_notification=notification)
            logger.info("Telegram notification sent successfully.")
        except TelegramError as e:
            logger.error(f"Error sending Telegram notification: {e}")

    def generate_message_link(self, chat_id, message_id):
        if chat_id < 0:
            chat_id_str = str(chat_id)[4:]  # Remove '-100' prefix
            link = f"https://t.me/c/{chat_id_str}/{message_id}"
        else:
            link = f"https://t.me/c/{chat_id}/{message_id}"  # Placeholder
        return link

    def is_duplicate(self, message_id, sender_id):
        query = """
            SELECT COUNT(*) AS count
            FROM sentiment_analysis
            WHERE message_id = %s AND sender_id = %s
            HAVING count > 1
        """
        try:
            logger.info(f"Checking duplicate for message_id: {message_id}, sender_id: {sender_id}")
            result = self.client.query(query, (message_id, sender_id))
            logger.info(f"Result rows: {result.result_rows}")
            exists = bool(result.result_rows)
            logger.info(f"Duplicate found: {exists}")
            return exists
        except Exception as e:
            logger.error(f"Error checking for duplicate: {e}")
            return False

    def write_to_clickhouse(self, response_data):
        insert_query = """
            INSERT INTO sentiment_analysis (
                message_id, chat_id, sender_id, message, sentiment, sentiment_intensity,
                emotion, subjectivity, sentiment_score, sentiment_reason, emotion_confidence, created_at
            ) VALUES
        """
        try:
            values = []
            for result in response_data:
                message_id = result.get('message_id')
                chat_id = result.get('chat_id')
                sender_id = result.get('sender_id')

                if self.is_duplicate(message_id, sender_id):
                    logger.info(f"Skipping duplicate message {message_id} in chat {sender_id}")
                    continue

                data = self.fetch_data_by_id(message_id, chat_id)
                if data:
                    original_message, sender_id = data
                else:
                    original_message = "N/A"
                    sender_id = "0"

                created_at = datetime.utcnow().replace(microsecond=0).isoformat()

                values.append((
                    message_id,
                    chat_id,
                    sender_id,
                    original_message,
                    result.get('sentiment'),
                    result.get('sentiment_intensity', 'unknown'),
                    result.get('emotion'),
                    result.get('subjectivity'),
                    result.get('sentiment_score', 0),
                    result.get('sentiment_reason', 'No reason provided'),
                    result.get('emotion_confidence', 0),
                    created_at
                ))

            if values:
                values_str = ", ".join([f"({', '.join(['%s'] * len(v))})" for v in values])
                flat_values = [item for v in values for item in v]
                full_query = insert_query + values_str
                self.client.command(full_query, flat_values)
                logger.info(f"Inserted {len(values)} records into ClickHouse.")
            else:
                logger.info("No new data to insert into ClickHouse.")

        except Exception as e:
            logger.error(f"Error inserting data into ClickHouse: {e}")

    async def log_sentiment(self, sentiment_results):
        for result in sentiment_results:
            message_id = result.get('message_id')
            chat_id = result.get('chat_id')
            sentiment = result.get('sentiment')
            sentiment_intensity = result.get('sentiment_intensity', 'unknown')
            emotion = result.get('emotion')
            subjectivity = result.get('subjectivity')
            sentiment_score = result.get('sentiment_score', 0)
            sentiment_reason = result.get('sentiment_reason', 'No reason provided')
            emotion_confidence = result.get('emotion_confidence', 0)

            data = self.fetch_data_by_id(message_id, chat_id)
            if data:
                original_message, sender_id = data
            else:
                original_message = "N/A"
                sender_id = "N/A"

            log_message = (
                f"Message ID: {message_id} | Chat ID: {chat_id}\n"
                f"Original Message: {original_message}\n"
                f"Sentiment: {sentiment}\n"
                f"Sentiment Score: {sentiment_score}\n"
                f"Sentiment Reason: {sentiment_reason}\n"
                f"Sentiment Intensity: {sentiment_intensity}\n"
                f"Emotion: {emotion}\n"
                f"Emotion Confidence: {emotion_confidence}\n"
                f"Subjectivity: {subjectivity}\n"
                "-----"
            )
            logger.info(log_message)

            if sentiment_score <= SENTIMENT_THRESHOLD_URGENT:
                logger.info("Urgent alert")
                message_link = self.generate_message_link(chat_id, message_id)
                notification_message = (
                    f"🔴 *High Priority Sentiment Alert* 🔴\n"
                    f"*Message ID:* {message_id}\n"
                    f"*Chat ID:* {chat_id}\n"
                    f"*Original Message:* {original_message}\n"
                    f"*Sentiment:* {sentiment}\n"
                    f"*Sentiment Score:* {sentiment_score}\n"
                    f"*Sentiment Reason:* {sentiment_reason}\n"
                    f"*Intensity:* {sentiment_intensity}\n"
                    f"*Emotion:* {emotion}\n"
                    f"*Confidence:* {emotion_confidence}\n"
                    f"[View Message]({message_link})"
                )
                await self.send_telegram_notification(notification_message, notification=False)
                logger.info(f"🆘 Very Low Sentiment detected for message {message_id}. Sent notification.")
            elif sentiment_score <= SENTIMENT_THRESHOLD_BASE:
                logger.info("Urgent alert")
                message_link = self.generate_message_link(chat_id, message_id)
                notification_message = (
                    f"🟡 *Mid Priority Sentiment Alert* 🟡\n"
                    f"*Message ID:* {message_id}\n"
                    f"*Chat ID:* {chat_id}\n"
                    f"*Original Message:* {original_message}\n"
                    f"*Sentiment:* {sentiment}\n"
                    f"*Sentiment Score:* {sentiment_score}\n"
                    f"*Sentiment Reason:* {sentiment_reason}\n"
                    f"*Intensity:* {sentiment_intensity}\n"
                    f"*Emotion:* {emotion}\n"
                    f"*Confidence:* {emotion_confidence}\n"
                    f"[View Message]({message_link})"
                )
                await self.send_telegram_notification(notification_message, notification=True)
                logger.info(f"⚠️ Low Sentiment detected for message {message_id}. Sent notification.")

    async def run(self):
        logger.info("Sentiment Analyzer started.")
        try:
            while True:
                current_time = datetime.utcnow()
                if self.latest_created_at:
                    loop = asyncio.get_event_loop()
                    df = await loop.run_in_executor(
                        None, self.fetch_messages_after_timestamp, self.latest_created_at, CHAT_IDS, EXCLUDED_SENDERS_ID)
                else:
                    loop = asyncio.get_event_loop()
                    df = await loop.run_in_executor(
                        None, self.fetch_messages_after_timestamp,
                        (current_time - timedelta(minutes=5)).isoformat(), CHAT_IDS, EXCLUDED_SENDERS_ID)

                if not df.empty:
                    logger.info("df is not empty in main loop")
                    sentiment_results = await self.send_to_external_api(df)
                    if sentiment_results:
                        await self.log_sentiment(sentiment_results)
                        # Uncomment the next line if you wish to log sentiments to ClickHouse
                        self.write_to_clickhouse(sentiment_results)
                        self.latest_created_at = pd.to_datetime(df['created_at']).max()
                        logger.info(f"Updated latest_created_at: {self.latest_created_at}")
                else:
                    logger.info("No new messages found.")

                await asyncio.sleep(CHECK_INTERVAL_SECONDS)

        except asyncio.CancelledError:
            logger.info("Sentiment Analyzer task cancelled.")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")


# Define the /start command handler
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Hello! Starting sentiment analysis.")
    # Start the sentiment analysis task
    analyzer = SentimentAnalyzer(context)
    context.application.create_task(analyzer.run())
    await context.bot.send_message(chat_id=update.effective_chat.id,
                                   text="Sentiment analysis is now running in the background.")


# Main application setup
if __name__ == '__main__':
    # Initialize the Telegram application
    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # Add a start handler to respond to /start command
    start_handler = CommandHandler('start', start)
    application.add_handler(start_handler)

    # Run the bot until manually stopped
    logger.info("Bot is polling...")
    application.run_polling()
