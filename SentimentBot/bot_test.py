import logging
import random
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# Configure logging for debugging purposes
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


# Define the /start command handler
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Hello! Starting experimenting.")
    # Start the random message task
    asyncio.create_task(random_message_sender(context))


# Random message sender function
async def random_message_sender(context: ContextTypes.DEFAULT_TYPE):
    chat_id = -1002303184948
    while True:
        await asyncio.sleep(5)  # Wait 10 seconds
        random_number = random.randint(3, 6)
        print(random_number)
        if random_number == 5:
            await context.bot.send_message(chat_id=chat_id, text="Here's a random message based on chance!")


# Main application setup
if __name__ == '__main__':
    application = ApplicationBuilder().token("7839045208:AAFmz_8jQBsiKObSscBvwFxKErKqKWIz560").build()

    # Add a start handler to respond to /start command
    start_handler = CommandHandler('start', start)
    application.add_handler(start_handler)

    # Run the bot until stopped
    application.run_polling()