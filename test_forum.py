#!/usr/bin/env python3
"""
Test script for forum topic detection
"""

import asyncio
import logging
from telegram import Bot
from src.core.config import Config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_forum_detection():
    """Test forum topic detection functionality"""
    try:
        config = Config.load()
        bot = Bot(token=config.telegram_token)
        
        # Replace with your forum chat ID
        forum_chat_id = -1001234567890  # Replace with actual forum chat ID
        
        logger.info(f"Testing forum topic detection for chat {forum_chat_id}")
        
        # Get chat info
        chat = await bot.get_chat(forum_chat_id)
        logger.info(f"Chat type: {chat.type}")
        logger.info(f"Is forum: {hasattr(chat, 'is_forum') and chat.is_forum}")
        
        if hasattr(chat, 'is_forum') and chat.is_forum:
            logger.info("This is a forum group!")
            
            # Test different topic IDs
            test_topics = [2, 3, 4, 5, 6, 7, 8, 9, 10]
            
            for topic_id in test_topics:
                try:
                    test_message = await bot.send_message(
                        chat_id=forum_chat_id,
                        text="🔍 Test",
                        message_thread_id=topic_id
                    )
                    await bot.delete_message(forum_chat_id, test_message.message_id)
                    logger.info(f"✅ Topic {topic_id} is OPEN")
                except Exception as e:
                    error_msg = str(e).lower()
                    if "topic_closed" in error_msg or "topic closed" in error_msg:
                        logger.info(f"❌ Topic {topic_id} is CLOSED")
                    elif "message thread not found" in error_msg:
                        logger.info(f"❌ Topic {topic_id} does not exist")
                    else:
                        logger.warning(f"⚠️ Topic {topic_id} error: {e}")
        else:
            logger.info("This is not a forum group")
            
    except Exception as e:
        logger.error(f"Error in forum detection test: {e}")

if __name__ == "__main__":
    asyncio.run(test_forum_detection())
