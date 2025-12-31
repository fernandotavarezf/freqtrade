#!/usr/bin/env python3
"""Test Telegram initialization to diagnose the async issue"""

import asyncio
import logging
from telegram.ext import Application

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Your telegram token from config
TOKEN = "8215075775:AAHkyu18Si2nFfHjz7vaR1gOSVyR1c4__eQ"

async def test_telegram_init():
    """Test telegram app initialization"""
    logger.info("Creating Telegram Application...")
    app = Application.builder().token(TOKEN).build()
    
    logger.info("Initializing Telegram Application...")
    await app.initialize()
    
    logger.info("Starting Telegram Application...")
    await app.start()
    
    logger.info("Telegram Application started successfully!")
    
    # Clean shutdown
    await app.stop()
    await app.shutdown()
    logger.info("Telegram Application stopped successfully!")

if __name__ == "__main__":
    try:
        asyncio.run(test_telegram_init())
        print("\n✓ SUCCESS: Telegram initialization works correctly!")
    except Exception as e:
        print(f"\n✗ FAILED: Telegram initialization error: {e}")
        import traceback
        traceback.print_exc()
