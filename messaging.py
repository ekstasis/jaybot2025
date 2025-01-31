import asyncio
import telegram

import config


async def async_send_message(msg: str):
    bot = telegram.Bot(config.TELEGRAM_BOT_TOKEN)
    await bot.send_message(chat_id=config.test_channel_id, parse_mode='MarkdownV2', text=message)


def send_message(msg: str):
    print("before asyncio.run")
    asyncio.run(async_send_message(msg))
    print("after asyncio.run")


if __name__ == '__main__':
    message = '__Testing__'
    send_message(message)