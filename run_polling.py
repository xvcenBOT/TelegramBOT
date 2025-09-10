import asyncio
from bot import bot

async def main():
    try:
        await bot.remove_webhook()
    except Exception:
        pass
    await bot.infinity_polling(skip_pending=True)

if __name__ == '__main__':
    asyncio.run(main())

