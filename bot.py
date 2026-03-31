import asyncio
import logging
import resource
import os
from datetime import datetime, timezone
from collections import defaultdict

import pandas as pd
import ccxt.pro as ccxt
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.session.aiohttp import AiohttpSession
from dotenv import load_dotenv

# ====================== НАСТРОЙКИ ======================
try:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (524288, hard))
    print(f"✅ Лимит файлов увеличен")
except:
    pass

load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

if not TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в .env!")

db_path = "signals.db"

bot = Bot(token=TOKEN, session=AiohttpSession())
dp = Dispatcher()

# Глобальный exchange
exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
})

last_signal_time = defaultdict(lambda: datetime(2000, 1, 1, tzinfo=timezone.utc))

# ====================== БАЗА ДАННЫХ ======================
async def init_db():
    async with aiosqlite.connect(db_path) as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS subscribers (...)''')  # оставь как было
        await db.execute('''CREATE TABLE IF NOT EXISTS signals (...)''')      # оставь как было
        await db.commit()

# (add_subscriber, remove_subscriber, get_all_subscribers, broadcast_message — оставь как в предыдущей версии)

async def broadcast_message(text: str):
    subscribers = await get_all_subscribers()
    tasks = [bot.send_message(uid, text, parse_mode="HTML", disable_web_page_preview=True) for uid in subscribers]
    await asyncio.gather(*tasks, return_exceptions=True)

# ====================== ОТПРАВКА СИГНАЛА ======================
async def send_signal(pair: str, direction: str, entry_price: float, tp: float, sl: float):
    # ... (оставь функцию как была в предыдущей версии — она рабочая)

# ====================== РЕАЛ-ТАЙМ ГЕНЕРАЦИЯ СИГНАЛОВ ======================
async def watch_ohlcv_for_signals():
    print("📡 Запуск реал-тайм мониторинга свечей (1h)")
    pairs = ["BTC/USDT", "ETH/USDT"]

    while True:
        try:
            await exchange.load_markets()  # ← КРИТИЧНО!
            print("✅ Рынки загружены успешно")

            while True:  # внутренний цикл
                for pair in pairs:
                    try:
                        ohlcv = await exchange.watch_ohlcv(pair, '1h', limit=3)
                        if len(ohlcv) < 2:
                            continue

                        df = pd.DataFrame(ohlcv[-2:], columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                        curr = df.iloc[-1]
                        prev = df.iloc[-2]
                        price = float(curr['close'])

                        # SMA
                        closes = pd.Series([float(c[4]) for c in ohlcv[-50:]])  # больше данных для SMA
                        sma20 = closes.rolling(20).mean().iloc[-1]
                        sma50 = closes.rolling(50).mean().iloc[-1]

                        if pd.isna(sma20) or pd.isna(sma50):
                            continue

                        if curr['sma20'] > curr['sma50'] and prev['sma20'] <= prev['sma50'] and sma20 > sma50:
                            sl = round(price * 0.985, 2)
                            tp = round(price * 1.03, 2)
                            await send_signal(pair, "LONG", price, tp, sl)
                        elif curr['sma20'] < curr['sma50'] and prev['sma20'] >= prev['sma50'] and sma20 < sma50:
                            sl = round(price * 1.015, 2)
                            tp = round(price * 0.97, 2)
                            await send_signal(pair, "SHORT", price, tp, sl)

                    except Exception as e:
                        logging.error(f"Ошибка по паре {pair}: {e}")
                        await asyncio.sleep(2)

                await asyncio.sleep(1)

        except Exception as e:
            logging.error(f"Критическая ошибка watch_ohlcv: {e}. Перезапуск через 10 сек...")
            await asyncio.sleep(10)

# ====================== РЕАЛ-ТАЙМ МОНИТОРИНГ TP/SL ======================
async def watch_tickers_for_monitoring():
    print("📡 Запуск реал-тайм мониторинга TP/SL")
    while True:
        try:
            await exchange.load_markets()
            # ... (остальная логика мониторинга как раньше, но с try/except вокруг watch_ticker)
            # Рекомендую: использовать watch_ticker только для активных пар
        except Exception as e:
            logging.error(f"Ошибка мониторинга: {e}")
            await asyncio.sleep(10)

# ====================== ХЭНДЛЕРЫ ======================
# (start, история, отписка — оставь как в предыдущей версии)

# ====================== ЗАПУСК ======================
async def main():
    await init_db()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    print("🚀 Бот запускается в реал-тайм режиме...")

    tasks = [
        asyncio.create_task(watch_ohlcv_for_signals()),
        asyncio.create_task(watch_tickers_for_monitoring()),
    ]

    try:
        await asyncio.gather(dp.start_polling(bot), *tasks)
    except asyncio.CancelledError:
        pass
    finally:
        await exchange.close()
        await bot.session.close()
        print("🛑 Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())
