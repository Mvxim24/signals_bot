import asyncio
import logging
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

load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

if not TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в .env!")

db_path = "signals.db"

bot = Bot(token=TOKEN, session=AiohttpSession())
dp = Dispatcher()

# ====================== MEXC ======================
exchange = ccxt.mexc({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'spot',
        'recvWindow': 20000,
    },
    'timeout': 30000,
})

last_signal_time = defaultdict(lambda: datetime(2000, 1, 1, tzinfo=timezone.utc))

# ====================== БАЗА ДАННЫХ ======================
async def init_db():
    async with aiosqlite.connect(db_path) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS subscribers (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                subscribed_at TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT,
                direction TEXT,
                entry_price REAL,
                tp REAL,
                sl REAL,
                timestamp TEXT,
                status TEXT DEFAULT 'open',
                close_price REAL,
                hashtag TEXT
            )
        ''')
        await db.commit()


async def add_subscriber(user: types.User):
    async with aiosqlite.connect(db_path) as db:
        await db.execute('''
            INSERT OR REPLACE INTO subscribers (user_id, username, first_name, subscribed_at)
            VALUES (?, ?, ?, ?)
        ''', (user.id, user.username, user.first_name, datetime.now(timezone.utc).isoformat()))
        await db.commit()


async def remove_subscriber(user_id: int):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("DELETE FROM subscribers WHERE user_id = ?", (user_id,))
        await db.commit()


async def get_all_subscribers():
    async with aiosqlite.connect(db_path) as db:
        rows = await db.execute_fetchall("SELECT user_id FROM subscribers")
        return [row[0] for row in rows]


async def broadcast_message(text: str):
    subscribers = await get_all_subscribers()
    tasks = [bot.send_message(uid, text, parse_mode="HTML", disable_web_page_preview=True) for uid in subscribers]
    await asyncio.gather(*tasks, return_exceptions=True)


# ====================== ОТПРАВКА СИГНАЛА ======================
async def send_signal(pair: str, direction: str, entry_price: float, tp: float, sl: float):
    key = f"{pair}_{direction}"
    now = datetime.now(timezone.utc)
    if (now - last_signal_time[key]).total_seconds() < 1800:  # 30 минут
        return
    last_signal_time[key] = now

    async with aiosqlite.connect(db_path) as db:
        await db.execute('''
            INSERT INTO signals (pair, direction, entry_price, tp, sl, timestamp, status, hashtag)
            VALUES (?, ?, ?, ?, ?, ?, 'open', 'temp')
        ''', (pair, direction, entry_price, tp, sl, now.isoformat()))
        await db.commit()
        cursor = await db.execute("SELECT last_insert_rowid()")
        signal_id = (await cursor.fetchone())[0]

    hashtag = f"SIG_{signal_id:04d}"
    emoji = "📈" if direction == "LONG" else "📉"
    direction_text = "LONG ▲" if direction == "LONG" else "SHORT ▼"

    tp_p = ((tp - entry_price) / entry_price) * 100
    sl_p = ((sl - entry_price) / entry_price) * 100

    text = f"""🚨 <b>НОВЫЙ ТОРГОВЫЙ СИГНАЛ #{hashtag}</b>

{emoji} <b>{pair}</b> — <b>{direction_text}</b> {emoji}

──────────────────
💰 <b>Цена входа:</b> <code>{entry_price:,.2f} USDT</code>
🎯 <b>Take Profit:</b> <code>{tp:,.2f} USDT</code> <b>(+{tp_p:.2f}%)</b>
🛑 <b>Stop Loss:</b> <code>{sl:,.2f} USDT</code> <b>({sl_p:.2f}%)</b>

🕒 <b>Время сигнала:</b> {now.strftime('%d.%m.%Y %H:%M:%S UTC')}

🔍 <b>#{hashtag}</b>"""

    await broadcast_message(text)
    print(f"✅ Сигнал отправлен → {pair} | {direction} | {entry_price:.2f}")


# ====================== РЕАЛ-ТАЙМ МОНИТОРИНГ СВЕЧЕЙ ======================
async def watch_ohlcv_for_signals():
    print("📡 Запуск реал-тайм мониторинга на MEXC (1h)")
    pairs = ["BTC/USDT", "ETH/USDT"]

    while True:
        try:
            await exchange.load_markets()
            print("✅ Рынки MEXC успешно загружены!")

            while True:
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
                        closes = pd.Series([float(c[4]) for c in ohlcv[-60:]])
                        sma20 = closes.rolling(20).mean().iloc[-1]
                        sma50 = closes.rolling(50).mean().iloc[-1]

                        if pd.isna(sma20) or pd.isna(sma50):
                            continue

                        if sma20 > sma50 and prev.get('sma20', 0) <= prev.get('sma50', 0):
                            sl = round(price * 0.985, 2)
                            tp = round(price * 1.03, 2)
                            await send_signal(pair, "LONG", price, tp, sl)
                        elif sma20 < sma50 and prev.get('sma20', 0) >= prev.get('sma50', 0):
                            sl = round(price * 1.015, 2)
                            tp = round(price * 0.97, 2)
                            await send_signal(pair, "SHORT", price, tp, sl)

                    except Exception as e:
                        logging.error(f"Ошибка по паре {pair}: {e}")
                        await asyncio.sleep(2)

                await asyncio.sleep(1)

        except Exception as e:
            logging.error(f"Критическая ошибка watch_ohlcv (MEXC): {e}")
            print("🔄 Перезапуск через 15 секунд...")
            await asyncio.sleep(15)


# ====================== ХЭНДЛЕРЫ ======================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await add_subscriber(message.from_user)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📜 История сигналов")],
            [KeyboardButton(text="❌ Отписаться")]
        ],
        resize_keyboard=True
    )
    await message.answer(f"👋 <b>Привет!</b>\n✅ Ты подписан на реал-тайм сигналы с MEXC.", parse_mode="HTML", reply_markup=kb)


@dp.message(F.text == "📜 История сигналов")
async def show_history(message: types.Message):
    async with aiosqlite.connect(db_path) as db:
        history = await db.execute_fetchall("SELECT * FROM signals ORDER BY id DESC LIMIT 50")
    if not history:
        await message.answer("📭 Пока нет сигналов.")
        return
    await message.answer("📜 История скоро будет полной. Пока показаны последние 50 сигналов (функция в разработке).")


@dp.message(Command("test_signal"))
async def test_signal(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Только для админа.")
        return
    await message.answer("🧪 Отправляю тестовый сигнал с MEXC...")
    await send_signal("BTC/USDT", "LONG", 65234.5, 66865.0, 63929.8)


# ====================== ЗАПУСК ======================
async def main():
    await init_db()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    print("🚀 Бот запущен на MEXC (реал-тайм режим)")

    task = asyncio.create_task(watch_ohlcv_for_signals())

    try:
        await asyncio.gather(dp.start_polling(bot), task)
    finally:
        await exchange.close()
        await bot.session.close()
        print("🛑 Бот остановлен")


if __name__ == "__main__":
    asyncio.run(main())
