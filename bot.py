import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import pandas as pd
import ccxt
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.session.aiohttp import AiohttpSession
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

if not TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в .env!")

db_path = "signals.db"

bot = Bot(token=TOKEN, session=AiohttpSession())
dp = Dispatcher()

exchange = ccxt.mexc({'enableRateLimit': True})

# ====================== ЗАЩИТА ======================
last_signal_time = defaultdict(lambda: datetime(2000, 1, 1, tzinfo=timezone.utc))
is_generating = False
scheduler = AsyncIOScheduler(timezone="UTC")


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

    # Усиленная защита — 45 минут между одинаковыми сигналами
    if (now - last_signal_time[key]).total_seconds() < 2700:   # 45 минут
        return
    last_signal_time[key] = now

    async with aiosqlite.connect(db_path) as db:
        await db.execute('''
            INSERT INTO signals (pair, direction, entry_price, tp, sl, timestamp, status)
            VALUES (?, ?, ?, ?, ?, ?, 'open')
        ''', (pair, direction, entry_price, tp, sl, now.isoformat()))
        await db.commit()
        
        cursor = await db.execute("SELECT last_insert_rowid()")
        signal_id = (await cursor.fetchone())[0]

    hashtag = f"SIG_{signal_id:04d}"

    async with aiosqlite.connect(db_path) as db:
        await db.execute("UPDATE signals SET hashtag = ? WHERE id = ?", (hashtag, signal_id))
        await db.commit()

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
    print(f"✅ Сигнал отправлен → {pair} | {direction}")


# ====================== ГЕНЕРАЦИЯ СИГНАЛОВ ======================
async def generate_signals():
    global is_generating
    if is_generating:
        return
    is_generating = True

    try:
        pairs = ["BTC/USDT", "ETH/USDT"]
        for pair in pairs:
            try:
                ohlcv = exchange.fetch_ohlcv(pair, '1h', limit=100)
                df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                df['sma20'] = df['close'].rolling(20).mean()
                df['sma50'] = df['close'].rolling(50).mean()

                curr = df.iloc[-1]
                prev = df.iloc[-2]
                price = float(curr['close'])

                if pd.isna(curr.get('sma20')) or pd.isna(curr.get('sma50')):
                    continue

                direction = sl = tp = None

                if curr['sma20'] > curr['sma50'] and prev['sma20'] <= prev['sma50']:
                    direction = "LONG"
                    sl = round(price * 0.985, 2)
                    tp = round(price * 1.03, 2)
                elif curr['sma20'] < curr['sma50'] and prev['sma20'] >= prev['sma50']:
                    direction = "SHORT"
                    sl = round(price * 1.015, 2)
                    tp = round(price * 0.97, 2)

                if direction:
                    await send_signal(pair, direction, price, tp, sl)
            except Exception as e:
                logging.error(f"Ошибка анализа {pair}: {e}")
    finally:
        is_generating = False


# ====================== МОНИТОРИНГ TP/SL ======================
async def monitor_open_signals():
    try:
        async with aiosqlite.connect(db_path) as db:
            rows = await db.execute_fetchall("""
                SELECT id, pair, direction, tp, sl, hashtag FROM signals WHERE status = 'open'
            """)

        for row in rows:
            signal_id, pair, direction, tp, sl, hashtag = row
            try:
                ticker = exchange.fetch_ticker(pair)
                current_price = ticker['last']

                closed = False
                status = None

                if direction == "LONG":
                    if current_price >= tp:
                        status = "closed_tp"
                        closed = True
                    elif current_price <= sl:
                        status = "closed_sl"
                        closed = True
                else:
                    if current_price <= tp:
                        status = "closed_tp"
                        closed = True
                    elif current_price >= sl:
                        status = "closed_sl"
                        closed = True

                if closed:
                    async with aiosqlite.connect(db_path) as db:
                        await db.execute(
                            "UPDATE signals SET status = ?, close_price = ? WHERE id = ?",
                            (status, current_price, signal_id)
                        )
                        await db.commit()

                    status_text = "✅ TAKE PROFIT" if status == "closed_tp" else "❌ STOP LOSS"
                    text = f"""📢 <b>Сигнал закрыт #{hashtag}</b>

{status_text}
Цена закрытия: <b>{current_price:,.2f} USDT</b>"""

                    await broadcast_message(text)
                    print(f"📌 Сигнал #{hashtag} закрыт → {status_text}")
            except Exception as e:
                logging.error(f"Ошибка мониторинга {hashtag}: {e}")
    except Exception as e:
        logging.error(f"Ошибка monitor_open_signals: {e}")


# ====================== ХЭНДЛЕРЫ (оставил твои) ======================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await add_subscriber(message.from_user)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📜 История сигналов")], [KeyboardButton(text="❌ Отписаться")]],
        resize_keyboard=True
    )
    await message.answer(
        f"👋 <b>Привет, {message.from_user.first_name}!</b>\n\n"
        "🤖 Бот анализирует MEXC и присылает сигналы.", 
        parse_mode="HTML", reply_markup=kb
    )


@dp.message(F.text == "📜 История сигналов")
async def show_history(message: types.Message):
    async with aiosqlite.connect(db_path) as db:
        history = await db.execute_fetchall("SELECT * FROM signals ORDER BY id DESC LIMIT 15")
    if not history:
        await message.answer("📭 История пуста.")
        return

    text = "📜 <b>Последние сигналы:</b>\n\n"
    for row in history:
        _, pair, direction, entry, tp, sl, ts, status, close_p, hashtag = row
        emoji = "📈" if direction == "LONG" else "📉"
        st = "✅ TP" if status == "closed_tp" else "❌ SL" if status == "closed_sl" else "⏳ Open"
        text += f"<b>#{hashtag}</b> {pair} {direction} {emoji}\nВход: {entry:,.2f} | Статус: {st}\n\n"
    await message.answer(text, parse_mode="HTML")


@dp.message(F.text == "❌ Отписаться")
async def unsubscribe(message: types.Message):
    await remove_subscriber(message.from_user.id)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="✅ Подписаться")]], resize_keyboard=True)
    await message.answer("❌ Подписка отключена.", reply_markup=kb)


@dp.message(F.text == "✅ Подписаться")
async def subscribe_again(message: types.Message):
    await add_subscriber(message.from_user)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📜 История сигналов")], [KeyboardButton(text="❌ Отписаться")]],
        resize_keyboard=True
    )
    await message.answer("✅ Подписка активирована!", reply_markup=kb)


# ====================== ЗАПУСК ======================
async def main():
    await init_db()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    scheduler.add_job(generate_signals, 'interval', seconds=20, replace_existing=True)   # ← Улучшено
    scheduler.add_job(monitor_open_signals, 'interval', seconds=30, replace_existing=True)

    scheduler.start()
    print("🚀 Бот запущен на MEXC")
    print("   • Генерация сигналов — каждые 20 секунд")
    print("   • Мониторинг TP/SL — каждые 30 секунд")
    print("   • Защита от дублей — 45 минут")

    try:
        await dp.start_polling(bot)
    finally:
        scheduler.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
