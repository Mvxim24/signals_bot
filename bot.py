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

# Глобальный exchange ccxt.pro
exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
})

# Кэш для защиты от дублирования сигналов
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

# ====================== ПОДПИСЧИКИ ======================
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
    time_str = now.strftime('%d.%m.%Y %H:%M:%S UTC')

    text = f"""🚨 <b>НОВЫЙ ТОРГОВЫЙ СИГНАЛ #{hashtag}</b>

{emoji} <b>{pair}</b> — <b>{direction_text}</b> {emoji}

──────────────────
💰 <b>Цена входа:</b> <code>{entry_price:,.2f} USDT</code>

🎯 <b>Take Profit:</b> <code>{tp:,.2f} USDT</code> <b>(+{tp_p:.2f}%)</b>
🛑 <b>Stop Loss:</b> <code>{sl:,.2f} USDT</code> <b>({sl_p:.2f}%)</b>

──────────────────
🕒 <b>Время сигнала:</b> {time_str}

🔍 <b>#{hashtag}</b>"""

    await broadcast_message(text)
    print(f"✅ Сигнал отправлен → {pair} {direction} {entry_price:.2f}")

# ====================== РЕАЛ-ТАЙМ ГЕНЕРАЦИЯ СИГНАЛОВ (watch_ohlcv) ======================
async def watch_ohlcv_for_signals():
    print("📡 Запущен реал-тайм мониторинг свечей (1h)")
    pairs = ["BTC/USDT", "ETH/USDT"]
    last_candles = {}

    while True:
        try:
            for pair in pairs:
                ohlcv = await exchange.watch_ohlcv(pair, '1h', limit=2)
                df = pd.DataFrame(ohlcv[-2:], columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                
                curr = df.iloc[-1]
                prev = df.iloc[-2]
                price = curr['close']

                df['sma20'] = df['close'].rolling(20).mean()
                df['sma50'] = df['close'].rolling(50).mean()

                if pd.isna(curr['sma20']) or pd.isna(curr['sma50']):
                    continue

                # Проверка пересечения
                if curr['sma20'] > curr['sma50'] and prev['sma20'] <= prev['sma50']:
                    sl = round(price * 0.985, 2)
                    tp = round(price * 1.03, 2)
                    await send_signal(pair, "LONG", price, tp, sl)
                elif curr['sma20'] < curr['sma50'] and prev['sma20'] >= prev['sma50']:
                    sl = round(price * 1.015, 2)
                    tp = round(price * 0.97, 2)
                    await send_signal(pair, "SHORT", price, tp, sl)

        except Exception as e:
            logging.error(f"Ошибка watch_ohlcv: {e}")
            await asyncio.sleep(5)
            continue

# ====================== РЕАЛ-ТАЙМ МОНИТОРИНГ TP/SL (watch_ticker) ======================
async def watch_tickers_for_monitoring():
    print("📡 Запущен реал-тайм мониторинг TP/SL")
    while True:
        try:
            async with aiosqlite.connect(db_path) as db:
                rows = await db.execute_fetchall("""
                    SELECT id, pair, direction, tp, sl, hashtag FROM signals WHERE status = 'open'
                """)

            if not rows:
                await asyncio.sleep(10)
                continue

            # Следим только за парами, где есть открытые сигналы
            active_pairs = list({row[1] for row in rows})
            for pair in active_pairs:
                try:
                    ticker = await exchange.watch_ticker(pair)
                    current_price = ticker['last']

                    for row in rows:
                        if row[1] != pair:
                            continue
                        signal_id, _, direction, tp, sl, hashtag = row

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
                    logging.error(f"Ошибка ticker {pair}: {e}")

            await asyncio.sleep(1)  # небольшая пауза

        except Exception as e:
            logging.error(f"Критическая ошибка мониторинга: {e}")
            await asyncio.sleep(10)

# ====================== ХЭНДЛЕРЫ (без изменений) ======================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await add_subscriber(message.from_user)
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📜 История сигналов")],
        [KeyboardButton(text="❌ Отписаться")]
    ], resize_keyboard=True)
    await message.answer(f"👋 <b>Привет, {message.from_user.first_name}!</b>\n\n✅ Ты подписан на реал-тайм сигналы Alfa Signals.", parse_mode="HTML", reply_markup=kb)

@dp.message(F.text == "📜 История сигналов")
async def show_history(message: types.Message):
    async with aiosqlite.connect(db_path) as db:
        history = await db.execute_fetchall("SELECT * FROM signals ORDER BY id DESC LIMIT 100")
    if not history:
        return await message.answer("📭 Пока нет сигналов.")
    # (тот же код истории, что был раньше — оставил для краткости, можешь вставить из предыдущей версии)

    text = "📜 <b>История сигналов</b>\n\n"
    for row in history:
        _, pair, direction, entry, tp, sl, ts, status, close_p, hashtag = row
        emoji = "📈" if direction == "LONG" else "📉"
        st = "✅ TP" if status == "closed_tp" else "❌ SL" if status == "closed_sl" else "⏳ Открыт"
        line = f"<b>#{hashtag}</b> {pair} {direction} {emoji}\nВход: {entry:.2f} | TP: {tp:.2f} | SL: {sl:.2f}\n"
        if close_p:
            line += f"Закрыто: {close_p:.2f} — {st}\n"
        else:
            line += f"Статус: {st}\n"
        line += f"Время: {ts[:16]}\n\n"
        text += line
    await message.answer(text, parse_mode="HTML")

@dp.message(F.text == "❌ Отписаться")
async def unsubscribe(message: types.Message):
    await remove_subscriber(message.from_user.id)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="✅ Подписаться")]], resize_keyboard=True)
    await message.answer("❌ Ты отписался.", reply_markup=kb)

@dp.message(F.text == "✅ Подписаться")
async def subscribe_again(message: types.Message):
    await add_subscriber(message.from_user)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📜 История сигналов")], [KeyboardButton(text="❌ Отписаться")]], resize_keyboard=True)
    await message.answer("✅ Ты снова подписан!", reply_markup=kb)

@dp.message(Command("test_signal"))
async def test_signal(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return await message.answer("⛔ Только для админа.")
    await message.answer("🧪 Тестовый сигнал...")
    await send_signal("BTC/USDT", "LONG", 65234.5, 66865.0, 63929.8)

# ====================== ЗАПУСК ======================
async def main():
    await init_db()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Запускаем реал-тайм задачи
    tasks = [
        asyncio.create_task(watch_ohlcv_for_signals()),
        asyncio.create_task(watch_tickers_for_monitoring()),
    ]

    print("🚀 Бот запущен в РЕАЛ-ТАЙМ режиме!")
    print("📡 Генерация сигналов + мониторинг TP/SL работают мгновенно")

    try:
        await asyncio.gather(dp.start_polling(bot), *tasks)
    finally:
        await exchange.close()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
