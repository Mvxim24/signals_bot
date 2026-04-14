import asyncio
import logging
import os
from datetime import datetime, timezone
from collections import defaultdict

import pandas as pd
import ccxt.async_support as ccxt  # Перешли на асинхронную версию CCXT
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

# ==================== EXCHANGE ====================
# Используем асинхронный класс для избежания блокировок потока
exchange = ccxt.mexc({
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
})

last_signal_time = defaultdict(lambda: datetime(2000, 1, 1, tzinfo=timezone.utc))
is_generating = False
generate_lock = asyncio.Lock()
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
        async with db.execute("SELECT user_id FROM subscribers") as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

async def broadcast_message(text: str):
    subscribers = await get_all_subscribers()
    if not subscribers:
        return
    tasks = [bot.send_message(uid, text, parse_mode="HTML", disable_web_page_preview=True) for uid in subscribers]
    await asyncio.gather(*tasks, return_exceptions=True)

# ====================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======================
def get_tick_size(symbol: str) -> float:
    try:
        market = exchange.market(symbol)
        return float(market['precision']['price'])
    except Exception:
        return 0.0001

def round_price(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return round(price, 4)
    return round(price / tick_size) * tick_size

async def load_markets_once():
    try:
        await exchange.load_markets()
        logging.info(f"✅ Загружено {len(exchange.markets)} рынков с MEXC")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки markets: {e}")

# ====================== ОТПРАВКА СИГНАЛА ======================
async def send_signal(pair: str, direction: str, entry_price: float, tp: float, sl: float):
    try:
        key = f"{pair}_{direction}"
        now = datetime.now(timezone.utc)

        if (now - last_signal_time[key]).total_seconds() < 2700:
            return

        last_signal_time[key] = now
        tick_size = get_tick_size(pair)
        entry_price = round_price(entry_price, tick_size)
        tp = round_price(tp, tick_size)
        sl = round_price(sl, tick_size)

        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute('''
                INSERT INTO signals (pair, direction, entry_price, tp, sl, timestamp, status)
                VALUES (?, ?, ?, ?, ?, ?, 'open')
            ''', (pair, direction, entry_price, tp, sl, now.isoformat()))
            signal_id = cursor.lastrowid
            hashtag = f"SIG_{signal_id:04d}"
            await db.execute("UPDATE signals SET hashtag = ? WHERE id = ?", (hashtag, signal_id))
            await db.commit()

        emoji = "📈" if direction == "LONG" else "📉"
        direction_text = "LONG ▲" if direction == "LONG" else "SHORT ▼"
        tp_p = ((tp - entry_price) / entry_price) * 100
        sl_p = ((sl - entry_price) / entry_price) * 100

        text = f"""🚨 <b>НОВЫЙ ТОРГОВЫЙ СИГНАЛ #{hashtag}</b>\n\n{emoji} <b>{pair}</b> — <b>{direction_text}</b> {emoji}\n\n──────────────────\n💰 <b>Цена входа:</b> <code>{entry_price:,.4f} USDT</code>\n🎯 <b>Take Profit:</b> <code>{tp:,.4f} USDT</code> <b>(+{tp_p:.2f}%)</b>\n🛑 <b>Stop Loss:</b> <code>{sl:,.4f} USDT</code> <b>({sl_p:.2f}%)</b>\n\n🕒 <b>Время сигнала:</b> {now.strftime('%d.%m.%Y %H:%M:%S UTC')}\n🔍 <b>#{hashtag}</b>"""

        await broadcast_message(text)
    except Exception as e:
        logging.error(f"Ошибка при отправке сигнала {pair}: {e}")

# ====================== ГЕНЕРАЦИЯ СИГНАЛОВ ======================
async def generate_signals():
    global is_generating
    if is_generating: return
    async with generate_lock:
        is_generating = True
        try:
            pairs = ["BTC/USDT", "ETH/USDT"]
            async with aiosqlite.connect(db_path) as db:
                async with db.execute("SELECT pair FROM signals WHERE status = 'open'") as cursor:
                    open_pairs = {row[0] for row in await cursor.fetchall()}

            for pair in pairs:
                if pair in open_pairs: continue
                
                # Увеличен лимит до 500 для точности EMA200
                ohlcv_1h = await exchange.fetch_ohlcv(pair, '1h', limit=500)
                df_1h = pd.DataFrame(ohlcv_1h, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                df_1h['ema50'] = df_1h['close'].ewm(span=50, adjust=False).mean()
                df_1h['ema200'] = df_1h['close'].ewm(span=200, adjust=False).mean()

                curr_1h = df_1h.iloc[-1]
                bullish_bias = (curr_1h['ema50'] > curr_1h['ema200']) and (curr_1h['close'] > curr_1h['ema200'])
                bearish_bias = (curr_1h['ema50'] < curr_1h['ema200']) and (curr_1h['close'] < curr_1h['ema200'])

                if not (bullish_bias or bearish_bias): continue

                ohlcv_15m = await exchange.fetch_ohlcv(pair, '15m', limit=100)
                df = pd.DataFrame(ohlcv_15m, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                df['bb_middle'] = df['close'].rolling(window=25).mean()
                df['bb_std'] = df['close'].rolling(window=25).std()
                df['bb_upper'] = df['bb_middle'] + (df['bb_std'] * 2)
                df['bb_lower'] = df['bb_middle'] - (df['bb_std'] * 2)
                df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
                df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()

                curr, prev = df.iloc[-1], df.iloc[-2]
                direction, entry_price, sl, tp = None, curr['close'], None, None

                if bullish_bias and prev['ema9'] <= prev['ema21'] and curr['ema9'] > curr['ema21'] and curr['low'] <= curr['bb_lower'] * 1.001:
                    direction, sl = "LONG", curr['low'] * 0.9975
                    tp = entry_price + ((entry_price - sl) * 2.2)
                elif bearish_bias and prev['ema9'] >= prev['ema21'] and curr['ema9'] < curr['ema21'] and curr['high'] >= curr['bb_upper'] * 0.999:
                    direction, sl = "SHORT", curr['high'] * 1.0025
                    tp = entry_price - ((sl - entry_price) * 2.2)

                if direction:
                    vol_ma = df['vol'].rolling(20).mean().iloc[-1]
                    if curr['vol'] >= vol_ma * 0.75:
                        await send_signal(pair, direction, entry_price, tp, sl)
        finally:
            is_generating = False

# ====================== МОНИТОРИНГ ======================
async def monitor_open_signals():
    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute("SELECT id, pair, direction, tp, sl, hashtag, entry_price FROM signals WHERE status = 'open'") as cursor:
                rows = await cursor.fetchall()
        
        if not rows: return

        # Оптимизация: получаем все цены одним запросом
        tickers = await exchange.fetch_tickers([r[1] for r in rows])
        
        for row in rows:
            sid, pair, direction, tp, sl, hashtag, entry = row
            current_price = tickers.get(pair, {}).get('last')
            if not current_price: continue

            closed, status = False, None
            if direction == "LONG":
                if current_price >= tp: closed, status = True, "closed_tp"
                elif current_price <= sl: closed, status = True, "closed_sl"
            else:
                if current_price <= tp: closed, status = True, "closed_tp"
                elif current_price >= sl: closed, status = True, "closed_sl"

            if closed:
                async with aiosqlite.connect(db_path) as db:
                    await db.execute("UPDATE signals SET status = ?, close_price = ? WHERE id = ?", (status, current_price, sid))
                    await db.commit()
                
                pref = "✅ TAKE PROFIT" if status == "closed_tp" else "❌ STOP LOSS"
                await broadcast_message(f"📢 <b>Сигнал закрыт #{hashtag}</b>\n\n{pref}\nЦена: <b>{current_price:,.4f}</b>\nВход: <b>{entry:,.4f}</b>")
    except Exception as e:
        logging.error(f"Ошибка мониторинга: {e}")

# ====================== ХЭНДЛЕРЫ ======================
@dp.message(F.text == "📜 История сигналов")
async def show_history(message: types.Message):
    async with aiosqlite.connect(db_path) as db:
        # Явное указание полей для стабильности распаковки
        async with db.execute("SELECT hashtag, pair, direction, entry_price, status, close_price, timestamp FROM signals ORDER BY id DESC LIMIT 20") as cursor:
            history = await cursor.fetchall()

    if not history:
        await message.answer("📭 История пуста.")
        return

    res = "📜 <b>Последние 20 сигналов:</b>\n\n"
    for row in history:
        h, p, d, entry, stat, cp, ts = row
        st_text = "✅ TP" if stat == "closed_tp" else "❌ SL" if stat == "closed_sl" else "⏳ Open"
        res += f"<b>#{h}</b> {p} {d}\nEntry: <code>{entry:,.4f}</code> | {st_text}\n"
    await message.answer(res, parse_mode="HTML")

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await add_subscriber(message.from_user)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📜 История сигналов")], [KeyboardButton(text="❌ Отписаться")]], resize_keyboard=True)
    await message.answer(f"👋 Привет, {message.from_user.first_name}!\nСтратегия MTF активна.", reply_markup=kb)

# Остальные хендлеры (stats, unsubscribe) остаются без изменений, но используют асинхронный контекст аналогично вышеописанным.

# ====================== ЗАПУСК ======================
async def main():
    logging.basicConfig(level=logging.INFO)
    await init_db()
    await load_markets_once()

    scheduler.add_job(generate_signals, 'interval', seconds=60) # Увеличил интервал для стабильности
    scheduler.add_job(monitor_open_signals, 'interval', seconds=20)
    scheduler.start()

    try:
        await dp.start_polling(bot)
    finally:
        scheduler.shutdown()
        await bot.session.close()
        await exchange.close() # Важное закрытие сессии биржи

if __name__ == "__main__":
    asyncio.run(main())
