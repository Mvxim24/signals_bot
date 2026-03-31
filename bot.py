import asyncio
import logging
import os
import pandas as pd
import ccxt
import aiosqlite
import pandas_ta as ta  # Профессиональная библиотека индикаторов
from datetime import datetime, timezone
from collections import defaultdict
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.session.aiohttp import AiohttpSession
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

load_dotenv()

# --- КОНФИГУРАЦИЯ ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
PAIRS = ["BTC/USDT", "ETH/USDT"]
TIMEFRAME = '1h'  # Часовой таймфрейм
db_path = "signals.db"

bot = Bot(token=TOKEN, session=AiohttpSession())
dp = Dispatcher()
exchange = ccxt.mexc({'enableRateLimit': True})
scheduler = AsyncIOScheduler(timezone="UTC")
is_generating = False

# --- БАЗА ДАННЫХ ---
async def init_db():
    async with aiosqlite.connect(db_path) as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS subscribers (user_id INTEGER PRIMARY KEY)''')
        await db.execute('''CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, pair TEXT, direction TEXT, 
            entry_price REAL, tp REAL, sl REAL, timestamp TEXT, status TEXT DEFAULT 'open', hashtag TEXT
        )''')
        await db.commit()

async def broadcast_message(text: str):
    async with aiosqlite.connect(db_path) as db:
        rows = await db.execute_fetchall("SELECT user_id FROM subscribers")
    tasks = [bot.send_message(row[0], text, parse_mode="HTML") for row in rows]
    await asyncio.gather(*tasks, return_exceptions=True)

# --- ЛОГИКА СИГНАЛОВ (Strategy Core) ---
async def generate_signals():
    global is_generating
    if is_generating: return
    is_generating = True
    
    try:
        for pair in PAIRS:
            # Загружаем чуть больше данных для точности индикаторов
            ohlcv = exchange.fetch_ohlcv(pair, TIMEFRAME, limit=100)
            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            
            # Считаем SMA через pandas_ta (как в проф. ботах)
            df['sma_fast'] = ta.sma(df['close'], length=20)
            df['sma_slow'] = ta.sma(df['close'], length=50)
            
            # Берем последнюю ЗАКРЫТУЮ свечу (индекс -2), так как -1 еще шевелится
            last_closed = df.iloc[-2]
            prev_closed = df.iloc[-3]
            current_price = df.iloc[-1]['close'] # Актуальная цена для входа

            # Условия пересечения (Golden Cross / Death Cross)
            long_cross = (prev_closed['sma_fast'] <= prev_closed['sma_slow']) and \
                         (last_closed['sma_fast'] > last_closed['sma_slow'])
            
            short_cross = (prev_closed['sma_fast'] >= prev_closed['sma_slow']) and \
                          (last_closed['sma_fast'] < last_closed['sma_slow'])

            # Доп. фильтр: объем должен быть выше среднего (защита от ложных входов)
            avg_vol = df['vol'].tail(20).mean()
            vol_ok = last_closed['vol'] > avg_vol

            direction = None
            if long_cross and vol_ok:
                direction, tp_mult, sl_mult = "LONG", 1.03, 0.985
            elif short_cross and vol_ok:
                direction, tp_mult, sl_mult = "SHORT", 0.97, 1.015

            if direction:
                entry = round(current_price, 2)
                tp = round(entry * tp_mult, 2)
                sl = round(entry * sl_mult, 2)
                await send_signal_logic(pair, direction, entry, tp, sl)

    except Exception as e:
        logging.error(f"Ошибка стратегии: {e}")
    finally:
        is_generating = False

async def send_signal_logic(pair, direction, entry, tp, sl):
    now = datetime.now(timezone.utc)
    hashtag = f"SIG_{now.strftime('%H%M%S')}"
    
    # Сохраняем в БД
    async with aiosqlite.connect(db_path) as db:
        await db.execute("INSERT INTO signals (pair, direction, entry_price, tp, sl, timestamp, hashtag) VALUES (?,?,?,?,?,?,?)",
                         (pair, direction, entry, tp, sl, now.isoformat(), hashtag))
        await db.commit()

    emoji = "🚀" if direction == "LONG" else "📉"
    text = f"""{emoji} <b>НОВЫЙ СИГНАЛ: {pair}</b> {emoji}
    
<b>Направление:</b> {direction}
<b>Вход:</b> <code>{entry}</code>
<b>Цель (TP):</b> <code>{tp}</code>
<b>Стоп (SL):</b> <code>{sl}</code>

📊 <i>Сигнал сформирован на основе пересечения SMA 20/50 с подтверждением объема.</i>
#{hashtag}"""
    await broadcast_message(text)

# --- МОНИТОРИНГ (Check TP/SL) ---
async def monitor_open_signals():
    async with aiosqlite.connect(db_path) as db:
        rows = await db.execute_fetchall("SELECT id, pair, direction, tp, sl, hashtag FROM signals WHERE status = 'open'")
    
    for row in rows:
        sid, pair, direct, tp, sl, hashtag = row
        ticker = exchange.fetch_ticker(pair)
        price = ticker['last']
        
        closed = False
        if direct == "LONG":
            if price >= tp: res = "✅ TAKE PROFIT"; closed = True
            elif price <= sl: res = "❌ STOP LOSS"; closed = True
        else:
            if price <= tp: res = "✅ TAKE PROFIT"; closed = True
            elif price >= sl: res = "❌ STOP LOSS"; closed = True
            
        if closed:
            async with aiosqlite.connect(db_path) as db:
                await db.execute("UPDATE signals SET status = 'closed' WHERE id = ?", (sid,))
                await db.commit()
            await broadcast_message(f"🏁 <b>Сигнал {hashtag} закрыт!</b>\nРезультат: {res}\nЦена: {price}")
            asyncio.create_task(generate_signals()) # Сразу ищем новый вход

# --- ХЭНДЛЕРЫ ---
@dp.message(Command("start"))
async def start(m: types.Message):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("INSERT OR IGNORE INTO subscribers VALUES (?)", (m.from_user.id,))
        await db.commit()
    await m.answer("👋 <b>Привет!</b>\nЭтот бот отслеживает сигналы по BTC и ETH (SMA 20/50 + Volume).")

# --- ЗАПУСК ---
async def main():
    await init_db()
    logging.basicConfig(level=logging.INFO)
    
    # Как ты просил: очень частая проверка (каждые 10 сек)
    scheduler.add_job(generate_signals, 'interval', seconds=10)
    scheduler.add_job(monitor_open_signals, 'interval', seconds=30)
    scheduler.start()
    
    print("💎 Снайпер-бот запущен. Охота на BTC/ETH началась!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
