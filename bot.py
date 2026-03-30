import asyncio
import logging
import resource
import os
from datetime import datetime, date

import pandas as pd
import ccxt
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.session.aiohttp import AiohttpSession
from aiohttp_socks import ProxyConnector  # Для обхода Connection Reset
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# ====================== НАСТРОЙКИ ДЛЯ MACOS ======================
try:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (524288, hard))
    print(f"✅ Лимит открытых файлов увеличен: {soft} → 524288")
except Exception as e:
    print(f"⚠️ Не удалось увеличить лимит файлов: {e}")

# ========================= НАСТРОЙКИ =========================
load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

if not TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в .env файле!")

db_path = "signals.db"

# --- НАСТРОЙКА ПРОКСИ (ДЛЯ ОБХОДА CONNECTION RESET) ---
# Если у вас есть конкретный прокси, расскомментируйте строку ниже и вставьте данные:
# connector = ProxyConnector.from_url('socks5://user:pass@host:port')
# session = AiohttpSession(connector=connector)

# Если используете системный VPN, оставляем так:
session = AiohttpSession()
bot = Bot(token=TOKEN, session=session)
dp = Dispatcher()


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
        ''', (user.id, user.username, user.first_name, datetime.now().isoformat()))
        await db.commit()


async def remove_subscriber(user_id: int):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("DELETE FROM subscribers WHERE user_id = ?", (user_id,))
        await db.commit()


async def get_all_subscribers():
    async with aiosqlite.connect(db_path) as db:
        rows = await db.execute_fetchall("SELECT user_id FROM subscribers")
        return [row[0] for row in rows]


async def broadcast_message(text: str, parse_mode="HTML"):
    subscribers = await get_all_subscribers()
    for user_id in subscribers:
        try:
            await bot.send_message(user_id, text, parse_mode=parse_mode, disable_web_page_preview=True)
        except Exception as e:
            logging.error(f"Ошибка отправки пользователю {user_id}: {e}")


# ====================== ОТПРАВКА СИГНАЛА ======================
async def send_signal(pair: str, direction: str, entry_price: float, tp: float, sl: float):
    async with aiosqlite.connect(db_path) as db:
        await db.execute('''
            INSERT INTO signals (pair, direction, entry_price, tp, sl, timestamp, status, hashtag)
            VALUES (?, ?, ?, ?, ?, ?, 'open', 'temp')
        ''', (pair, direction, entry_price, tp, sl, datetime.now().isoformat()))
        await db.commit()

        cursor = await db.execute("SELECT last_insert_rowid()")
        row = await cursor.fetchone()
        signal_id = row[0]

    hashtag = f"SIG_{signal_id:04d}"

    async with aiosqlite.connect(db_path) as db:
        await db.execute("UPDATE signals SET hashtag = ? WHERE id = ?", (hashtag, signal_id))
        await db.commit()

    emoji = "📈" if direction == "LONG" else "📉"
    text = f"""🚨 <b>Новый торговый сигнал #{hashtag}</b>

Пара: <b>{pair}</b>
Направление: <b>{direction} {emoji}</b>
Цена входа: <b>~{entry_price:.2f} USDT</b>
Take Profit: <b>{tp:.2f} USDT</b>
Stop Loss: <b>{sl:.2f} USDT</b>

Время: {datetime.now().strftime('%d.%m.%Y %H:%M')}

Поиск: <b>#{hashtag}</b>"""

    await broadcast_message(text)


# ====================== ИСТОРИЯ ======================
async def get_history(limit: int = 30):
    async with aiosqlite.connect(db_path) as db:
        return await db.execute_fetchall("""
            SELECT * FROM signals ORDER BY id DESC LIMIT ?
        """, (limit,))


# ====================== ГЕНЕРАЦИЯ СИГНАЛОВ ======================
async def generate_signals():
    today_str = date.today().isoformat()
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM signals WHERE timestamp LIKE ?", (f"{today_str}%",))
        row = await cursor.fetchone()
        if row[0] >= 3:
            return

    for pair in ["BTC/USDT", "ETH/USDT"]:
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("SELECT timestamp FROM signals WHERE pair = ? ORDER BY id DESC LIMIT 1", (pair,))
            last = await cursor.fetchone()

        if last and (datetime.now() - datetime.fromisoformat(last[0])).total_seconds() < 14400:
            continue

        try:
            exchange = ccxt.binance({'enableRateLimit': True})
            ohlcv = exchange.fetch_ohlcv(pair, '1h', limit=100)
            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            df['sma20'] = df['close'].rolling(20).mean()
            df['sma50'] = df['close'].rolling(50).mean()

            curr = df.iloc[-1]
            prev = df.iloc[-2]
            price = curr['close']

            if pd.isna(curr.get('sma20')) or pd.isna(curr.get('sma50')):
                continue

            direction = None
            if curr['sma20'] > curr['sma50'] and prev['sma20'] <= prev['sma50']:
                direction = "LONG"
                sl = price * 0.985
                tp = price * 1.03
            elif curr['sma20'] < curr['sma50'] and prev['sma20'] >= prev['sma50']:
                direction = "SHORT"
                sl = price * 1.015
                tp = price * 0.97

            if direction:
                await send_signal(pair, direction, price, tp, sl)
        except Exception as e:
            logging.error(f"Ошибка генерации {pair}: {e}")


# ====================== МОНИТОРИНГ TP/SL ======================
async def monitor_open_signals():
    async with aiosqlite.connect(db_path) as db:
        rows = await db.execute_fetchall("""
            SELECT id, pair, direction, tp, sl, hashtag FROM signals WHERE status = 'open'
        """)

    for row in rows:
        signal_id, pair, direction, tp, sl, hashtag = row
        try:
            exchange = ccxt.binance({'enableRateLimit': True})
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
                    await db.execute("UPDATE signals SET status = ?, close_price = ? WHERE id = ?",
                                     (status, current_price, signal_id))
                    await db.commit()

                status_text = "✅ Take Profit" if status == "closed_tp" else "❌ Stop Loss"
                text = f"📢 <b>Сигнал закрыт #{hashtag}</b>\nСтатус: <b>{status_text}</b>\nЦена: <b>{current_price:.2f} USDT</b>"
                await broadcast_message(text)
        except Exception as e:
            logging.error(f"Ошибка мониторинга #{hashtag}: {e}")


# ====================== ХЭНДЛЕРЫ ======================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await add_subscriber(message.from_user)
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📜 История сигналов")],
            [KeyboardButton(text="❌ Отписаться от сигналов")]
        ],
        resize_keyboard=True
    )
    await message.answer(
        f"👋 <b>Привет, {message.from_user.first_name}!</b>\n\n"
        "✅ Ты успешно подписан на торговые сигналы.\n"
        "Используй кнопки ниже.",
        parse_mode="HTML",
        reply_markup=keyboard
    )


@dp.message(F.text == "📜 История сигналов")
async def show_history(message: types.Message):
    history = await get_history()
    if not history:
        await message.answer("Пока нет сигналов.")
        return

    text = "📜 <b>История сигналов</b>\n\n"
    for row in history:
        _, pair, direction, entry, tp, sl, ts, status, close_p, hashtag = row
        emoji = "📈" if direction == "LONG" else "📉"
        st = "🟢 TP" if status == "closed_tp" else "🔴 SL" if status == "closed_sl" else "⏳ Открыт"
        line = f"<b>#{hashtag}</b> {pair} {direction} {emoji}\n"
        line += f"Вход: {entry:.2f} | TP: {tp:.2f} | SL: {sl:.2f}\n"
        if close_p:
            line += f"Закрыто: {close_p:.2f} — {st}\n"
        else:
            line += f"Статус: {st}\n"
        line += f"Время: {ts[:16]}\n\n"
        text += line
    await message.answer(text, parse_mode="HTML")


@dp.message(F.text == "❌ Отписаться от сигналов")
async def unsubscribe(message: types.Message):
    await remove_subscriber(message.from_user.id)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="✅ Подписаться на сигналы")]], resize_keyboard=True)
    await message.answer("❌ Ты отписался.", reply_markup=kb)


@dp.message(F.text == "✅ Подписаться на сигналы")
async def subscribe_again(message: types.Message):
    await add_subscriber(message.from_user)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📜 История сигналов")], [KeyboardButton(text="❌ Отписаться от сигналов")]],
        resize_keyboard=True
    )
    await message.answer("✅ Ты снова подписан!", reply_markup=kb)


@dp.message(Command("test_signal"))
async def test_signal(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Эта команда только для администратора.")
        return
    await message.answer("🧪 Отправляю тестовые сигналы...")
    await send_signal("BTC/USDT", "LONG", 65234.5, 66865.0, 63929.8)
    await message.answer("✅ Тестовые сигналы отправлены.")


# ====================== ЗАПУСК ======================
async def main():
    await init_db()
    logging.basicConfig(level=logging.INFO)

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(generate_signals, "interval", minutes=30)
    scheduler.add_job(monitor_open_signals, "interval", minutes=5)
    scheduler.start()

    print("🚀 Бот успешно запущен! Напиши ему /start в Telegram.")

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен")