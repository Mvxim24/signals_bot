import asyncio
import logging
import resource
import os
from datetime import datetime

import pandas as pd
import ccxt
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.session.aiohttp import AiohttpSession
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# ====================== НАСТРОЙКИ ДЛЯ MACOS ======================
try:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (524288, hard))
    print(f"✅ Лимит открытых файлов увеличен")
except Exception:
    pass

# ========================= НАСТРОЙКИ =========================
load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

if not TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в .env файле!")

db_path = "signals.db"

session = AiohttpSession()
bot = Bot(token=TOKEN, session=session)
dp = Dispatcher()

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


# ====================== ИСТОРИЯ ======================
async def get_history(limit: int = 30):
    async with aiosqlite.connect(db_path) as db:
        return await db.execute_fetchall("""
            SELECT * FROM signals ORDER BY id DESC LIMIT ?
        """, (limit,))


# ====================== ОТПРАВКА СИГНАЛА (С ИСПРАВЛЕННЫМ ВРЕМЕНЕМ) ======================
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

    emoji = "📈" if direction == "LONG" else "📉"
    direction_text = "LONG ▲" if direction == "LONG" else "SHORT ▼"

    # Время в UTC — теперь будет совпадать с реальным временем сигнала
    current_time_utc = datetime.now(datetime.UTC).strftime('%d.%m.%Y %H:%M:%S UTC')

    text = f"""🚨 <b>НОВЫЙ ТОРГОВЫЙ СИГНАЛ #{hashtag}</b>

{emoji} <b>{pair}</b> — <b>{direction_text}</b> {emoji}

💰 <b>Цена входа:</b> <code>{entry_price:.2f} USDT</code>
🎯 <b>Take Profit:</b> <code>{tp:.2f} USDT</code>
🛑 <b>Stop Loss:</b> <code>{sl:.2f} USDT</code>

🕒 <b>Время сигнала:</b> {current_time_utc}

🔍 <b>#{hashtag}</b>"""

    await broadcast_message(text)
    print(f"✅ Сигнал отправлен → {pair} | {direction} | {entry_price:.2f}")


# ====================== ГЕНЕРАЦИЯ СИГНАЛОВ (БЕЗ ЛИМИТОВ) ======================
async def generate_signals():
    print(f"\n🔄 [{datetime.now().strftime('%H:%M:%S')}] Запуск генерации сигналов...")

    for pair in ["BTC/USDT", "ETH/USDT"]:
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
                print(f"   ⚠️  Недостаточно данных для {pair}")
                continue

            direction = None
            sl = None
            tp = None

            if curr['sma20'] > curr['sma50'] and prev['sma20'] <= prev['sma50']:
                direction = "LONG"
                sl = round(price * 0.985, 2)
                tp = round(price * 1.03, 2)
            elif curr['sma20'] < curr['sma50'] and prev['sma20'] >= prev['sma50']:
                direction = "SHORT"
                sl = round(price * 1.015, 2)
                tp = round(price * 0.97, 2)

            if direction:
                print(f"   🎯 Найден сигнал: {direction} | {pair} | Цена: {price:.2f}")
                await send_signal(pair, direction, price, tp, sl)
            else:
                print(f"   ❌ Нет сигнала по {pair}")

        except Exception as e:
            logging.error(f"Ошибка генерации {pair}: {e}")
            print(f"   ❌ Ошибка при анализе {pair}: {e}")

    print(f"✅ Генерация сигналов завершена [{datetime.now().strftime('%H:%M:%S')}]\n")


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

                status_text = "✅ TAKE PROFIT" if status == "closed_tp" else "❌ STOP LOSS"
                text = f"""📢 <b>Сигнал закрыт #{hashtag}</b>

{status_text}
Цена закрытия: <b>{current_price:.2f} USDT</b>"""

                await broadcast_message(text)
                print(f"📌 Сигнал закрыт: #{hashtag} → {status_text}")
        except Exception as e:
            logging.error(f"Ошибка мониторинга #{hashtag}: {e}")


# ====================== ХЭНДЛЕРЫ ======================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await add_subscriber(message.from_user)
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📜 История сигналов")],
            [KeyboardButton(text="❌ Отписаться")]
        ],
        resize_keyboard=True
    )
    await message.answer(
        f"👋 <b>Привет, {message.from_user.first_name}!</b>\n\n"
        "✅ Ты подписан на торговые сигналы Alfa Signals.\n"
        "Новые сигналы будут приходить автоматически.",
        parse_mode="HTML",
        reply_markup=keyboard
    )


@dp.message(F.text == "📜 История сигналов")
async def show_history(message: types.Message):
    history = await get_history()
    if not history:
        await message.answer("📭 Пока нет сигналов.")
        return

    text = "📜 <b>История сигналов</b>\n\n"
    for row in history:
        _, pair, direction, entry, tp, sl, ts, status, close_p, hashtag = row
        emoji = "📈" if direction == "LONG" else "📉"
        st = "✅ TP" if status == "closed_tp" else "❌ SL" if status == "closed_sl" else "⏳ Открыт"
        line = f"<b>#{hashtag}</b> {pair} {direction} {emoji}\n"
        line += f"Вход: {entry:.2f} | TP: {tp:.2f} | SL: {sl:.2f}\n"
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
    await message.answer("❌ Ты отписался от сигналов.", reply_markup=kb)


@dp.message(F.text == "✅ Подписаться")
async def subscribe_again(message: types.Message):
    await add_subscriber(message.from_user)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📜 История сигналов")], [KeyboardButton(text="❌ Отписаться")]],
        resize_keyboard=True
    )
    await message.answer("✅ Ты снова подписан на сигналы!", reply_markup=kb)


@dp.message(Command("test_signal"))
async def test_signal(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Команда только для администратора.")
        return
    await message.answer("🧪 Отправляю тестовый сигнал...")
    await send_signal("BTC/USDT", "LONG", 65234.5, 66865.0, 63929.8)


# ====================== ЗАПУСК ======================
async def main():
    await init_db()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    scheduler.add_job(
        generate_signals,
        trigger="interval",
        minutes=10,                    # каждые 10 минут для теста
        id="generate_signals",
        replace_existing=True,
        max_instances=1
    )

    scheduler.add_job(
        monitor_open_signals,
        trigger="interval",
        minutes=5,
        id="monitor_open_signals",
        replace_existing=True,
        max_instances=1
    )

    scheduler.start()
    print("✅ APScheduler запущен (генерация каждые 10 мин, мониторинг каждые 5 мин)")
    print("🚀 Бот успешно запущен!\n")

    try:
        await dp.start_polling(bot)
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=False)
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен")
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
