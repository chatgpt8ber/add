import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from sqlalchemy import BigInteger, DateTime, Integer, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip()
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

PORT = int(os.getenv("PORT", "8080"))

MIN_MESSAGES = int(os.getenv("MIN_MESSAGES", "5"))
MIN_INTERVAL_SECONDS = int(os.getenv("MIN_INTERVAL_SECONDS", "3600"))
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "15"))

AD_MESSAGE = os.getenv(
    "AD_MESSAGE",
    "سلام\nاین یک تبلیغ تست است.\nhttps://example.com"
).strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing")

if not WEBHOOK_URL:
    if not PUBLIC_URL:
        raise RuntimeError("Set either WEBHOOK_URL or PUBLIC_URL")
    WEBHOOK_URL = PUBLIC_URL.rstrip("/") + WEBHOOK_PATH

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ad-bot")


# ============================================================
# DATABASE
# ============================================================

class Base(DeclarativeBase):
    pass


class GroupState(Base):
    __tablename__ = "group_state"

    chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    message_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_ad_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


# ============================================================
# TELEGRAM
# ============================================================

bot = Bot(
    BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)

dp = Dispatcher()
router = Router()
dp.include_router(router)

send_lock = asyncio.Lock()
checker_task: asyncio.Task | None = None


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables are ready")


async def get_group_state(session: AsyncSession, chat_id: int) -> GroupState | None:
    result = await session.execute(
        select(GroupState).where(GroupState.chat_id == chat_id)
    )
    return result.scalar_one_or_none()


def ad_is_due(state: GroupState) -> bool:
    if state.message_count < MIN_MESSAGES:
        return False

    if state.last_ad_time is None:
        return True

    elapsed = utcnow() - state.last_ad_time
    return elapsed >= timedelta(seconds=MIN_INTERVAL_SECONDS)


async def try_send_ad_for_chat(chat_id: int) -> bool:
    async with send_lock:
        async with SessionLocal() as session:
            state = await get_group_state(session, chat_id)
            if state is None:
                logger.warning("No DB row found for chat_id=%s", chat_id)
                return False

            if not ad_is_due(state):
                return False

            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=AD_MESSAGE,
                    disable_web_page_preview=False,
                )
            except TelegramAPIError:
                logger.exception("Telegram API error while sending ad to chat_id=%s", chat_id)
                return False
            except Exception:
                logger.exception("Unexpected error while sending ad to chat_id=%s", chat_id)
                return False

            state.message_count = 0
            state.last_ad_time = utcnow()
            await session.commit()

            logger.info("Ad sent successfully to chat_id=%s", chat_id)
            return True


async def periodic_checker() -> None:
    while True:
        try:
            async with SessionLocal() as session:
                result = await session.execute(select(GroupState.chat_id))
                chat_ids = [row[0] for row in result.all()]

            for chat_id in chat_ids:
                await try_send_ad_for_chat(chat_id)

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Periodic checker failed")

        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


# ============================================================
# HANDLERS
# ============================================================

@router.message(Command("start"))
async def start_handler(message: Message) -> None:
    await message.answer("ربات فعال است.")


@router.message(F.chat.type.in_({"group", "supergroup"}))
async def group_message_handler(message: Message) -> None:
    if not message.from_user:
        return

    if message.from_user.is_bot:
        return

    async with SessionLocal() as session:
        state = await get_group_state(session, message.chat.id)
        if state is None:
            return

        state.message_count += 1
        await session.commit()

    await try_send_ad_for_chat(message.chat.id)


# ============================================================
# STARTUP / SHUTDOWN
# ============================================================

async def on_startup(app: web.Application) -> None:
    global checker_task

    await init_db()

    await bot.set_webhook(
        url=WEBHOOK_URL,
        secret_token=WEBHOOK_SECRET or None,
        drop_pending_updates=True,
    )
    logger.info("Webhook set to: %s", WEBHOOK_URL)

    checker_task = asyncio.create_task(periodic_checker())
    app["checker_task"] = checker_task


async def on_shutdown(app: web.Application) -> None:
    task = app.get("checker_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    try:
        await bot.delete_webhook(drop_pending_updates=False)
    except Exception:
        logger.exception("Failed to delete webhook on shutdown")

    await bot.session.close()
    await engine.dispose()


# ============================================================
# APP
# ============================================================

def main() -> None:
    app = web.Application()

    SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
        secret_token=WEBHOOK_SECRET or None,
    ).register(app, path=WEBHOOK_PATH)

    setup_application(app, dp, bot=bot)

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()