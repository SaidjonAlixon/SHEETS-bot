from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage
import config

# Bot ob'ektini yaratish
bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

# Xotira saqlash (state'lar uchun)
storage = MemoryStorage()

# Dispatcher ob'ektini yaratish
dp = Dispatcher(storage=storage)
