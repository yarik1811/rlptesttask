from aiogram.contrib.fsm_storage.memory import MemoryStorage
import aiogram
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram import executor, Dispatcher

import config
import database
from datetime import datetime
import logging
import json


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

bot = aiogram.Bot(token=config.BOT_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())


def parse_datetime_iso(s):
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


@dp.message_handler(commands=['start'])
async def cmd_start(message: aiogram.types.Message):
    await message.answer('Пришлите данные для запроса в формате:\n\n'
                         '```\n{\n'
                         '  "dt_from": "2022-09-01T00:00:00",\n'
                         '  "dt_upto": "2022-12-31T23:59:00",\n'
                         '  "group_type": "month"\n'
                         '}```', parse_mode='Markdown')


@dp.message_handler()
async def incoming_message(message: aiogram.types.Message):
    try:
        input_data_dict = json.loads(message.text)
    except:
        return await message.reply('Недопустимый формат запроса')

    if 'dt_from' not in input_data_dict.keys():
        return await message.reply('Отсутсвует параметр ```dt_from```', parse_mode='Markdown')
    if 'dt_upto' not in input_data_dict.keys():
        return await message.reply('Отсутсвует параметр ```dt_upto```', parse_mode='Markdown')
    if 'group_type' not in input_data_dict.keys():
        return await message.reply('Отсутсвует параметр ```group_type```', parse_mode='Markdown')
    if input_data_dict["group_type"] not in ['month', 'day', 'hour']:
        return await message.reply('Не верный формат параметра ```group_type```', parse_mode='Markdown')

    dt_from = parse_datetime_iso(input_data_dict["dt_from"])
    dt_upto = parse_datetime_iso(input_data_dict["dt_upto"])

    if dt_from is None:
        return await message.reply('Не верный формат параметра ```dt_from```', parse_mode='Markdown')
    if dt_upto is None:
        return await message.reply('Не верный формат параметра ```dt_upto```', parse_mode='Markdown')

    await message.reply('...Ответ...')


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)