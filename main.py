import logging

from pymongo import MongoClient

from aiogram import Bot, Dispatcher, executor, types
from config import TOKEN

from config import USER, PASSWORD, CLUSTER

from func import Aggregate


# Подключение к MongoDB Atlas
uri = f"mongodb+srv://{USER}:{PASSWORD}@{CLUSTER}?retryWrites=true&w=majority"

client = MongoClient(uri)

db = client.sampleDB
coll = db.sample_collection


# Подключение Телеграм бота
logging.basicConfig(level=logging.INFO)

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)


@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    """
    This handler will be called when user sends '/start' command
    """
    user = types.User.get_current()
    await message.answer(f"Привет, {user.first_name}\nВариант решения тестового задания")


@dp.message_handler(commands=['help'])
async def send_info(message: types.Message):
    """
    This handler will be called when user sends '/help' command
    """
    await message.reply(
        'Пример входных данных:\n{"dt_from":"2022-09-01T00:00:00","dt_upto":"2022-12-31T23:59:00","group_type":"month"}'
    )


@dp.message_handler()
async def get_dataset(message: types.Message):
    """
    This handler will answer on user request
    """
    values = eval(message.text)
    dt_from = values.get("dt_from")
    dt_upto = values.get("dt_upto")
    group_type = values.get("group_type")

    if group_type == "month":
        data = Aggregate().get_dataset_month(coll, dt_from, dt_upto)
    elif group_type == "day":
        data = Aggregate().get_dataset_day(coll, dt_from, dt_upto)
    else:
        data = "group_type=hour"

    await message.answer(data)


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
