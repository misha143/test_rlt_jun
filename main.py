import json
import motor.motor_asyncio
from datetime import datetime
from dateutil.relativedelta import relativedelta
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

mongo_client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
db = mongo_client['mydatabase']
collection = db['sample_collection']

API_TOKEN = '6986121625:AAEvk5hQZR0ktHfc9Cyq8yZgQP8H52_XNJQ'
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)


async def aggregate_salary_data_async(json_string):
    data = json.loads(json_string)
    dt_from = datetime.fromisoformat(data["dt_from"])
    dt_upto = datetime.fromisoformat(data["dt_upto"])
    group_type = data["group_type"]

    current_date = dt_from
    end_date = dt_upto

    dataset = []
    labels = []
    if group_type == "hour":
        end_date += relativedelta(seconds=1)
        next_date = min(current_date + relativedelta(hours=1), end_date)
    elif group_type == "day":
        end_date += relativedelta(seconds=1)
        next_date = min(current_date + relativedelta(days=1), end_date)
    elif group_type == "month":
        next_date = min(current_date + relativedelta(months=1), end_date)

    while True:
        print(f"{current_date} = {next_date}")
        if current_date == end_date:
            break


        pipeline = [
            {
                "$match": {
                    "dt": {
                        "$gte": current_date,
                        ("$lt" if next_date != end_date else "$lte"): next_date
                    }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_salary": {
                        "$sum": "$value"
                    }
                }
            }
        ]

        result = await collection.aggregate(pipeline).to_list(None)


        if result:
            dataset.append(result[0]["total_salary"])
        else:
            dataset.append(0)


        labels.append(current_date.isoformat())

        current_date = next_date

        if group_type == "hour":
            next_date = min(current_date + relativedelta(hours=1), end_date)
        elif group_type == "day":
            next_date = min(current_date + relativedelta(days=1), end_date)
        elif group_type == "month":
            next_date = min(current_date + relativedelta(months=1), end_date)

    return json.dumps({"dataset": dataset, "labels": labels})


@dp.message_handler(commands=['start'])
async def on_start(message: types.Message):
    await message.answer("Привет! Это выполненное тестовое задание. Отправь Json")


@dp.message_handler(content_types=types.ContentType.TEXT)
async def process_json_message(message: types.Message):
    try:
        json_string = message.text
        result = await aggregate_salary_data_async(json_string)
        print(result)
        await message.answer(result)
    except Exception as e:
        await message.answer("Ошибка: " + str(e))


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
