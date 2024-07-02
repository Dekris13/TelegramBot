
import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command
from aiogram import F
import datetime
import time
from AsincConnectionToDB import DB_conn
import config
from os import system
import sys
import os


chatActionManager = {} # менеджкр состояний чата, ключ - чат ID,
#значения:
# 0 - нет активных опций
# 1 - активна опция Введите номер лицевого счета для идентификации
# 2 - активна опция Передать показания
# 3 - активна опция Получить информацию задолженности

chatInfoManager = {} # Ключ - чат ID, значение CustomerId
DB_connection = DB_conn()


# Включаем логирование, чтобы не пропустить важные сообщения
logging.basicConfig(level=logging.INFO)

# Объект бота
bot = Bot(token=config.token)
# Диспетчер
dp = Dispatcher()



# Хэндлер на команду /start
try:
    @dp.message(Command("start"))
    async def cmd_start(message: types.Message):
        chatActionManager[message.chat.id] = 1
        await message.answer("Введите номер лицевого счета.",reply_markup=types.ReplyKeyboardRemove())
except:
    #Если в момент активации нет связи с ботом уведомляем консоль и пробуем снова каддые 5 секунд.
    print('Нет соединения с ботом. Попытка активации через 1 секундe. В процессе')
    time.sleep(1)
    python = sys.executable
    os.execl(python, python, *sys.argv)

# Хэндлер на входящие сообщения
try:
    @dp.message()
    async def check_id(message: types.Message):
        mes = None

        # Блок операций по идентификации пользователя
        if (message.text not in ['Передать показания', 'Получить информацию о задолженности']):
            if (chatActionManager[message.chat.id] == 1):
                try:
                    mes = int(message.text)
                except:
                    await message.answer("Введите корректный номер лицевого счета. Лицевой счет - это номер Вашего договора теплоснабжения и содержит только цифры.")

                # Проверка номера лицевого счета в БД
                if (mes != None ):
                    if (await DB_connection.tg.create_task(DB_connection.checkCustomerId(mes))):

                        chatActionManager[message.chat.id] = 0

                        # Сохраняем информацию о пользователе в Боте и БД
                        DB_connection.chatInfoManager[message.chat.id] = mes
                        DB_connection.tg.create_task(DB_connection.SaveBotInfo(message.chat.id, mes))

                        kb = [
                        [types.KeyboardButton(text="Передать показания")],
                        [types.KeyboardButton(text="Получить информацию о задолженности")]
                        ]
                        keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

                        await message.answer("Номер лицевого счета верен. Выберите дальнейшие действия.",reply_markup=keyboard)
                    else:
                        await message.answer("Номер лицевого счета не найден. Проверь правильность набора номера или обратить в службу поддержки.")



        # Блок операций по передаче показаний счетчика
        if (message.text == 'Передать показания'):
            chatActionManager[message.chat.id] = 2

            kb = [
                [types.KeyboardButton(text="Отказаться от передачи показаний")]
                ]
            keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

            await message.answer("Введите значение", reply_markup=keyboard)
            return
        
        if (message.text == 'Отказаться от передачи показаний' and chatActionManager[message.chat.id] == 2):
            chatActionManager[message.chat.id] = 0
            kb = [
                [types.KeyboardButton(text="Передать показания")],
                [types.KeyboardButton(text="Получить информацию о задолженности")]
                ]
            keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
            await message.answer("Выберите дальнейшие действия.",reply_markup=keyboard)
            return

        if (chatActionManager[message.chat.id] == 2):
            try:
                mes = float(message.text)
            except:
                await message.answer("Не корректный ввод показаний прибора учета.")

            if (mes != None):
                #Получаем Cuctomer_id на основе Chat.Id
                Cuctomer_id = DB_connection.chatInfoManager[message.chat.id]
                result = await DB_connection.tg.create_task(DB_connection.Inser_meter_readings(mes, Cuctomer_id))
                chatActionManager[message.chat.id] = 0
                if (result):
                    kb = [
                    [types.KeyboardButton(text="Передать показания")],
                    [types.KeyboardButton(text="Получить информацию о задолженности")]
                    ]
                    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
                    await message.answer("Ваши показания приняты.", reply_markup=keyboard)
                else:
                    await message.answer("Произошла ошибка. Попробуйте позднее или обратитесь в службу поддежки.", reply_markup=keyboard)
                return
        


        # Блок операцйи по получению информации о текущей задолженности
        if (message.text == 'Получить информацию о задолженности'):
            chatActionManager[message.chat.id] = 3
            Cuctomer_id = DB_connection.chatInfoManager[message.chat.id]
            day = datetime.date.today()

            result = await DB_connection.tg.create_task(DB_connection.Get_debt_info(Cuctomer_id))

            if (result[0]):
                main_debt = result[1][1]
                penalty_fee = result[1][2]
                if (main_debt == 0 and penalty_fee == 0):
                    answer = f"По состоянию на {day}  задолженность отсутствует."
                else:
                    answer = f"По стостоянию на {day} Ваша задолженность за теплоснабжение составляет {main_debt} рублей."

                if (penalty_fee > 0):
                    answer = answer + ' ' + f'Пеня {penalty_fee} рублей.'
                await message.answer(answer)
            else:
                await message.answer("Произошла ошибка. Попробуйте позднее или обратитесь в службу поддежки.")
except:
    #Если в момент активации нет связи с ботом уведомляем консоль и пробуем снова каддые 5 секунд.
        print('Нет соединения с ботом. Попытка активации через 1 секунду. В процессе.')
        time.sleep(1)
        python = sys.executable
        os.execl(python, python, *sys.argv)



# Запуск бота подключения к БД
async def main():    
    async with asyncio.TaskGroup() as tg:
        startBot = tg.create_task(dp.start_polling(bot))
        startDBConn = tg.create_task(DB_connection.main())
    


if __name__ == "__main__":

    try:
        print('Запуск бота.')
        asyncio.run(main())
        print('Бот активирован')
    except:
        #Если в момент активации нет связи с ботом уведомляем консоль и пробуем снова каддые 5 секунд.
        print('Нет соединения с ботом. Попытка активации через 5 секунд. На Старте')
        time.sleep(1)
        python = sys.executable
        os.execl(python, python, *sys.argv)


