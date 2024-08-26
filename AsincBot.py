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


DB_connection = DB_conn()
chatActionManager = DB_connection.chatActionManager
chatInfoManager = DB_connection.chatInfoManager

#chatActionManager = {} # менеджкр состояний чата, ключ - чат ID,
#значения:
# 0 - нет активных опций
# 1 - активна опция Введите номер лицевого счета для идентификации
# 2 - активна опция Передать показания
# 3 - активна опция Получить информацию задолженности

#chatInfoManager = {} # Ключ - чат ID, значение CustomerId


# Включаем логированиу
log = logging.getLogger('TG_bot__log')
logging.basicConfig(level=logging.INFO)

# Объект бота
bot = Bot(config.token)
# Диспетчер
dp = Dispatcher()

# Запуск бота подключения к БД
async def main():    
    async with asyncio.TaskGroup() as tg:
        startBot = tg.create_task(dp.start_polling(bot))
        startDBConn = tg.create_task(DB_connection.main())

# Функция актоматического перезапуска  бота в случае потери соединения с сервером телеграма
def restart_bot():
    #Если в момент активации нет связи с ботом уведомляем консоль и пробуем снова каддые 10 секунд.
    log.info(f"{datetime.datetime.now()}: Нет соединения с ботом. Попытка перезапуска бота через 10 секунд.")

    # Закрываем пул соединений с БД
    DB_connection.tg.create_task(DB_connection.terminate_pool())

    # Перезапускаем файл через 10 секунд
    time.sleep(10)
    python = sys.executable
    os.execl(python, python, *sys.argv)
    

# Хэндлер на команду /start
try:
    @dp.message(Command("start"))
    async def cmd_start(message: types.Message):
        chatActionManager[message.chat.id] = 1
        await message.answer("Введите номер лицевого счета.",reply_markup=types.ReplyKeyboardRemove())
except:
    #Если нет связи с ботом уведомляем консоль и пробуем снова каддые 10 секунд.
    restart_bot()

# Хэндлер на входящие сообщения
try:
    @dp.message()
    async def message_prosessor(message: types.Message):
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

                        # Сохраняем информацию о пользователе в Боте и БД
                        chatActionManager[message.chat.id] = 0
                        chatInfoManager[message.chat.id] = mes
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

            # Сохраняем информацию об активности в Боте и БД
            DB_connection.tg.create_task(DB_connection.SaveBotActionInfo(message.chat.id, 2))
            chatActionManager[message.chat.id] = 2

            kb = [
                [types.KeyboardButton(text="Отказаться от передачи показаний")]
                ]
            keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

            await message.answer("Введите значение", reply_markup=keyboard)
            return
        
        if (message.text == 'Отказаться от передачи показаний' and chatActionManager[message.chat.id] == 2):

            # Сохраняем информацию об активности в Боте и БД
            DB_connection.tg.create_task(DB_connection.SaveBotActionInfo(message.chat.id, 0))
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

                # Сохраняем информацию об активности в Боте и БД
                DB_connection.tg.create_task(DB_connection.SaveBotActionInfo(message.chat.id, 0))
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
                
            # Сохраняем информацию об активности в Боте и БД
            DB_connection.tg.create_task(DB_connection.SaveBotActionInfo(message.chat.id, 3))
            chatActionManager[message.chat.id] = 3

            Cuctomer_id = chatInfoManager[message.chat.id]
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
    #Если нет связи с ботом уведомляем консоль и пробуем снова каддые 10 секунд.
    restart_bot()
    
if __name__ == "__main__":

    try:
        print('Запуск бота.')
        asyncio.run(main())
        print('Бот активирован')
    except:
        #Если в момент активации нет связи с ботом уведомляем консоль и пробуем снова каддые 10 секунд.
        restart_bot()


