import asyncio
import asyncpg
import config



class DB_conn():

    def __init__(self) -> None:
        self.tg = asyncio.TaskGroup()
        self.pool =  asyncpg.pool
        self.chatInfoManager = {}
        self.chatActionManager = {}
        self.poolIsCreated = False

    async def main(self):

        self.pool = await self.create_pool()
        async with self.tg:
            self.tg.create_task(self.foreverFunc())
            self.tg.create_task(self.LoadStartBotInfo())

    async def create_pool(self):
        pool = await asyncpg.create_pool(config.conn)
        self.poolIsCreated = True
        return pool
    
    # Вечная функция для бесконечной работы бота
    async def foreverFunc(self):
        while True:
            await asyncio.sleep(60)
    
    #Закрываем пул соединений при завершении работы бота
    async def terminate_pool(self):
        #Если пул соединений уже был создан пытаемся его закрыть. Если его нет  - ничего не дедаем.
        try:
            await self.pool.close()
        except:
            pass

    # Загружем информацию, необходимую для работы бота. 1- Взаимосвьзь чат ID и лицевого счета. 2- Последняя активность пользователя.
    async def LoadStartBotInfo(self):
        sql = '''select * from stage.telebot_info'''
        async with self.pool.acquire() as conn:
            result = await conn.fetch(sql)
            for row in result:
                self.chatInfoManager[row[0]] = row[1]

        sql1 = '''select * from stage.telebot_last_action'''
        async with self.pool.acquire() as conn:
            result = await conn.fetch(sql1)
            for row in result:
                self.chatActionManager[row[0]] = row[1]
            
    # Сохраняем в БД рабочую информацию бота
    async def SaveBotInfo(self, chat_id, customer_id):
        sql = '''insert into stage.telebot_info (chat_id, customer_id)
                values ({}, {})
                on conflict (chat_id)
                DO UPDATE set customer_id = {}
                '''.format(chat_id, customer_id, customer_id)
        async with self.pool.acquire() as conn:
            result = await conn.execute(sql)
            return result

    # Сохраняем информацю о последней активности пользователей в боте    
    async def SaveBotActionInfo(self, chat_id, action):
        sql = '''insert into stage.telebot_last_action (chat_id, action)
                values ({}, {})
                on conflict (chat_id)
                DO UPDATE set action = {}
                '''.format(chat_id, action, action)
        async with self.pool.acquire() as conn:
            result = await conn.execute(sql)
            return result

    # Проверяем номер лицевого счета абонента     
    async def checkCustomerId(self, CustomerId):
        CustomerId = str(CustomerId)
        sql = '''select * from stage.d_customers where id = {}'''.format(CustomerId)
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(sql)
            if (result != None):
                return True
            else: return False

    # Вносим в БД информацию о переданных показаниях
    async def Inser_meter_readings(self, val, CustomerId):  
        val = str(val)
        CustomerId = str(CustomerId)
        sql = '''update stage.readings set meter_readings = {} where customer_id = {}'''.format(val, CustomerId)
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(sql)
                return True
            except:
                return False

    #Получаем информацю р задолженности абонента  
    async def Get_debt_info(self, CustomerId):
        CustomerId = str(CustomerId)
        sql = '''select * from stage.debt where customer_id = {}'''.format(CustomerId)
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchrow(sql)
                ret_result = (True, result)
                return ret_result
            except:
                return False



