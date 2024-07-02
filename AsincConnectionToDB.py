import asyncio
import asyncpg
import numpy as np
import pandas as pd
import psycopg2
import time
import datetime
import config



class DB_conn():

    def __init__(self) -> None:
        self.tg = asyncio.TaskGroup()
        self.pool =  asyncpg.pool
        self.chatInfoManager = {}
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
    
   # async def terminate_pool():
       # (await get_pool()).terminate()

    async def LoadStartBotInfo(self):
        sql = '''select * from stage.telebot_info'''
        async with self.pool.acquire() as conn:
            result = await conn.fetch(sql)
            for row in result:
                self.chatInfoManager[row[0]] = row[1]
            
        
    async def SaveBotInfo(self, chat_id, customer_id):
        sql = '''insert into stage.telebot_info (chat_id, customer_id)
                values ({}, {})
                on conflict (chat_id)
                DO UPDATE set customer_id = {}
                '''.format(chat_id, customer_id, customer_id)
        async with self.pool.acquire() as conn:
            result = await conn.execute(sql)
            return result
        
    async def SaveBotActionInfo(self, chat_id, action):
        sql = '''insert into stage.telebot_last_action (chat_id, action)
                values ({}, {})
                on conflict (chat_id)
                DO UPDATE set action = {}
                '''.format(chat_id, action, action)
        async with self.pool.acquire() as conn:
            result = await conn.execute(sql)
            return result

    async def foreverFunc(self):
        while True:
            print('Bot still onLine.')
            await asyncio.sleep(60)
        
    async def checkCustomerId(self, CustomerId):
        CustomerId = str(CustomerId)
        sql = '''select * from stage.d_customers where id = {}'''.format(CustomerId)
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(sql)
            #print(result)
            if (result != None):
                return True
            else: return False

    async def Inser_meter_readings(self, val, CustomerId):  
        val = str(val)
        CustomerId = str(CustomerId)
        #day = datetime.date.today()

        sql1 = '''update stage.readings set meter_readings = {} where customer_id = {}'''.format(val, CustomerId)
        #sql2 = '''update stage.readings set date_of_readings = {} where customer_id = {}'''.format(dayObj, CustomerId)
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(sql1)
                return True
            except:
                return False
            
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


        
#qqqq = DB_conn()

#asyncio.run(qqqq.main())


