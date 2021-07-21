# run cmd: python -m test_db
# running async because of database
# ctl+c to interupt when done

import unittest
import sys
from pool.store.sqlite_store import SqlitePoolStore
import aiosqlite
import asyncio
import os.path

def check_table_sql_string(table_name):
    return f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"

class Testdb(unittest.TestCase):
    def setUp(self):
        self.store = SqlitePoolStore()

    def test_db_file_exist(self):         

        self.assertEqual(os.path.isfile('pooldb.sqlite') ,True)
    def test_if_tables_exist(self):
        async def check_table_exist(self,tablename):
            sql_string = check_table_sql_string(tablename)
            self.store.connection = await aiosqlite.connect(self.store.db_path)
            await self.store.connection.execute("pragma journal_mode=wal")
            await self.store.connection.execute("pragma synchronous=2")
            cursor = await self.store.connection.execute(sql_string)              
            result = await cursor.fetchone()
            if (result):
                return result[0]
            else:
                return "Not found"
        
        self.assertEqual(asyncio.run(check_table_exist(self,"farmer")),"farmer") #check if table farmer exist
        self.assertEqual(asyncio.run(check_table_exist(self,"partial")),"partial") #check if table partial exist
        # check if columns exists ????
    
    def test_get_farmer_record(self):
        async def insert_test_farmer_record(self,farmer_record):
            self.store.connection = await aiosqlite.connect(self.store.db_path)
            await self.store.connection.execute("pragma journal_mode=wal")
            await self.store.connection.execute("pragma synchronous=2")
            async with self.store.lock: 
                cursor = await self.store.connection.execute(
                    f"INSERT OR REPLACE INTO farmer VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        farmer_record['launcherid'],
                        farmer_record['p2_singleton_puzzle_hash'],
                        farmer_record['delay_time'],
                        farmer_record['delay_puzzle_hash'],
                        (farmer_record['authentication_public_key']).encode('utf_8'),
                        (farmer_record['singleton_tip']).encode('utf_8'),
                        (farmer_record['singleton_tip_state']).encode('utf_8'),
                        farmer_record['points'],
                        farmer_record['difficulty'],
                        farmer_record['payout_instructions'],
                        (farmer_record['is_pool_member']),
                    )
                )  
                await cursor.close()
                await self.connection.commit()
                await self.store.connection.commit()  

        #see table schema in sqlite_store.py
        data = {
            "launcherid":'test launcherid',
            "p2_singleton_puzzle_hash":'test p2_singleton_puzzle_hash',
            "delay_time":1000,
            "delay_puzzle_hash":'test delay_puzzle_hash',
            "authentication_public_key":'test authentication_public_key',
            "singleton_tip":'test singleton_tip',
            "singleton_tip_state":'test singleton_tip_state',
            "points":999,           
            "difficulty":888,
            "payout_instructions":'test payout_instructions',
            "is_pool_member":True
        }
        asyncio.run(insert_test_farmer_record(self,data))
        res = self.store.get_farmer_record(self,data['launcherid'])
        print(res)

if __name__ == '__main__':
    unittest.main()