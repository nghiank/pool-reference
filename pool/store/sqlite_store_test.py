# =>python -m unittest pool/store/sqlite_store_test.py
# Suggest to have Python > 3.9.5
# Ctl+C to interupt when done

from unittest import IsolatedAsyncioTestCase
import unittest
import sys
import aiosqlite
import asyncio
import os.path
from secrets import token_bytes
from pathlib import Path
from clvm_tools import binutils
from blspy import G1Element, PrivateKey
sys.path.append('..')
from pool.store.sqlite_store import SqlitePoolStore
from pool.record import FarmerRecord
from pool.util import RequestMetadata
from chia.types.coin_solution import CoinSolution
from chia.util.ints import uint64,uint32
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program, SerializedProgram
from chia.pools.pool_wallet_info import PoolState, FARMING_TO_POOL
from chia.util.byte_types import hexstr_to_bytes

PAYOUT_INSTRUCTION = '344587cf06a39db471d2cc027504e8688a0a67cce961253500c956c73603fd58'

def get_table_sql_query(table_name):
    return f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"

class Testdb(IsolatedAsyncioTestCase):


    async def asyncSetUp(self):
        self.store = SqlitePoolStore(Path("test.sqlite"))
        await self.store.connect()
    
    async def asyncTearDown(self):
        cursor = await self.store.connection.execute("delete from farmer")        
        await cursor.close()
        await self.store.connection.commit()
        cursor = await self.store.connection.execute("delete from partial")        
        await cursor.close()
        await self.store.connection.commit()

    async def check_table_exist(self,tablename):
            sql_string = get_table_sql_query(tablename)
            self.store.connection = await aiosqlite.connect(self.store.db_path)
            cursor = await self.store.connection.execute(sql_string)              
            result = await cursor.fetchone()
            return result                
    
    def make_child_solution(self) -> CoinSolution:
        new_puzzle_hash: bytes32 = token_bytes(32)
        solution = "()"
        puzzle = f"(q . ((51 0x{new_puzzle_hash.hex()} 1)))"
        puzzle_prog = Program.to(binutils.assemble(puzzle))
        solution_prog = Program.to(binutils.assemble(solution))
        sol: CoinSolution = CoinSolution(
            Coin(token_bytes(32), token_bytes(32), uint64(12312)),
            SerializedProgram.from_program(puzzle_prog),
            SerializedProgram.from_program(solution_prog),
        )
        return sol

    def make_singleton_tip_state(self) -> PoolState:
        random = 1
        p = PrivateKey.from_bytes(random.to_bytes(32, "big")).get_g1()
        blob = bytes(p)
        target_puzzle_hash = hexstr_to_bytes('344587cf06a39db471d2cc027504e8688a0a67cce961253500c956c73603fd58')
        return PoolState(1, FARMING_TO_POOL, target_puzzle_hash, p, "pool.com", uint32(10))

    def make_farmer_record(self) -> FarmerRecord:
        random=1
        p = PrivateKey.from_bytes(random.to_bytes(32, "big")).get_g1()
        blob = bytes(p)
        authentication_pk = G1Element.from_bytes(blob)
        singleton_tip:CoinSolution = self.make_child_solution()
        singleton_tip_state:PoolState = self.make_singleton_tip_state()
        delay_time:uint64 = 60
        point:uint64 = 10000
        difficulty:uint64 = 2000
        launcher_id = random.to_bytes(32, 'big')
        payout_instruction = '344587cf06a39db471d2cc027504e8688a0a67cce961253500c956c73603fd58'
        return FarmerRecord(
                #launcher_id
                launcher_id,                

                #p2_singleton_puzzle_hash
                random.to_bytes(32, 'big'),

                #delay_time
                delay_time,

                #delay_puzzle_hash,
                random.to_bytes(32, 'big'),

                #authentication_public_key   
                authentication_pk,

                #singleton_tip
                singleton_tip,

                #singleton_tip_state
                singleton_tip_state,

                #points
                point,

                #difficulty
                difficulty,

                #payout_instruction
                PAYOUT_INSTRUCTION,

                #is_pool_member
                True
            )
    def make_request_metadata(self) -> RequestMetadata:
        return RequestMetadata(        
            url='www.chia.com',
            scheme= 'https',
            headers={},
            cookies=dict({}),
            query=dict({}),
            remote='1.1.1.1',
        )

    
    async def test_if_tables_exist(self):     
        is_farmer_table_exist = await self.check_table_exist("farmer")          
        is_partial_table_exist = await self.check_table_exist("partial")
        self.assertTrue(is_farmer_table_exist) 
        self.assertTrue(is_partial_table_exist)

    async def test_add_farmer_record(self):
        farmer_record = self.make_farmer_record()
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)
        res = await self.store.get_farmer_record(farmer_record.launcher_id)
        self.assertEqual(farmer_record, res)

    async def test_update_difficult(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)
        await self.store.update_difficulty(launcher_id, 1234)
        new_farmer = await self.store.get_farmer_record(launcher_id)
        self.assertEqual(new_farmer.difficulty, 1234)

    async def test_update_singleton(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)
        new_singleton_tip:CoinSolution = self.make_child_solution()
        random = 2
        p = PrivateKey.from_bytes(random.to_bytes(32, "big")).get_g1()
        blob = bytes(p)
        target_puzzle_hash = hexstr_to_bytes('344587cf06a39db471d2cc027504e8688a0a67cce961253500c956c73603fd58')
        new_singleton_tip_state = PoolState(2, FARMING_TO_POOL, target_puzzle_hash, p, "chia.com", uint32(10))
        await self.store.update_singleton(launcher_id, new_singleton_tip, new_singleton_tip_state, False)
        new_farmer = await self.store.get_farmer_record(launcher_id)
        self.assertEqual(new_farmer.singleton_tip, new_singleton_tip)
        self.assertEqual(new_farmer.singleton_tip_state, new_singleton_tip_state)
        self.assertFalse(new_farmer.is_pool_member)
    
    async def test_get_pay_to_singleton_phs(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)
        res = await self.store.get_pay_to_singleton_phs()
        random = 1
        self.assertEqual(res, {random.to_bytes(32, "big")})

    async def test_get_farmer_records_for_p2_singleton_phs(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)
        random = 1
        puzzle_hashes = {random.to_bytes(32, "big")}
        res = await self.store.get_farmer_records_for_p2_singleton_phs(puzzle_hashes)
        self.assertEqual(farmer_record, res[0])

    async def test_get_farmer_points_and_payout_instructions(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)
        res = await self.store.get_farmer_points_and_payout_instructions()
        self.assertEqual(res, [(10000, bytes32(bytes.fromhex(PAYOUT_INSTRUCTION)))])
 
    async def test_clear_farmer_points(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)
        await self.store.clear_farmer_points()
        new_farmer = await self.store.get_farmer_record(launcher_id)
        self.assertEqual(new_farmer.points, 0)

    async def test_add_partial(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)
        new_difficulty = 3000
        timestamp:uint64 = 123456
        await self.store.add_partial(launcher_id, timestamp, new_difficulty)
        new_farmer = await self.store.get_farmer_record(launcher_id)
        self.assertEqual(new_farmer.points, 10000 + 3000)
        partial = await self.store.get_recent_partials(launcher_id, 1)
        self.assertEqual(partial[0], (123456, 3000))
    
    async def test_add_many_partial(self):        
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)
        new_difficulty = 3000
        timestamp = 123456
        await self.store.add_partial(launcher_id, timestamp, new_difficulty)
        
        new_difficulty = 4000
        timestamp = 123457
        await self.store.add_partial(launcher_id, timestamp, new_difficulty)
        partial = await self.store.get_recent_partials(launcher_id, 1)
        self.assertEqual(partial[0], (123457,4000))

        partial = await self.store.get_recent_partials(launcher_id, 2)
        self.assertEqual(partial[0], (123457,4000))
        self.assertEqual(partial[1], (123456,3000))





if __name__ == '__main__':
    unittest.main()