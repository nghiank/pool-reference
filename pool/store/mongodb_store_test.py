# Start local DynamoDB: 
# =>java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb
# Run test:
# =>python -m unittest pool/store/dynamo_store_test.py
# Suggest to have Python > 3.9.5
# Ctl+C to interupt when done
from unittest import IsolatedAsyncioTestCase
from typing import Optional, Set, List, Tuple, Dict
import unittest
import sys
import asyncio
import os.path
from secrets import token_bytes
import bson
from pathlib import Path
from clvm_tools import binutils
from blspy import G1Element, PrivateKey
sys.path.append('..')
from pool.store.mongodb_store import MongoDbPoolStore
from pool.record import FarmerRecord
from pool.util import RequestMetadata
from chia.types.coin_spend import CoinSpend
from chia.util.ints import uint64,uint32
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program, SerializedProgram
from chia.pools.pool_wallet_info import PoolState, FARMING_TO_POOL
from chia.util.byte_types import hexstr_to_bytes

PAYOUT_INSTRUCTION = '344587cf06a39db471d2cc027504e8688a0a67cce961253500c956c73603fd58'
ONE=1
TWO=2
THREE=3
ONE_BYTES=ONE.to_bytes(32, 'big')
TWO_BYTES=TWO.to_bytes(32, 'big')
THREE_BYTES=THREE.to_bytes(32, 'big')
LAUNCHER_ID=ONE_BYTES
LAUNCHER_ID2=TWO_BYTES
LAUNCHER_ID3=THREE_BYTES

class MongoDbPoolStoreTest(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = MongoDbPoolStore()

    async def asyncSetUp(self):
        await self.store.connect()

    async def asyncTearDown(self):
        self.store.db[MongoDbPoolStore.FARMER].drop()
        self.store.db[MongoDbPoolStore.PARTIAL].drop()

       
    def make_child_solution(self) -> CoinSpend:
        new_puzzle_hash: bytes32 = token_bytes(32)
        solution = "()"
        puzzle = f"(q . ((51 0x{new_puzzle_hash.hex()} 1)))"
        puzzle_prog = Program.to(binutils.assemble(puzzle))
        solution_prog = Program.to(binutils.assemble(solution))
        sol: CoinSpend = CoinSpend(
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

    def make_farmer_record(self, 
        launcher_id=LAUNCHER_ID, 
        p2_singleton_puzzle_hash=ONE_BYTES,
        delay_time=60, 
        point=10000, difficulty=2000) -> FarmerRecord:

        p = PrivateKey.from_bytes(ONE_BYTES).get_g1()
        blob = bytes(p)
        authentication_pk = G1Element.from_bytes(blob)
        singleton_tip:CoinSpend = self.make_child_solution()
        singleton_tip_state:PoolState = self.make_singleton_tip_state()
        payout_instruction = '344587cf06a39db471d2cc027504e8688a0a67cce961253500c956c73603fd58'
        return FarmerRecord(
                #launcher_id
                launcher_id,                

                #p2_singleton_puzzle_hash
                p2_singleton_puzzle_hash,

                #delay_time
                delay_time,

                #delay_puzzle_hash,
                ONE_BYTES,

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

    async def test_get_unavailable_farmer_record(self):
        farmer_record = self.make_farmer_record()
        metadata = self.make_request_metadata()
        res = await self.store.get_farmer_record(farmer_record.launcher_id)
        self.assertIsNone(res)

    async def test_add_farmer_record(self):
        farmer_record = self.make_farmer_record()
        metadata = self.make_request_metadata()
        pp = bytes(farmer_record.singleton_tip)
        await self.store.add_farmer_record(farmer_record, metadata)

        res = await self.store.get_farmer_record(farmer_record.launcher_id)
        self.assertEqual(farmer_record, res)

    async def test_add_farmer_record_twice(self):
        metadata = self.make_request_metadata()

        farmer_record = self.make_farmer_record()
        await self.store.add_farmer_record(farmer_record, metadata)

        updated_farmer_record = self.make_farmer_record(point=1)
        await self.store.add_farmer_record(updated_farmer_record, metadata)

        res = await self.store.get_farmer_record(farmer_record.launcher_id)
        self.assertEqual(res.points, 1)

    async def test_update_difficult(self):
        metadata = self.make_request_metadata()

        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id        
        await self.store.add_farmer_record(farmer_record, metadata)

        await self.store.update_difficulty(launcher_id, 1234)

        new_farmer = await self.store.get_farmer_record(launcher_id)
        self.assertEqual(new_farmer.difficulty, 1234) 
    
    async def test_update_difficult_for_non_exist_farmer(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        await self.store.update_difficulty(launcher_id, 1234)
        farmer = self.store.db[MongoDbPoolStore.FARMER]
        res = farmer.find_one({"launcher_id": launcher_id.hex()})
        self.assertEqual(res, None)

    async def test_update_singleton(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)
        new_singleton_tip:CoinSpend = self.make_child_solution()
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

    async def test_update_singleton_for_non_exist_farmer(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        new_singleton_tip:CoinSpend = self.make_child_solution()
        random = 2
        p = PrivateKey.from_bytes(random.to_bytes(32, "big")).get_g1()
        blob = bytes(p)
        target_puzzle_hash = hexstr_to_bytes('344587cf06a39db471d2cc027504e8688a0a67cce961253500c956c73603fd58')
        new_singleton_tip_state = PoolState(2, FARMING_TO_POOL, target_puzzle_hash, p, "chia.com", uint32(10))
        await self.store.update_singleton(launcher_id, new_singleton_tip, new_singleton_tip_state, False)

        farmer = self.store.db[MongoDbPoolStore.FARMER]
        res = farmer.find_one({"launcher_id": launcher_id.hex()})
        self.assertEqual(res, None)

    async def test_get_pay_to_singleton_phs(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)

        res = await self.store.get_pay_to_singleton_phs()
        self.assertEqual(res, {ONE_BYTES})

    async def test_get_pay_to_singleton_phs_return_multiples(self):
        metadata = self.make_request_metadata()

        farmer_record = self.make_farmer_record()
        await self.store.add_farmer_record(farmer_record, metadata)

        farmer_record = self.make_farmer_record(launcher_id=TWO_BYTES, p2_singleton_puzzle_hash=TWO_BYTES)
        await self.store.add_farmer_record(farmer_record, metadata)

        res = await self.store.get_pay_to_singleton_phs()
        self.assertEqual(res, {ONE_BYTES, TWO_BYTES})

    async def test_get_pay_to_singleton_phs_duplicate_p2sph(self):
        metadata = self.make_request_metadata()

        farmer_record = self.make_farmer_record(launcher_id=ONE_BYTES, p2_singleton_puzzle_hash=TWO_BYTES)
        await self.store.add_farmer_record(farmer_record, metadata)

        farmer_record = self.make_farmer_record(launcher_id=TWO_BYTES, p2_singleton_puzzle_hash=TWO_BYTES)
        await self.store.add_farmer_record(farmer_record, metadata)

        res = await self.store.get_pay_to_singleton_phs()
        self.assertEqual(res, {TWO_BYTES})

    async def test_get_farmer_records_for_p2_singleton_phs(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)
        puzzle_hashes: Set[bytes32] = set()
        puzzle_hashes.add(ONE_BYTES)
        res = await self.store.get_farmer_records_for_p2_singleton_phs(puzzle_hashes)
        self.assertEqual(farmer_record, res[0])
    
    async def test_get_farmer_records_for_multiple_p2_singleton_phs(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)

        farmer_record2 = self.make_farmer_record(launcher_id=LAUNCHER_ID2 ,p2_singleton_puzzle_hash=TWO_BYTES)
        await self.store.add_farmer_record(farmer_record2, metadata)

        farmer_record3 = self.make_farmer_record(launcher_id=LAUNCHER_ID3 ,p2_singleton_puzzle_hash=THREE_BYTES)
        await self.store.add_farmer_record(farmer_record3, metadata)

        puzzle_hashes: Set[bytes32] = set()
        puzzle_hashes.add(ONE_BYTES)
        puzzle_hashes.add(TWO_BYTES)
        res = await self.store.get_farmer_records_for_p2_singleton_phs(puzzle_hashes)
        self.assertEqual(2, len(res))
        self.assertEqual(farmer_record, res[0])
        self.assertEqual(farmer_record2, res[1])
        
    async def test_get_farmer_points_and_payout_instructions(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)
        res = await self.store.get_farmer_points_and_payout_instructions()
        self.assertEqual(res, [(10000, bytes32(bytes.fromhex(PAYOUT_INSTRUCTION)))])

    async def test_get_farmer_points_and_multple_payout_instructions(self):
        metadata = self.make_request_metadata()

        farmer_record = self.make_farmer_record()
        await self.store.add_farmer_record(farmer_record, metadata)

        farmer_record2 = self.make_farmer_record(launcher_id=LAUNCHER_ID2 ,p2_singleton_puzzle_hash=TWO_BYTES)
        await self.store.add_farmer_record(farmer_record2, metadata)

        res = await self.store.get_farmer_points_and_payout_instructions()
        self.assertEqual(res, [(10000*2, bytes32(bytes.fromhex(PAYOUT_INSTRUCTION)))])
    
    async def test_clear_farmer_points(self):
        metadata = self.make_request_metadata()

        farmer_record = self.make_farmer_record()
        await self.store.add_farmer_record(farmer_record, metadata)

        farmer_record2 = self.make_farmer_record(launcher_id=LAUNCHER_ID2, p2_singleton_puzzle_hash=TWO_BYTES)
        await self.store.add_farmer_record(farmer_record2, metadata)

        await self.store.clear_farmer_points()

        farmer = await self.store.get_farmer_record(LAUNCHER_ID)
        self.assertEqual(farmer.points, 0)
        farmer2 = await self.store.get_farmer_record(LAUNCHER_ID2)
        self.assertEqual(farmer2.points, 0)

    async def test_add_partial(self):
        farmer_record = self.make_farmer_record()
        launcher_id = farmer_record.launcher_id
        metadata = self.make_request_metadata()
        await self.store.add_farmer_record(farmer_record, metadata)
        new_difficulty = 3000
        timestamp:uint64 = 123456
        await self.store.add_partial(launcher_id, timestamp, new_difficulty)
        new_farmer = await self.store.get_farmer_record(launcher_id)
        self.assertEqual(new_farmer.points, 10000 + new_difficulty)

        await self.store.add_partial(launcher_id, timestamp+1, new_difficulty+1)
        partial = await self.store.get_recent_partials(launcher_id, 1)
        self.assertEqual(partial[0], (123457,3001))

        await self.store.add_partial(launcher_id, timestamp+2, new_difficulty+2)
        partial = await self.store.get_recent_partials(launcher_id, 2)
        self.assertEqual(partial[0], (123458,3002))
        self.assertEqual(partial[1], (123457,3001))

if __name__ == '__main__':
    unittest.main()