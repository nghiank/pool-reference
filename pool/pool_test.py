#Start mongoDB locally
# Run test:
# =>python -m unittest pool/pool_test.py
# Suggest to have Python > 3.9.5
# Ctl+C to interupt when done
from unittest import IsolatedAsyncioTestCase
from typing import Optional, Set, List, Tuple, Dict
from unittest.mock import AsyncMock
import unittest
import sys
import asyncio
import os.path
import yaml
from secrets import token_bytes
from pathlib import Path
from clvm_tools import binutils
from blspy import G1Element, G2Element, PrivateKey
sys.path.append('..')
from pool.store.mongodb_store import MongoDbPoolStore
from chia.rpc.full_node_rpc_client import FullNodeRpcClient
from pool.record import FarmerRecord
from pool.util import RequestMetadata
from chia.types.coin_spend import CoinSpend
from chia.util.ints import uint64,uint32
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program, SerializedProgram
from chia.pools.pool_wallet_info import PoolState, FARMING_TO_POOL
from chia.util.byte_types import hexstr_to_bytes
from .pool import Pool
from chia.util.config import load_config
from chia.util.default_root import DEFAULT_ROOT_PATH
from chia.consensus.constants import ConsensusConstants
from chia.consensus.default_constants import DEFAULT_CONSTANTS
from chia.types.coin_record import CoinRecord
from chia.protocols.pool_protocol import (
    PoolErrorCode,
    PostPartialRequest,
    PostPartialResponse,
    PostFarmerRequest,
    PostFarmerResponse,
    PutFarmerPayload,
    PutFarmerRequest,
    PutFarmerResponse,
    POOL_PROTOCOL_VERSION,
    get_current_authentication_token,
)
from chia.wallet.derive_keys import (
    master_sk_to_pooling_authentication_sk,
    find_owner_sk,
)
from blspy import AugSchemeMPL, G1Element
from unittest.mock import patch


PAYOUT_INSTRUCTION = '344587cf06a39db471d2cc027504e8688a0a67cce961253500c956c73603fd58'
PAYOUT_INSTRUCTION2 = '9123451111111111111111111111111111111111111111111111111111111111'
ONE=1
TWO=2
THREE=3
ONE_BYTES=ONE.to_bytes(32, 'big')
TWO_BYTES=TWO.to_bytes(32, 'big')
THREE_BYTES=THREE.to_bytes(32, 'big')
LAUNCHER_ID=ONE_BYTES
LAUNCHER_ID2=TWO_BYTES
LAUNCHER_ID3=THREE_BYTES

# class AsyncMock(MagicMock):
#     async def __call__(self, *args, **kwargs):
#         return super(AsyncMock, self).__call__(*args, **kwargs)

class PoolTest(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):

        cls.config = load_config(DEFAULT_ROOT_PATH, "config.yaml")
        # We load our configurations from here
        with open(os.getcwd() + "/config-example.yaml") as f:
            cls.pool_config: Dict = yaml.safe_load(f)
        cls.overrides = cls.config["network_overrides"]["constants"][cls.config["selected_network"]]
        cls.constants: ConsensusConstants = DEFAULT_CONSTANTS.replace_str_to_bytes(**cls.overrides)
        cls.store = MongoDbPoolStore()

    async def asyncSetUp(self):
        await self.store.connect()
        self.pool = Pool(self.config, self.pool_config, self.constants, self.store)

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
        point=10000, difficulty=2000,
        authentication_pk_seed = ONE_BYTES,
        payout_instruction = PAYOUT_INSTRUCTION
        ) -> FarmerRecord:

        p = PrivateKey.from_bytes(authentication_pk_seed).get_g1()
        blob = bytes(p)
        authentication_pk = G1Element.from_bytes(blob)
        singleton_tip:CoinSpend = self.make_child_solution()
        singleton_tip_state:PoolState = self.make_singleton_tip_state()
        payout_instruction = '344587cf06a39db471d2cc027504e8688a0a67cce961253500c956c73603fd58'
        return FarmerRecord(
                #launcher_id
                bytes32(bytes.fromhex("78aec4d523b0bea49829a1322d5de92a86a553ce8774690b8c8ad5fc1f7540a8")),                

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
                payout_instruction,

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
            remote='1.1.1.1')
    @unittest.skip("reason for skipping")
    @patch.object(Pool, 'get_and_validate_singleton_state')
    async def test_update_farmer_record(self, mock_get_and_validate_singleton_state):
        farmer_record = self.make_farmer_record()
        metadata = self.make_request_metadata()
        pp = bytes(farmer_record.singleton_tip)        
        await self.store.add_farmer_record(farmer_record, metadata)

        authentication_public_key = G1Element.from_bytes(hexstr_to_bytes("0x875e321d6c564fe8aa594f47d2b41cb26319d500de9e2a49c42c42fd82b18243356b6e7aa8b2e259df9079eb834bbe03")) 
        put_farmer_payload = PutFarmerPayload(
            bytes32(bytes.fromhex("78aec4d523b0bea49829a1322d5de92a86a553ce8774690b8c8ad5fc1f7540a8")),
            uint64(15049374353843709257),
            farmer_record.authentication_public_key,
            "0xc2b08e41d766da4116e388357ed957d04ad754623a915f3fd65188a8746cf3e8",
            uint64(201241879360854600),
        )
        owner_sk = PrivateKey.from_bytes(ONE_BYTES)
        signature: G2Element = AugSchemeMPL.sign(owner_sk, put_farmer_payload.get_hash())
        put_farmer_request = PutFarmerRequest(put_farmer_payload, signature)
        put_farmer_request = PutFarmerRequest(
            put_farmer_payload,
            signature
        )
        self.pool.farmer_update_cooldown_seconds = 4
        mock_get_and_validate_singleton_state.return_value = (self.make_child_solution(), self.make_singleton_tip_state(), True)

        # Start to update farmer.
        put_farmer_response = await self.pool.update_farmer(put_farmer_request, self.make_request_metadata())
        ret_tasks = [
            t for t in asyncio.all_tasks() if t is not asyncio.current_task()
        ]        
        assert 1 == len(ret_tasks)

        # Try to update the farmer point while the update_farmer task is still 
        # in progress - Simulate the real scenarios
        # async with self.store.lock:            
        #     new_point = 99999        
        #     new_difficulty = 99
        #     latest_farmer_record = self.make_farmer_record(
        #         bytes32(bytes.fromhex("78aec4d523b0bea49829a1322d5de92a86a553ce8774690b8c8ad5fc1f7540a8")), 
        #         ONE_BYTES,
        #         delay_time=60, 
        #         point=new_point, difficulty = new_difficulty)         
        #     await self.store.add_farmer_record(latest_farmer_record, metadata)

        # Wait for update_farmer task to be completed.
        await asyncio.gather(*ret_tasks)
        
        # Verify the latest farmer info.
        #final_farmer = await self.store.get_farmer_record(farmer_record.launcher_id)
        #self.assertEqual(new_point, final_farmer.points)        
        #self.assertEqual(2000, final_farmer.difficulty)  
        #self.assertEqual(new_authentication_pk, final_farmer.authentication_public_key)
        #self.assertEqual(PAYOUT_INSTRUCTION2, final_farmer.payout_instructions)
    
    @patch('pool.pool.asyncio.sleep', new_callable=AsyncMock)
    async def test_create_payment_loop(self, mock_sleep):
        self.pool.blockchain_state = {
            "sync" : {
                "synced": True
            }
        }
        self.pool.pending_payments = asyncio.Queue()
        coin_records: List[CoinRecord] = []
        coin_records.append(CoinRecord(Coin(ONE_BYTES, ONE_BYTES, 1750000000000), 0, 0, False, False, 0))
        print(f"Len = {len(coin_records)}")
        self.pool.node_rpc_client = AsyncMock(FullNodeRpcClient)

        f = asyncio.Future()
        f.set_result(coin_records)
        self.pool.node_rpc_client.get_coin_records_by_puzzle_hash = AsyncMock(return_value=coin_records)

        points_and_ph: List[
            Tuple[uint64, bytes]
        ] = []
        points_and_ph.append((10, ONE_BYTES))
        points_and_ph.append((20, TWO_BYTES))
        points_and_ph.append((30, THREE_BYTES))
        self.pool.store.get_farmer_points_and_payout_instructions = AsyncMock(return_value=points_and_ph)

        ph_fee : Dict = {}
        ph_fee[ONE_BYTES] = 0.01
        ph_fee[TWO_BYTES] = 0.01
        ph_fee[THREE_BYTES] = 0.01
        self.pool.store.get_farmer_fee = AsyncMock(return_value=ph_fee)

        await self.pool.create_payment()
        self.assertEqual(self.pool.pending_payments.qsize(), 1)   
        payment_targets = await self.pool.pending_payments.get()
                                                   #1750000000000 
        self.assertEqual(payment_targets[0]['amount'], 288749999994)
        self.assertEqual(payment_targets[1]['amount'], 577499999987)
        self.assertEqual(payment_targets[2]['amount'], 866249999981)
        self.assertEqual(payment_targets[3]['amount'], 17499999998)
                                                       
    @patch('pool.pool.asyncio.sleep', new_callable=AsyncMock)
    async def test_create_payment_loop_different_fee(self, mock_sleep):
        self.pool.blockchain_state = {
            "sync" : {
                "synced": True
            }
        }
        self.pool.pending_payments = asyncio.Queue()
        coin_records: List[CoinRecord] = []
        coin_records.append(CoinRecord(Coin(ONE_BYTES, ONE_BYTES, 1750000000000), 0, 0, False, False, 0))
        print(f"Len = {len(coin_records)}")
        self.pool.node_rpc_client = AsyncMock(FullNodeRpcClient)

        f = asyncio.Future()
        f.set_result(coin_records)
        self.pool.node_rpc_client.get_coin_records_by_puzzle_hash = AsyncMock(return_value=coin_records)

        points_and_ph: List[
            Tuple[uint64, bytes]
        ] = []
        points_and_ph.append((10, ONE_BYTES))
        points_and_ph.append((20, TWO_BYTES))
        points_and_ph.append((30, THREE_BYTES))
        self.pool.store.get_farmer_points_and_payout_instructions = AsyncMock(return_value=points_and_ph)

        ph_fee : Dict = {}
        ph_fee[ONE_BYTES] = 0.02
        ph_fee[TWO_BYTES] = 0.01
        ph_fee[THREE_BYTES] = 0.01
        self.pool.store.get_farmer_fee = AsyncMock(return_value=ph_fee)

        await self.pool.create_payment()
        self.assertEqual(self.pool.pending_payments.qsize(), 1)   
        payment_targets = await self.pool.pending_payments.get()
                                                   #1750000000000 
        self.assertEqual(payment_targets[0]['amount'], 285833333327)
        self.assertEqual(payment_targets[1]['amount'], 577499999987)
        self.assertEqual(payment_targets[2]['amount'], 866249999981)
        self.assertEqual(payment_targets[3]['amount'], 20416666665)
    
if __name__ == '__main__':
    unittest.main()