# Start mongodb locally.
# Run test:
# =>python -m unittest pool/pool_server_test.py
# Suggest to have Python > 3.9.5
# Ctl+C to interupt when done
from unittest import IsolatedAsyncioTestCase
from typing import Optional, Set, List, Tuple, Dict
import unittest
import sys
import asyncio
import os.path
import yaml
from secrets import token_bytes
import bson
from pathlib import Path
from clvm_tools import binutils
from blspy import G1Element, G2Element, PrivateKey
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
from .pool import Pool
from .pool_server import PoolServer
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from chia.util.config import load_config
from chia.util.default_root import DEFAULT_ROOT_PATH
from chia.consensus.constants import ConsensusConstants
from chia.consensus.default_constants import DEFAULT_CONSTANTS
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

class PoolServerTest(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_config(DEFAULT_ROOT_PATH, "config.yaml")
        # We load our configurations from here
        with open(os.getcwd() + "/config-example.yaml") as f:
            cls.pool_config: Dict = yaml.safe_load(f)
        cls.overrides = cls.config["network_overrides"]["constants"][cls.config["selected_network"]]
        cls.constants: ConsensusConstants = DEFAULT_CONSTANTS.replace_str_to_bytes(**cls.overrides)
        cls.store = MongoDbPoolStore()
        cls.pool_server = PoolServer(cls.config, cls.constants, cls.store)


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
        
    @patch('pool.pool_server.validate_authentication_token')
    @patch('pool.pool_server.AugSchemeMPL.verify')
    async def test_get_login(self, mock_validate_authentication_token, mock_verify):
        
        farmer_record = self.make_farmer_record()
        metadata = self.make_request_metadata()
        pp = bytes(farmer_record.singleton_tip)        
        await self.store.add_farmer_record(farmer_record, metadata)

        url = ('/login_info?launcher_id=' + farmer_record.launcher_id.hex() + '&authentication_token=123' +
                '&signature=95f0807b7d302aee80928082667ea7cedbe0bd5229e43d791c64c15fbc2aab00deb1a7fb8981239c0aaec32f5dc0e3d305e2ba54d6a6d55122862b4342d368161f45b859a4b989853d2a0bc51483ab2273ab009cdfc0e8df240e581f4c18a3fc')
        req = make_mocked_request('GET', url, headers={'token': 'x'})                

        mock_validate_authentication_token.return_value = True
        mockk_verify = True
        res = await self.pool_server.get_login(request_obj=req)        
        filter = { 'launcher_id': farmer_record.launcher_id.hex() }
        res = self.store.db['authentication'].find(filter)
        self.assertEqual(len(list(res)), 1)
        
    
if __name__ == '__main__':
    unittest.main()