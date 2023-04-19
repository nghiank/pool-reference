from pathlib import Path
from typing import Optional, Set, List, Tuple, Dict
from blspy import G1Element
from chia.pools.pool_wallet_info import PoolState
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.coin_spend import CoinSpend
from chia.util.ints import uint64
from bson.binary import Binary
import base64
import pickle

from .abstract import AbstractPoolStore
from ..record import FarmerRecord
from ..util import RequestMetadata

import pymongo
import logging

class MongoDbPoolStore(AbstractPoolStore):
    """
    Pool store based on MongoDb.
    """

    # MongoDB collection name
    FARMER = 'farmer'
    PARTIAL = 'partial'
    AUTH = 'authentication'

    def __init__(self, endpoint='mongodb://127.0.0.1:27017', db_name='pool'):
        super().__init__()
        self.client = pymongo.MongoClient(endpoint, serverSelectionTimeoutMS=8888)                
        self.db_name = db_name
        self.log = logging.getLogger(__name__)
    
    def set_log(self, log):
        self.log = log

    def get_update_result(self, update_result):
        return {
            'acknowledged': update_result.acknowledged,
            'matched_count': update_result.matched_count,
            'modified_count': update_result.modified_count,
            'raw_result': update_result.raw_result,
            'upserted_id': update_result.upserted_id
        }

    def create_farmer(self):
        farmer = self.db[MongoDbPoolStore.FARMER]
        farmer.create_index([("launcher_id", pymongo.DESCENDING)], unique=True)
    
    def create_partial(self):
        partial = self.db[MongoDbPoolStore.PARTIAL]
        partial.create_index([("launcher_id", pymongo.DESCENDING)], unique=True)

    def create_auth(self):
        auth = self.db[MongoDbPoolStore.AUTH]
        auth.create_index([("launcher_id", pymongo.DESCENDING)], unique=True)
    
    async def connect(self): 
        self.db = self.client[self.db_name]          
        if not(MongoDbPoolStore.FARMER in self.db.list_collection_names()):
            self.create_farmer()           
        if not(MongoDbPoolStore.PARTIAL in self.db.list_collection_names()):
            self.create_partial()           
        if not(MongoDbPoolStore.AUTH in self.db.list_collection_names()):
            self.create_auth()           

    @staticmethod
    def _row_to_farmer_record(item) -> FarmerRecord:
        return FarmerRecord(
            bytes.fromhex(item['launcher_id']),
            bytes.fromhex(item['p2_singleton_puzzle_hash']),
            item['delay_time'],
            bytes.fromhex(item['delay_puzzle_hash']),
            G1Element.from_bytes(bytes.fromhex(item['authentication_public_key'])),
            CoinSpend.from_bytes(item['singleton_tip']),
            PoolState.from_bytes(item['singleton_tip_state']),
            item['points'],
            item['difficulty'],
            item['payout_instructions'],
            True if item['is_pool_member'] == 1 else False,
        )

    async def add_farmer_record(self, farmer_record: FarmerRecord, metadata: RequestMetadata):
        farmer_record_json = {
            'launcher_id': farmer_record.launcher_id.hex(),
            'p2_singleton_puzzle_hash': farmer_record.p2_singleton_puzzle_hash.hex(),
            'delay_time': farmer_record.delay_time,
            'delay_puzzle_hash':  farmer_record.delay_puzzle_hash.hex(),
            'authentication_public_key': bytes(farmer_record.authentication_public_key).hex(),
            'singleton_tip': Binary(bytes(farmer_record.singleton_tip), subtype=0),
            'singleton_tip_state': Binary(bytes(farmer_record.singleton_tip_state), subtype=0),
            'points': farmer_record.points,
            'difficulty': farmer_record.difficulty,
            'payout_instructions': farmer_record.payout_instructions,
            'is_pool_member': int(farmer_record.is_pool_member)  
        }
        farmer = self.db[MongoDbPoolStore.FARMER]
        filter = { 'launcher_id':farmer_record.launcher_id.hex() }
        result = farmer.update_one(filter, {'$set': farmer_record_json }, upsert=True)

    async def get_farmer_record(self, launcher_id: bytes32) -> Optional[FarmerRecord]:
        farmer = self.db[MongoDbPoolStore.FARMER]
        response = farmer.find_one({"launcher_id": launcher_id.hex()})
        if not response:
            return None
        return self._row_to_farmer_record(response)

    async def update_difficulty(self, launcher_id: bytes32, difficulty: uint64):
        farmer = self.db[MongoDbPoolStore.FARMER]
        filter = { 'launcher_id': launcher_id.hex() }
        result = farmer.update_one(filter, {'$set': {'difficulty':difficulty} }, upsert=False)

    async def update_singleton(
        self,
        launcher_id: bytes32,
        singleton_tip: CoinSpend,
        singleton_tip_state: PoolState,
        is_pool_member: bool):

        member = 0
        if is_pool_member:
            member=1
        farmer = self.db[MongoDbPoolStore.FARMER]
        filter = { 'launcher_id': launcher_id.hex() }
        updated_obj = {
            'singleton_tip': Binary(bytes(singleton_tip), subtype=0),
            'singleton_tip_state': Binary(bytes(singleton_tip_state), subtype=0),
            'is_pool_member': member
        }
        result = farmer.update_one(filter, {'$set': updated_obj }, upsert=False)

    async def get_pay_to_singleton_phs(self) -> Set[bytes32]:
        # TODO(do we need only get those which has is_pool_member: true??)
        farmer = self.db[MongoDbPoolStore.FARMER]
        all_items = farmer.find()
        all_phs: Set[bytes32] = set() 
        for item in all_items:
            all_phs.add(bytes32(bytes.fromhex(item['p2_singleton_puzzle_hash'])))
        return all_phs

    async def get_farmer_records_for_p2_singleton_phs(self, puzzle_hashes: Set[bytes32]) -> List[FarmerRecord]:
        farmer = self.db[MongoDbPoolStore.FARMER]
        all_items = farmer.find()
        result = []
        for item in all_items:
            p2sph = bytes.fromhex(item['p2_singleton_puzzle_hash'])
            if p2sph in puzzle_hashes:
                result.append(self._row_to_farmer_record(item))
        return result


    async def get_farmer_points_and_payout_instructions(self) -> List[Tuple[uint64, bytes]]:
        farmer = self.db[MongoDbPoolStore.FARMER]
        all_items = farmer.find()
        accumulated: Dict[bytes32, uint64] = {}
        for item in all_items:
            points: uint64 = uint64(item['points'])
            ph: bytes32 = bytes32(bytes.fromhex(item['payout_instructions']))
            if ph in accumulated:
                accumulated[ph] += points
            else:
                accumulated[ph] = points
        
        ret: List[Tuple[uint64, bytes32]] = []
        for ph, total_points in accumulated.items():
            ret.append((total_points, ph))
        return ret

    async def clear_farmer_points(self) -> None:
        farmer = self.db[MongoDbPoolStore.FARMER]
        result = farmer.update_many({}, {'$set': { 'points': 0 }})
        self.log.info(f"Clear point farmer results: {self.get_update_result(result)}")


    async def add_partial(self, launcher_id: bytes32, timestamp: uint64, difficulty: uint64):
        partial = self.db[MongoDbPoolStore.PARTIAL]
        filter = { 'launcher_id': launcher_id.hex(), 'timestamp': timestamp }
        updated_obj = {
            'launcher_id': launcher_id.hex(),
            'timestamp': timestamp,
            'difficulty': difficulty
        }

        result = partial.update_one(filter, {'$set': updated_obj }, upsert=True)
        farmer = self.db[MongoDbPoolStore.FARMER]
        filter = { 'launcher_id': launcher_id.hex()}
        result = farmer.update_one(filter, {
            '$inc': { 'points': difficulty} 
        })
        self.log.info(f"Update point result {launcher_id} difficulty: {difficulty} update_result={self.get_update_result(result)}")

    async def get_recent_partials(self, launcher_id: bytes32, count: int) -> List[Tuple[uint64, uint64]]:
        partial = self.db[MongoDbPoolStore.PARTIAL]
        results = partial.find({'launcher_id': launcher_id.hex()}).limit(count).sort("timestamp", -1)
        
        ret: List[Tuple[uint64, uint64]]  = []
        for item in results:
            ret.append((uint64(item['timestamp']), uint64(item['difficulty'])))
        
        if len(ret) > 0:
            self.log.info(f"{launcher_id}, current difficulty : {ret[0][1]}, partials count : {len(ret)}")        
        return ret

    async def update_login_token(self, launcher_id, login_token, expires):
        auth = self.db[MongoDbPoolStore.AUTH]
        filter = { 'launcher_id': launcher_id.hex() }
        updated_obj = {            
            'login_token': login_token,
            'timestamp': expires
        }
        result = auth.update_one(filter, {'$set': updated_obj }, upsert=True)
        self.log.info(f"Update login token : update_result={self.get_update_result(result)}")

    async def get_farmer_fee(self) -> Dict:
        result = self.db[MongoDbPoolStore.FARMER].aggregate([{
            '$lookup': {
                'from': "farmerMeta",
                'localField': "launcher_id",
                'foreignField': "_id",
                'as': "farmerMeta"
            }
        }])
        ret:Dict = {}
        for item in result:
            ph: bytes32 = bytes32(bytes.fromhex(item['payout_instructions']))
            farmerMeta = item['farmerMeta']
            if len(farmerMeta) > 0:
                if 'fee' in farmerMeta[0]:
                    ret[ph] = farmerMeta[0]['fee']
        return ret
            

