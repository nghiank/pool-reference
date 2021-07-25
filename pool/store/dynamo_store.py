import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import Binary
from pathlib import Path
from typing import Optional, Set, List, Tuple, Dict
from blspy import G1Element
from chia.pools.pool_wallet_info import PoolState
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.coin_spend import CoinSpend
from chia.util.ints import uint64

from .abstract import AbstractPoolStore
from ..record import FarmerRecord
from ..util import RequestMetadata

class DynamoPoolStore(AbstractPoolStore):
    """
    Pool store based on SQLite.
    """

    def __init__(self, endpoint="http://localhost:8000"):
        super().__init__()
        # Get the service resource.
        self.client = boto3.client('dynamodb', endpoint_url=endpoint)
        self.dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint)

    def get_farmer_table(self):
       return self.dynamodb.Table('farmer') 

    def get_partial_table(self):
       return self.dynamodb.Table('partial')

    def create_farmer_table(self):
        table = self.dynamodb.create_table(
            TableName='farmer',
            KeySchema=[
                {
                    'AttributeName': 'launcher_id',
                    'KeyType': 'HASH'
                }                
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'launcher_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'p2_singleton_puzzle_hash',
                    'AttributeType': 'S'
                }
            ],
            # TODO(nghia): update this
            ProvisionedThroughput={
                'ReadCapacityUnits': 20,
                'WriteCapacityUnits': 20 
            },
            GlobalSecondaryIndexes=[{
                "IndexName": "p2_singleton_puzzle_hashIndex",
                "KeySchema": [
                    {
                        "AttributeName": "p2_singleton_puzzle_hash",
                        "KeyType": "HASH"
                    }
                ],
                "Projection": {
                    "ProjectionType": "ALL"
                },
                # TODO(nghiaround): update this
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 20,
                    "WriteCapacityUnits": 20,
                }
            }],
        )

        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName='farmer')

    def create_partial_table(self):
        table = self.dynamodb.create_table(
            TableName='partial',
            KeySchema=[
                {
                    'AttributeName': 'launcher_id',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'timestamp',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'launcher_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'timestamp',
                    'AttributeType': 'N'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 20,
                'WriteCapacityUnits': 20
            }
        )

        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName='partial')

    async def connect(self):       
        existing_tables = self.client.list_tables()['TableNames']
        if 'farmer' not in existing_tables:
            self.create_farmer_table()
        if 'partial' not in existing_tables:
            self.create_partial_table()

    @staticmethod
    def _row_to_farmer_record(item) -> FarmerRecord:
        return FarmerRecord(
            bytes.fromhex(item['launcher_id']),
            bytes.fromhex(item['p2_singleton_puzzle_hash']),
            item['delay_time'],
            bytes.fromhex(item['delay_puzzle_hash']),
            G1Element.from_bytes(bytes.fromhex(item['authentication_public_key'])),
            CoinSpend.from_bytes(item['singleton_tip'].value),
            PoolState.from_bytes(item['singleton_tip_state'].value),
            item['points'],
            item['difficulty'],
            item['payout_instructions'],
            True if item['is_pool_member'] == 1 else False,
        )

    async def add_farmer_record(self, farmer_record: FarmerRecord, metadata: RequestMetadata):
        table = self.get_farmer_table()
        response = table.put_item(
            Item={
            'launcher_id': farmer_record.launcher_id.hex(),
            'p2_singleton_puzzle_hash': farmer_record.p2_singleton_puzzle_hash.hex(),
            'delay_time': farmer_record.delay_time,
            'delay_puzzle_hash':  farmer_record.delay_puzzle_hash.hex(),
            'authentication_public_key': bytes(farmer_record.authentication_public_key).hex(),
            'singleton_tip': Binary(bytes(farmer_record.singleton_tip)),
            'singleton_tip_state': Binary(bytes(farmer_record.singleton_tip_state)),
            'points': farmer_record.points,
            'difficulty': farmer_record.difficulty,
            'payout_instructions': farmer_record.payout_instructions,
            'is_pool_member': int(farmer_record.is_pool_member)  
           }
        )        

    async def get_farmer_record(self, launcher_id: bytes32) -> Optional[FarmerRecord]:
        table = self.get_farmer_table()
        response = table.get_item(
            Key={
                'launcher_id': launcher_id.hex()
            })
        if response is None or 'Item' not in response:
            return None
        return self._row_to_farmer_record(response['Item'])

    async def update_difficulty(self, launcher_id: bytes32, difficulty: uint64):
        table = self.get_farmer_table()
        response = table.update_item(
            Key={
                'launcher_id': launcher_id.hex(),
            },
            UpdateExpression="set difficulty=:d",
            ExpressionAttributeValues={
                ':d': difficulty,
            },
            ReturnValues="UPDATED_NEW"
        )
        return response

    async def update_singleton(
        self,
        launcher_id: bytes32,
        singleton_tip: CoinSpend,
        singleton_tip_state: PoolState,
        is_pool_member: bool):
        table = self.get_farmer_table()
        member = 0
        if is_pool_member:
            member=1
        response = table.update_item(
            Key={
                'launcher_id': launcher_id.hex(),
            },
            UpdateExpression="set singleton_tip=:t, singleton_tip_state=:s, is_pool_member=:m",
            ExpressionAttributeValues={
                ':t': Binary(bytes(singleton_tip)),
                ':s': Binary(bytes(singleton_tip_state)),
                ':m': member
            },
            ReturnValues="UPDATED_NEW"
        )
        return response

    async def get_pay_to_singleton_phs(self) -> Set[bytes32]:
        # TODO(do we need only get those which has is_pool_member: true??)
        table = self.get_farmer_table()
        scan_kwargs = {
            'ProjectionExpression': "p2_singleton_puzzle_hash",
        }
        response = table.scan(**scan_kwargs)
        all_items = response.get('Items', [])
        all_phs: Set[bytes32] = set() 
        for item in all_items:
            all_phs.add(bytes32(bytes.fromhex(item['p2_singleton_puzzle_hash'])))
        return all_phs

    async def get_farmer_records_for_p2_singleton_phs(self, puzzle_hashes: Set[bytes32]) -> List[FarmerRecord]:
        table = self.get_farmer_table()
        scan_kwargs = {}
        response = table.scan(**scan_kwargs)
        all_items = response.get('Items', [])
        result=[]
        for item in all_items:
            p2sph = bytes.fromhex(item['p2_singleton_puzzle_hash'])
            if p2sph in puzzle_hashes:
                result.append(self._row_to_farmer_record(item))
        return result

    async def get_farmer_points_and_payout_instructions(self) -> List[Tuple[uint64, bytes]]:
        table = self.get_farmer_table()
        scan_kwargs = {} 
        response = table.scan(**scan_kwargs)
        all_items = response.get('Items', [])
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
        table = self.get_farmer_table()
        scan_kwargs = {}
        response = table.scan(**scan_kwargs)
        all_items = response.get('Items', [])
        for item in all_items:
            response = table.update_item(
                Key={'launcher_id': item['launcher_id']},
                UpdateExpression="set points=:p",
                ExpressionAttributeValues={
                 ':p': 0
                },
                ReturnValues="UPDATED_NEW"
                
            )

    async def add_partial(self, launcher_id: bytes32, timestamp: uint64, difficulty: uint64):
        partial_table = self.get_partial_table()
        response = partial_table.put_item(
            Item={
            'launcher_id': launcher_id.hex(),
            'timestamp': timestamp,
            'difficulty': difficulty,
           }
        )        

        farmer_table = self.get_farmer_table()
        response = farmer_table.update_item(
                Key={'launcher_id': launcher_id.hex()},
                UpdateExpression="set points=points + :d",
                ExpressionAttributeValues={
                 ':d': difficulty 
                },
                ReturnValues="UPDATED_NEW"
                
            )


    async def get_recent_partials(self, launcher_id: bytes32, count: int) -> List[Tuple[uint64, uint64]]:
        partial_table = self.get_partial_table()
        response = partial_table.query(
            KeyConditionExpression='launcher_id = :id',
            ExpressionAttributeValues={
                ':id': launcher_id.hex()
            },
            Select='ALL_ATTRIBUTES',
            ScanIndexForward=False,
            Limit=count
        )
        ret: List[Tuple[uint64, uint64]]  = []
        for item in response['Items']:
            ret.append((uint64(item['timestamp']), uint64(item['difficulty'])))
        return ret 

