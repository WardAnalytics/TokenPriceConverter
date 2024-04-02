import os
from dataclasses import dataclass
import requests
import json
import asyncio

from aiohttp import ClientSession
from dotenv import load_dotenv
import rustworkx as rx
import redis

# Use QuickNode Ethereum node to get all logs between block range. Import from .env
load_dotenv()
NODE_URL = os.getenv("NODE_URL")

# Functions for getting the token addresses of a pool
# NOTE - We use Redis to speed up this procedure as it's highly cacheable.

r = redis.Redis(host='localhost', port=6379, db=0)

async def get_single_pool_token_async(pool_address: str, session: ClientSession, token: int) -> str:
    """ #### Get a single token address of a pool
    :param pool_address: The address of the pool
    :param session: The aiohttp ClientSession object
    :param token: The index of the token to get (0 or 1)
    :return: The address of the token at the index
    """

    functionSignature = None
    if token == 0: functionSignature = "0x0dfe1681"
    elif token == 1: functionSignature = "0xd21220a7"
    else: raise Exception(f"Invalid token index: {token} - must be 0 or 1")

    payload = json.dumps({
        "method": "eth_call",
        "params": [
            {
                "to": pool_address,
                "data": functionSignature
            }
        ],
        "id": 1,
        "jsonrpc": "2.0"
    })
    
    while True:
        try:
            async with session.post(NODE_URL, headers={'Content-Type': 'application/json'}, data=payload) as response:
                if response.status == 429: 
                    # print("Rate limited, waiting 1 second")
                    await asyncio.sleep(1) # Rate limited

                if response.status == 200:
                    tokenAddress = "0x" + (await response.json())['result'][26:66]
                    return tokenAddress
                
                raise Exception(f"Failed to get pool token: [{response.status}] {response.text}")
            
        except Exception as e:
            # print(f"Got a weird error, retrying anyways lol: {response.status} {response.text} - {e}")
            await asyncio.sleep(1)

async def get_pool_tokens_async(pool_address: str, session: ClientSession) -> tuple[str, str]:
    """ #### Get the token addresses of a pool
    :param pool_address: The address of the pool
    :param session: The aiohttp ClientSession object
    :return: A tuple of the token addresses of the pool
    
    Also caches the result in Redis to speed up future requests
    """    

    # Check Redis
    current = r.get(pool_address)
    if current: return tuple(current.decode().split(','))

    print(f"Fetching tokens for pool {pool_address}...")

    token0 = await get_single_pool_token_async(pool_address, session, 0)
    token1 = await get_single_pool_token_async(pool_address, session, 1)

    print(f"Got them! {token0}, {token1}")

    r.set(pool_address, f"{token0},{token1}")

    return (token0, token1)


@dataclass
class SwapEvent:
    """ #### Represents a swap event between two tokens in a pool """

    blockNumber: int
    transactionHash: str
    logIndex: int
    address: str
    
    fromToken: str
    toToken: str
    
    fromAmount: int
    toAmount: int
    
    @property
    def ratio(self) -> float:
        return self.toAmount / self.fromAmount

    def __str__(self):
        return f"SwapEvent(blockNumber={self.blockNumber}, transactionHash={self.transactionHash}, logIndex={self.logIndex}, fromToken={self.fromToken}, toToken={self.toToken}, fromAmount={self.fromAmount}, toAmount={self.toAmount}, ratio={self.ratio})"

    def __repr__(self):
        return self.__str__()

async def get_swap_logs(from_block: int, to_block: int) -> list[SwapEvent]:
    """ #### Get all swap log events between two blocks 
    :param from_block: The block number to start from
    :param to_block: The block number to end at
    :return: A list of SwapEvent objects
    """

    EVENT_SIGNATURE = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
    
    payload = json.dumps({
        "method": "eth_getLogs",
        "params": [
            {
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "topics": [EVENT_SIGNATURE]
            }
        ],
        "id": 1,
        "jsonrpc": "2.0" 
        })
    
    headers = { 'Content-Type': 'application/json' }

    response = requests.request("POST", NODE_URL, headers=headers, data=payload)
    
    # Compile all swap event token emitters so we can get the token addresses for each pool
    contracts_to_query = set()
    for log in response.json()['result']:
        contracts_to_query.add(log['address'])

    # Limit to 50 contracts (TEMPORARY)
    contracts_to_query = list(contracts_to_query)

    # Spam the node with requests to get the token addresses for each pool
    tasks = []
    async with ClientSession() as session:
        for contract in contracts_to_query:
            tasks.append(get_pool_tokens_async(contract, session))

        await asyncio.gather(*tasks)
    
    if response.status_code == 200:
        data = response.json()
        logs = data['result']

        swap_events = []
        for log in logs:

            poolAddress = log['address']

            # TEMPORARY - Discard logs whose pool address didn't fit into the limit
            if poolAddress not in contracts_to_query: continue
            fromToken, toToken = await get_pool_tokens_async(poolAddress, session)

            swap_event = SwapEvent(
                blockNumber = int(log['blockNumber'], 16),
                transactionHash = log['transactionHash'],
                logIndex = int(log['logIndex'], 16),
                address = poolAddress,
                fromToken = fromToken,
                toToken = toToken,
                fromAmount = abs(int.from_bytes(bytes.fromhex(log['data'][2:66]), signed=True)),
                toAmount = abs(int.from_bytes(bytes.fromhex(log['data'][66:130]), signed=True))
            )            
            # print(f"Swap: TXN {swap_event.transactionHash} FROM {swap_event.fromToken} TO {swap_event.toToken} RATIO {swap_event.ratio} POOL {swap_event.address}")

            swap_events.append(swap_event)
        return swap_events

    raise Exception(f"Failed to get swap logs: [{response.status_code}] {response.text}")

async def get_token_conversion_rate(from_token: str, to_token: str, block_number: int) -> float:
    """ #### Get the conversion rate between two tokens at a specific block number
    :param from_token: The address of the token to convert from
    :param to_token: The address of the token to convert to
    :param block_number: The block number to get the conversion rate at
    :return: The conversion rate between the two tokens
    """

    RANGE = 100

    # Get the swap events
    swap_events = await get_swap_logs(block_number - RANGE, block_number + RANGE)

    print(f"Swap events found: {len(swap_events)}")

    # Create a graph of the swap events. Each token is a node, and each swap is an edge. The final goal is to find multiple paths between two tokens
    # and calculate how much of token A token B is worth

    graph = rx.PyGraph()

    # Create an ID for each token
    token_addresses: set[str] = set()
    for swap_event in swap_events:
        token_addresses.add(swap_event.fromToken)
        token_addresses.add(swap_event.toToken)

    token_id_map = { token: i for i, token in enumerate(token_addresses) }
    token_id_map_inv = { i: token for token, i in token_id_map.items() }

    for id in token_id_map:
        graph.add_node(token_id_map[id])

    for swap_event in swap_events:
        from_token_id = token_id_map[swap_event.fromToken]
        to_token_id = token_id_map[swap_event.toToken]

        graph.add_edge(from_token_id, to_token_id, swap_event)
    
    # Find all shortest paths between two tokens
    from_token_id = token_id_map.get(from_token)
    if from_token_id is None: raise Exception(f"Token {from_token} not found in swap events")
    
    to_token_id = token_id_map.get(to_token)
    if to_token_id is None: raise Exception(f"Token {to_token} not found in swap events")

    paths = rx.graph_all_shortest_paths(graph, from_token_id, to_token_id)

    # Make sure the paths are unique
    paths = list(set([tuple(path) for path in paths]))

    # Each path is a list of nodes. Let's just go to the first path for the sake of simplicity.
    # We'll want to calculate how much of token A token B is worth, therefore we need to get each edge between each two nodes and get the fromAmount and toAmount of each edge
    # to calculate the final amount of token B we get from token A
    try:
        path = paths[0]
    except IndexError:
        raise Exception(f"No path found between {from_token} and {to_token}, unable to calculate conversion rate.")
    
    total_ratio = 1
    for i in range(len(path) - 1):
        swap: SwapEvent = graph.get_edge_data(path[i], path[i + 1])
        print(f"Swap from {swap.fromToken} to {swap.toToken} with ratio {swap.ratio}: {swap}")

        # If the swap is from the first token to the second token, we need to divide the fromAmount by the toAmount to get the ratio
        # If the swap is from the second token to the first token, we need to multiply the toAmount by the fromAmount to get the ratio

        if swap.fromToken == token_id_map_inv[path[i]] and swap.toToken == token_id_map_inv[path[i + 1]]: total_ratio *= swap.ratio
        elif swap.toToken == token_id_map_inv[path[i]] and swap.fromToken == token_id_map_inv[path[i + 1]]: total_ratio /= swap.ratio

    return total_ratio


if __name__ == "__main__":

    FROM_ADDRESS = "0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0".lower()
    TO_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower()

    conversion_rate = asyncio.run(get_token_conversion_rate(FROM_ADDRESS, TO_ADDRESS, 14000000))

    print(f"Conversion rate from {FROM_ADDRESS} to {TO_ADDRESS} at block 14000000: {conversion_rate}")

    exit()