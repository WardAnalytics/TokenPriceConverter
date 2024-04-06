import os
import json
import asyncio
from dotenv import load_dotenv

# We're gonna be using aiohttp for requests instead of web3.py so we can avoid conflicts. Web3.py is kinda ass
from aiohttp import ClientSession

# Import node URL
load_dotenv()
NODE_URL = os.getenv("NODE_URL")


""" Getting token reserves of a pair for a specific block """

async def get_pool_ratio(session: ClientSession, pool_address: str) -> float:
    """ Returns the ratio between token0 and token1 of the pool's addresses """

""" Getting the token addresses of a pool contract """

async def get_single_pool_token_async(session: ClientSession, pool_address: str, token: int) -> str:
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

async def get_pool_tokens_async(session: ClientSession, pool_address: str) -> tuple[str, str]:
    """ #### Get the token addresses of a pool
    :param pool_address: The address of the pool
    :param session: The aiohttp ClientSession object
    :return: A tuple of the token addresses of the pool
    
    Also caches the result in Redis to speed up future requests
    """    

    # Fetch both tokens concurrently

    tasks = [get_single_pool_token_async(session, pool_address, 0), get_single_pool_token_async(session, pool_address, 1)]
    tokens: list[str] = await asyncio.gather(*tasks)

    return tuple(tokens)


""" Getting raw swap logs for a range of blocks """

async def get_raw_swap_logs(session: ClientSession, from_block: int, to_block: int) -> list[dict]:
    """ #### Get all swap log events between two blocks 
    :param from_block: The block number to start from
    :param to_block: The block number to end at
    :return: A list of swap log events
    """

    # Signature of the Swap event for Uniswap contracts
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

    while True:
        try:
            response = await session.post(NODE_URL, headers=headers, data=payload)
            
            if response.status == 200:
                data = await response.json()
                logs = data['result']

                return logs
            
            print(f"Failed to get swap logs: [{response.status}] {response.text}")
            await asyncio.sleep(1)

        except Exception as e:
            raise e
            print(f"Got a weird error, retrying anyways lol: {e}")
            await asyncio.sleep(1)
            continue


""" Getting the token decimals of a token contract """

async def get_token_decimals(session: ClientSession, token_address: str) -> int:
    """ #### Get the decimals of a token
    :param token_address: The address of the token
    :return: The decimals of the token
    """

    functionSignature = "0x313ce567"
    payload = json.dumps({
        "method": "eth_call",
        "params": [
            {
                "to": token_address,
                "data": functionSignature
            }
        ],
        "id": 1,
        "jsonrpc": "2.0"
    })

    while True:
        try:
            response = await session.post(NODE_URL, headers={'Content-Type': 'application/json'}, data=payload)
            
            if response.status == 200:
                decimals = int((await response.json())['result'], 16)
                return decimals
            
            print(f"Failed to get token decimals: [{response.status}] {response.text}")
            await asyncio.sleep(1)

        except Exception as e:
            print(f"Got a weird error, retrying anyways lol: {e}")
            await asyncio.sleep(1)
            continue

async def get_token_symbol(session: ClientSession, token_address: str) -> str:
    """ #### Get the symbol of a token
    :param token_address: The address of the token
    :return: The symbol of the token
    """

    functionSignature = "0x95d89b41"
    payload = json.dumps({
        "method": "eth_call",
        "params": [
            {
                "to": token_address,
                "data": functionSignature
            }
        ],
        "id": 1,
        "jsonrpc": "2.0"
    })

    while True:
        try:
            response = await session.post(NODE_URL, headers={'Content-Type': 'application/json'}, data=payload)
            
            if response.status == 200:
                symbol = (await response.json())['result']
                return bytes.fromhex(symbol[2:]).decode()
            
            print(f"Failed to get token symbol: [{response.status}] {response.text}")
            await asyncio.sleep(1)

        except Exception as e:
            print(f"Got a weird error, retrying anyways lol: {e}")
            await asyncio.sleep(1)
            continue