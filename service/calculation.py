from dataclasses import dataclass
import asyncio
import time
from aiohttp import ClientSession
from core.node import get_raw_swap_logs, get_token_decimals as fetch_token_decimals, get_token_symbol as fetch_token_symbol, get_pool_tokens_async as fetch_pool_tokens
from service.cache import (
    get_token_decimals as get_cached_token_decimals, 
    get_token_symbol as get_cached_token_symbol, 
    set_token_decimals as set_cached_token_decimals, 
    set_token_symbol as set_cached_token_symbol, 
    get_pool_pair as get_cached_pool_pair, 
    set_pool_pair as set_cached_pool_pair
)

import rustworkx as rx

# Cache fetchers

class ContractNotFoundException(Exception):...
    
async def get_token_decimals(session: ClientSession, token_address: str) -> int:
    """ Gets the token decimals from cache. If they're not there, gets 
    them from the node and then caches the answer for next time. 
    
    :token_address: The address of the token contract
    :returns: The token decimals

    :raises ContractNotFoundException: If the token contract is not found
    """

    decimals: int | None = get_cached_token_decimals(token_address)
    
    try:
        if decimals is None: decimals = await fetch_token_decimals(session, token_address)
    except Exception as e:
        raise ContractNotFoundException(f"Token contract not found: {token_address}") from e
    
    set_cached_token_decimals(token_address, decimals)

    return decimals
    
async def get_token_symbol(session: ClientSession, token_address: str) -> str:
    """ Gets the token symbol from cache. If it's not there, gets 
    it from the node and then caches the answer for next time. 
    
    :token_address: The address of the token contract
    :returns: The token symbol

    :raises ContractNotFoundException: If the token contract is not found    

    """

    symbol: str | None = get_cached_token_symbol(token_address)
    
    try:
        if symbol is None: symbol = await fetch_token_symbol(session, token_address)
    except Exception as e:
        raise ContractNotFoundException(f"Token contract not found: {token_address}") from e
    
    set_cached_token_symbol(token_address, symbol)

    return symbol

async def get_pool_tokens(session: ClientSession, pool_address: str) -> tuple[str, str]:
    """ Gets the pool tokens from cache. If they're not there, gets 
    them from the node and then caches the answer for next time. 
    
    :pool_address: The address of the pool contract
    :returns: A tuple of the token addresses

    :raises ContractNotFoundException: If the pool contract is not found
    
    """

    tokens: tuple[str, str] | None = get_cached_pool_pair(pool_address)
    
    try:
        if tokens is None: tokens = await fetch_pool_tokens(session, pool_address)
    except Exception as e:
        raise e
        raise ContractNotFoundException(f"Pool contract not found: {pool_address}") from e
    
    set_cached_pool_pair(pool_address, tokens)

    return tokens


# Get swaps within a block range

@dataclass
class SwapEvent:
    """ #### Represents a swap event between two tokens in a pool """

    block_number: int
    transaction_hash: str
    log_index: int
    address: str
    
    from_token: str
    to_token: str
    
    from_amount: int
    to_amount: int
    
    @property
    def ratio(self) -> float:
        """ #### Get the ratio of the swap """
        
        return self.to_amount / self.from_amount

async def get_swaps(session: ClientSession, from_block: int, to_block: int) -> list[SwapEvent]:
    """ Gets the swaps from the node between two blocks
    
    :from_block: The block to start from
    :to_block: The block to end at
    :returns: A list of SwapEvent objects
    
    """

    logs: list[dict] = await get_raw_swap_logs(session, from_block, to_block)
    
    # Parse all the contract addresses from the logs
    pool_addresses: list[str] = list(set([log["address"] for log in logs]))

    # Get the token pool pairs for each contract
    pool_tokens_lst: list[tuple[str, str]] = await asyncio.gather(*[get_pool_tokens(session, pool_address) for pool_address in pool_addresses])
    pool_tokens: dict[str, tuple[str, str]] = dict(zip(pool_addresses, pool_tokens_lst))
        
    # Parse list of SwapEvent objects
    swaps: list[SwapEvent] = []

    for log in logs:
        pool_address: str = log["address"]
        from_token, to_token = pool_tokens[pool_address]
        
        swap = SwapEvent(
                block_number = int(log['blockNumber'], 16),
                transaction_hash = log['transactionHash'],
                log_index = int(log['logIndex'], 16),
                address = pool_address.lower(),
                from_token = from_token.lower(),
                to_token = to_token.lower(),
                from_amount = abs(int.from_bytes(bytes.fromhex(log['data'][2:66]), signed=True)),
                to_amount = abs(int.from_bytes(bytes.fromhex(log['data'][66:130]), signed=True))
            )     
        
        swaps.append(swap)

    return swaps


# Get ratio between two tokens

async def get_token_conversion_rate(token0: str, token1: str, block_number: int) -> dict:
    """ #### Get the conversion rate between two tokens at a specific block number
    :token0: The address of the first token
    :token1: The address of the second token
    :block_number: The block number to get the conversion rate at

    :returns: A dictionary with the conversion rate, token decimals and token symbols

    Example:
    ```json
    {
        "conversion_rate": 0.45,
        "token0_decimals": 18,
        "token1_decimals": 18,
        "token0_symbol": "DAI",
        "token1_symbol": "USDC"
    }
    ```
    """

    token0 = token0.lower()
    token1 = token1.lower()

    session = ClientSession()

    token0_decimals = get_token_decimals(session, token0)
    token1_decimals = get_token_decimals(session, token1)
    token0_symbol = get_token_symbol(session, token0)
    token1_symbol = get_token_symbol(session, token1)

    # Get the swap logs
    BLOCK_RANGE = 200
    start = time.time()
    swaps = await get_swaps(session, int(block_number - BLOCK_RANGE/2), int(block_number + BLOCK_RANGE/2))
    print(f"Took {round(time.time() - start, 2)}s to get {len(swaps)} swap events for {BLOCK_RANGE} blocks.")

    # Compile all tokens into a set. Map each token to an integer and back
    token_addresses: set[str] = set()
    for swap in swaps:
        token_addresses.add(swap.from_token)
        token_addresses.add(swap.to_token)

    token_id_map = { token: i for i, token in enumerate(token_addresses) }
    token_id_map_inv = { i: token for token, i in token_id_map.items() }

    # Build swap graph
    graph = rx.PyGraph()

    for id in token_id_map:
        graph.add_node(token_id_map[id])

    for swap in swaps:
        from_token_id = token_id_map[swap.from_token]
        to_token_id = token_id_map[swap.to_token]

        graph.add_edge(from_token_id, to_token_id, swap)

    # Find all shortest paths between two tokens
    from_token_id = token_id_map.get(token0)
    if from_token_id is None: raise Exception(f"Token0 {token0} not found in swap events")
    
    to_token_id = token_id_map.get(token1)
    if to_token_id is None: raise Exception(f"Token {token1} not found in swap events")

    # Get all shortest paths of swaps between two tokens
    paths = rx.graph_all_shortest_paths(graph, from_token_id, to_token_id)

    # Make sure the paths are unique
    paths = list(set([tuple(path) for path in paths]))

    # Get the path with the lowest conversion rate
    try:
        path = paths[0]
    except IndexError:
        raise Exception(f"No swap path found between token0 {token0} and token1 {token1}, unable to calculate conversion rate.")

    total_ratio = 1
    for i in range(len(path) - 1):
        swap: SwapEvent = graph.get_edge_data(path[i], path[i + 1])
        print(f"Swap from {swap.from_token} to {swap.to_token} with ratio {swap.ratio}: {swap}")

        # If the swap is from the first token to the second token, we need to divide the fromAmount by the toAmount to get the ratio
        # If the swap is from the second token to the first token, we need to multiply the toAmount by the fromAmount to get the ratio

        if swap.from_token == token_id_map_inv[path[i]] and swap.to_token == token_id_map_inv[path[i + 1]]: total_ratio *= swap.ratio
        elif swap.to_token == token_id_map_inv[path[i]] and swap.from_token == token_id_map_inv[path[i + 1]]: total_ratio /= swap.ratio

    # Multiple & Divide the ratio by the decimals
    token0_decimals = await token0_decimals
    token1_decimals = await token1_decimals

    total_ratio = total_ratio * (10 ** token0_decimals)
    total_ratio = total_ratio / (10 ** token1_decimals)

    def parse_symbol(symbol: str) -> str:

        REPLACE_CHARS = ["\x00", "\x01", "\x02", "\x03", "\x04", "\x05", "\x06", "\x07", "\x08", "\x09", " "]

        for char in REPLACE_CHARS:
            symbol = symbol.replace(char, "")

        return symbol
        

    result = {
        "conversion_rate": total_ratio,
        "token0_decimals": token0_decimals,
        "token1_decimals": token1_decimals,
        "token0_symbol": parse_symbol(await token0_symbol),
        "token1_symbol": parse_symbol(await token1_symbol),
        "token_pair_path": [token_id_map_inv[token_id] for token_id in path]
    }

    await session.close()

    return result