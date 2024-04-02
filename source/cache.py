import os
from dotenv import load_dotenv
import redis

# Load the Redis URL
load_dotenv()
REDIS_URL = os.getenv('REDIS_URL', 'localhost')

""" Pool Address Caching 

We cache the two addresses for each pool contract so that we can avoid spamming
the node with token0() and token1() contract calls. Because most trades are done
on extremely popular pools, we can just cache the addresses and avoid the calls.

"""

POOL_ADDRESSES_DB = 0
pool_address_redis = redis.Redis(host=REDIS_URL, port=6379, db=POOL_ADDRESSES_DB)

def set_pool_pair(pool_address: str, tokens: tuple[str, str]):
    """ #### Set the token addresses of a pool
    :param pool_address: The address of the pool
    :param tokens: A tuple of the token addresses of the pool
    """
    
    pool_address_redis.set(pool_address, f"{tokens[0]},{tokens[1]}")

def get_pool_pair(pool_address: str) -> tuple[str, str] | None:
    """ #### Get the token addresses of a pool
    :param pool_address: The address of the pool
    :return: A tuple of the token addresses of the pool
    """
    
    if pool_address_redis.exists(pool_address): return tuple(pool_address_redis.get(pool_address).decode().split(","))
    
    return None


""" Token Decimals Caching 

To make the result more readable, we cache the decimals of the two input tokens
for conversion so that we don't need to get the decimals of each token contract
each time.

"""

TOKEN_DECIMALS_DB = 1
token_decimals_db = redis.Redis(host=REDIS_URL, port=6379, db=TOKEN_DECIMALS_DB)

def set_token_decimals(token_address: str, decimals: int):
    """ #### Set the decimals of a token
    :param token_address: The address of the token
    :param decimals: The decimals of the token
    """
    
    token_decimals_db.set(token_address, decimals)

def get_token_decimals(token_address: str) -> int | None:
    """ #### Get the decimals of a token
    :param token_address: The address of the token
    :return: The decimals of the token
    """
    
    if token_decimals_db.exists(token_address): return int(token_decimals_db.get(token_address).decode())
    
    return None


""" Token Symbol Caching """

TOKEN_SYMBOLS_DB = 2
token_symbols_db = redis.Redis(host=REDIS_URL, port=6379, db=TOKEN_SYMBOLS_DB)

def set_token_symbol(token_address: str, symbol: str):
    """ #### Set the symbol of a token
    :param token_address: The address of the token
    :param symbol: The symbol of the token
    """
    
    token_symbols_db.set(token_address, symbol)

def get_token_symbol(token_address: str) -> str | None:
    """ #### Get the symbol of a token
    :param token_address: The address of the token
    :return: The symbol of the token
    """
    
    if token_symbols_db.exists(token_address): return token_symbols_db.get(token_address).decode()
    
    return None


""" Token Decimals Caching """

TOKEN_DECIMALS_DB = 3
token_decimals_db = redis.Redis(host=REDIS_URL, port=6379, db=TOKEN_DECIMALS_DB)

def set_token_decimals(token_address: str, decimals: int):
    """ #### Set the decimals of a token
    :param token_address: The address of the token
    :param decimals: The decimals of the token
    """
    
    token_decimals_db.set(token_address, decimals)

def get_token_decimals(token_address: str) -> int | None:
    """ #### Get the decimals of a token
    :param token_address: The address of the token
    :return: The decimals of the token
    """
    
    if token_decimals_db.exists(token_address): return int(token_decimals_db.get(token_address).decode())
    
    return None

