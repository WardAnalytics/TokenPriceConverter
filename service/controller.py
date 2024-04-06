from fastapi import APIRouter

router = APIRouter(prefix="/tokens")


@router.get("/{token_address}")
async def get_token_usd_price(token_address: str):
    return {"token_address": token_address, "usd_price": 1.0}


@router.get("/{source_token_address}/to/{target_token_address}")
async def get_token_exchange_rate(source_token_address: str, target_token_address: str):
    return {
        "source_token_address": source_token_address,
        "target_token_address": target_token_address,
        "exchange_rate": 1.0,
    }