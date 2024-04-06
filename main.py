from service.calculation import get_token_conversion_rate

from asyncio import run


if __name__ == "__main__":

    TOKEN0 = "0xCF3C8Be2e2C42331Da80EF210e9B1b307C03d36A"
    TOKEN1 = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    BLOCK_NUMBER = 19_577_771

    res = run(get_token_conversion_rate(TOKEN0, TOKEN1, BLOCK_NUMBER))

    print(res)