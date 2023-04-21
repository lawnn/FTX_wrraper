import pybotters
from traceback import format_exc
from .base import BotBase
from .exceptions import APIException


class BitBank(BotBase):
    def __init__(self, config: str, symbol: str):
        super().__init__(config)
        self.symbol = symbol
        self.key = {"bitbank": self.config["bitbank"]}

    async def _requests(self, method: str, url: str, params=None, data=None):
        async with pybotters.Client(apis=self.key, base_url='https://api.bitbank.cc/v1') as client:
            response = await client.request(method, url=url, params=params, data=data)
            if not str(response.status).startswith('2'):
                self.statusNotify(f"{response.status} error")
                raise APIException(f"{response.status} error")
            data = await response.json()

            if data["success"] == 0:
                self.statusNotify(f"[Error code] {data['data']['code']}\n" +
                    "https://github.com/bitbankinc/bitbank-api-docs/blob/master/errors_JP.md")
                raise APIException(f"[Error code] {data['data']['code']}\n" +
                    "https://github.com/bitbankinc/bitbank-api-docs/blob/master/errors_JP.md")
            else:
                return data["data"]

    async def _replace_order(self, side: str, size: float, order_type: str, price: any = None, post_only: bool = False):
        request = {
            "pair": self.symbol,
            "amount": size,
            "side": side,
            "type": order_type,
            "post_only": post_only
            }

        if order_type == "limit":
            request["price"] = price

        return await self._requests('POST', url="/user/spot/order", data=request)

    async def market_order(self, side: str, size: float):
        try:
            return await self._replace_order(side, size, "market")
        except Exception as e:
            self.log_error("API request failed in market_order.")
            self.log_error(format_exc())
            raise e

    async def limit_order(self, side: str, size: float, price: any, post_only: bool = False):
        try:
            return await self._replace_order(side=side, size=size, order_type="limit", price=price, post_only=post_only)
        except Exception as e:
            self.log_error("API request failed in limit_order.")
            self.log_error(format_exc())
            raise e
