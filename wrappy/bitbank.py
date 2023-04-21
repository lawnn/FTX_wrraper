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
