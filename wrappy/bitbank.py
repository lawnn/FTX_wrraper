import asyncio
import pybotters
from decimal import Decimal
from traceback import format_exc
from .base import BotBase
from .exceptions import APIException, RequestException


class BitBank(BotBase):
    def __init__(self, config: str, symbol: str):
        super().__init__(config)
        self.symbol = symbol
        self.key = {"bitbank": self.config["bitbank"]}
        self.retry_count = 3

    async def _requests(self, method: str, url: str, params=None, data=None):
        async with pybotters.Client(apis=self.key, base_url='https://api.bitbank.cc/v1') as client:
            response = await client.request(method, url=url, params=params, data=data)
            if not str(response.status).startswith('2'):
                if str(response.status).startswith("429"):
                    self.log_error("429 Too Many Requests")
                    await asyncio.sleep(1)
                self.statusNotify(f"{response.status} error")
                raise APIException(response)
            data = await response.json()

            if data["success"] == 0:
                raise RequestException(f"{data['data']['code']} error")
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

    async def fetch_balance(self) -> dict:
        """
        口座情報を取得します
        """
        try:
            return await self._requests("GET", url="/user/assets")
        except Exception as e:
            self.log_error("API request failed in market_order.")
            self.log_error(format_exc())
            raise e

    async def market_order(self, side: str, size: float) -> dict:
        """
        成行注文です
        :param side: buy or sell
        :param size: 数量
        """
        try:
            return await self._replace_order(side, size, "market")
        except Exception as e:
            self.log_error("API request failed in market_order.")
            self.log_error(format_exc())
            raise e

    async def limit_order(self, side: str, size: float, price: any, post_only: bool = False) -> dict:
        """
        指値注文です
        :param side: buy or sell
        :param size: 数量
        :param price: 値段
        :param post_only: 必ずMakerの注文にするか否か Takerの注文の場合発注されません True or False
        """
        try:
            return await self._replace_order(side=side, size=size, order_type="limit", price=price, post_only=post_only)
        except Exception as e:
            self.log_error("API request failed in limit_order.")
            self.log_error(format_exc())
            raise e

    async def _fetch_active_order(self) -> dict:
        """
        注文中の情報を取得します
        """
        try:
            return await self._requests("GET", url="/user/spot/active_orders", params={"pair": self.symbol})
        except Exception as e:
            self.log_error("API request failed in fetch active order.")
            self.log_error(format_exc())
            raise e

    async def fetch_open_orders(self) -> list:
        """
        注文中の全てのorder idを取得します
        :return:
        """
        failed_count = 0
        try:
            open_orders = await self._fetch_active_order()
            return [open_orders["orders"][i]["order_id"]
                    for i in range(len(open_orders["orders"]))
                    if open_orders["orders"][i]["status"] == "UNFILLED"
                    or open_orders["orders"][i]["status"] == "PARTIALLY_FILLED"]
        except Exception as e:
            failed_count += 1
            if failed_count > self.retry_count:
                self.logger.error("API request failed in fetch_open_order.")
                self.logger.error(format_exc())
                raise e
            await asyncio.sleep(0.2)

    async def _fetch_order_info(self, order_id: int) -> dict:
        """
        注文の情報を取得します(単品)
        主に非アクティブな情報を取得することに使います
        """
        try:
            return await self._requests("GET", url="/user/spot/order", params={"pair": self.symbol, "order_id": order_id})
        except Exception as e:
            self.log_error("API request failed in fetch order info.")
            self.log_error(format_exc())
            raise e

    async def _fetch_orders_info(self, order_ids: list) -> dict:
        """
        注文の情報を取得します(複数)
        主に非アクティブな情報を取得することに使います
        """
        try:
            return await self._requests("POST", url="/user/spot/orders_info", data={"pair": self.symbol, "order_ids": order_ids})
        except Exception as e:
            self.log_error("API request failed in fetch orders info.")
            self.log_error(format_exc())
            raise e

    async def fetch_trades_history(self) -> dict:
        """
        自分の約定履歴を取得します
        """
        try:
            return await self._requests("GET", url="/user/spot/trade_history", params={"pair": self.symbol})
        except Exception as e:
            self.log_error("API request failed in fetch orders info.")
            self.log_error(format_exc())
            raise e

    async def fetch_my_position(self) -> float:
        """
        ポジション数を取得します
        実行するとAPIを一度に2回消費します
        """
        failed_count = 0
        try:
            balance = await self.fetch_balance()
            open_orders = await self._fetch_active_order()
            symbol = self.symbol.replace("_jpy", "")
            position = "0"

            for i in range(1, len(balance["assets"])):
                if balance["assets"][i]["asset"].startswith(symbol):
                    position = Decimal(balance["assets"][i]["free_amount"])
                    break

            remaining_amount = sum([Decimal(order["remaining_amount"])
                                    for order in open_orders["orders"]
                                    if order["side"] == "sell"])

            return float(position + remaining_amount)
        except RequestException as e:
            failed_count += 1
            if failed_count > self.retry_count:
                self.logger.error("API request failed in fetch_my_position.")
                self.logger.error(format_exc())
                self.logger.error(e)
                raise e
            await asyncio.sleep(0.2)

    async def cancel_and_fetch_position(self) -> float:
        """
        ポジション数を取得します
        実行するとAPIを3回消費します
        注文をキャンセルしつつポジション数を取得したいときに使います
        """
        failed_count = 0
        try:
            await self.cancel_all_orders()
            symbol = self.symbol.replace("_jpy", "")
            balance = await self.fetch_balance()
            for i in range(1, len(balance["assets"])):
                if balance["assets"][i]["asset"].startswith(symbol):
                    return float(balance["assets"][i]["free_amount"])

        except RequestException as e:
            failed_count += 1
            if failed_count > self.retry_count:
                self.logger.error("API request failed in cancel_and_fetch_position.")
                self.logger.error(format_exc())
                self.logger.error(e)
                raise e
            await asyncio.sleep(0.2)

    async def _cancel_order(self, order_id: int) -> any:
        """
        単品の注文をキャンセルします
        """
        try:
            return await self._requests("POST", url="/user/spot/cancel_order", data={"pair": self.symbol, "order_id":order_id})
        except APIException as e:
            if e.status == 404:
                return None
            else:
                raise e
        except RequestException as e:
            raise e

    async def _cancel_any_orders(self, order_ids: list) -> any:
        """
        いくつかの注文をキャンセルします
        """
        try:
            return await self._requests("POST", url="/user/spot/cancel_orders", data={"pair": self.symbol, "order_ids": order_ids})
        except APIException as e:
            if str(e.status).startswith("404"):
                return None
            else:
                self.log_exception("API request failed in cancel orders.")
                self.log_exception(format_exc())
                raise e
        except RequestException as e:
            raise e

    async def cancel_all_orders(self):
        """
        全ての注文をキャンセルします
        APIを2回消費します
        """
        failed_count = 0
        try:
            open_orders = await self.fetch_open_orders()
            return await self._cancel_any_orders(open_orders)
        except RequestException as e:
            if str(e).startswith("40014"):
                return None
        except Exception as e:
            failed_count += 1
            if failed_count > self.retry_count:
                self.logger.exception("API request failed in cancel_all_orders.")
                self.logger.exception(format_exc())
                raise e
            await asyncio.sleep(0.2)

    async def exchange_status(self) -> dict:
        """
        サーバーの状態を見たいときに使います
        """
        try:
            return await self._requests("GET", url="/spot/status")
        except Exception as e:
            self.log_error("API request failed in exchange status")
            self.log_error(format_exc())
            raise e