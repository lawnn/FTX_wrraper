import asyncio
import pybotters
from typing import Literal, Union
from .time_util import now_jst
from decimal import Decimal
from .base import BotBase
from .exceptions import APIException, RequestException


class BitBank(BotBase):
    def __init__(self, config: str, symbol: str):
        super().__init__(config)
        # 通貨ペア
        self.symbol = symbol
        # APIを呼ぶ回数
        self.total_api_call_count = 0
        # API keyの設定
        try:
            self.keys = self.config["bitbank_keys"]
            self.current_key_index = 0
            self.key = {"bitbank": self.keys[self.current_key_index]}
            self.check_keys = True
        except KeyError:
            self.key = {"bitbank": self.config["bitbank"]}
            self.check_keys = False
        # 何かしらのエラーがでたときに繰り返す回数
        self.retry_count = 3


    async def stop(self):
        """
        ボットを停止します.
        """
        self.log_debug("bitbank stop start")
        super().stop()
        await self._cancel_and_liquidate()
        self.log_debug("bitbank stop end")


    async def _cancel_and_liquidate(self):
        """
        ※bitbankは現物取引なので信用取引ができるようになりAPIの仕様が変わったら修正する.
        全ての注文をキャンセルした後、ポジションを成行で反対売買してクローズします.
        """
        self.log_debug("_cancel_and_liquidate start.")
        self.log_info("Canceling all open orders.")
        # 全てキャンセル.
        await self.cancel_all_orders()
        await asyncio.sleep(5)
        # 5秒置いてさらに全てキャンセル.
        await self.cancel_all_orders()
        await asyncio.sleep(5)
        position = await self.fetch_my_position()
        if position >= 0.0001:
            self.log_info(f"Liquidating current position {position} lot.")
            order_datetime = now_jst()
            order = await self.market_order("sell", position)
            current_position = await self.fetch_my_position()
        self.log_debug("_cancel_and_liquidate end.")


    async def _requests(self, method: str, url: str, params=None, data=None):
        # 複数のkeyを使いまわすには"bitbank_keys"をコンフィグに設定します.
        if self.check_keys:
            self.current_key_index = self.total_api_call_count % len(self.keys)
            current_key = {"bitbank": self.keys[self.current_key_index]}
            self.total_api_call_count += 1
        else:
            current_key = self.key

        async with pybotters.Client(apis=current_key, base_url='https://api.bitbank.cc/v1') as client:
            response = await client.request(method, url=url, params=params, data=data)
            if not str(response.status).startswith('2'):
                if str(response.status).startswith("429"):
                    raise RequestException(f"429 Too Many Requests")
                self.statusNotify(f"Status {response.status} Error")
                raise APIException(response)
            data = await response.json()

            if data["success"] == 0:
                raise RequestException(f"[Error code] {data['data']['code']} Error")
            else:
                return data["data"]


    async def _replace_order(self, side: str, size, order_type: str, price: any = None, post_only: bool = False):
        request = {
            "pair": self.symbol,
            "amount": str(size),
            "side": side,
            "type": order_type,
            "post_only": post_only
            }

        if order_type == "limit":
            request["price"] = str(price)

        return await self._requests('POST', url="/user/spot/order", data=request)


    async def market_order(self, side: Literal['buy', 'sell'], size: Union[float, int, Decimal]) -> dict:
        """
        成行注文です
        :param side: buy or sell
        :param size: 数量
        """
        try:
            return await self._replace_order(side, size, "market")
        except Exception as e:
            self.log_exception("API request failed in market_order.")
            raise e


    async def limit_order(self, side: Literal['buy', 'sell'], size: Union[float, int, Decimal], price: Union[float, int, Decimal], post_only: bool = False) -> dict:
        """
        指値注文です
        :param side: buy or sell
        :param size: 数量
        :param price: 値段
        :param post_only: 必ずMakerの注文にするか否か Takerの注文の場合発注されません True or False
        """
        try:
            return await self._replace_order(side=side, size=size, order_type="limit", price=price, post_only=post_only)
        except RequestException as e:
            self.log_exception(f"API request failed in limit_order.")
            raise e
        except Exception as e:
            self.log_exception("API request failed in limit_order.")
            raise e


    async def fetch_balance(self) -> dict:
        """
        口座情報を取得します
        """
        return await self._requests("GET", url="/user/assets")



    async def _fetch_active_order(self) -> dict:
        """
        注文中の情報を取得します
        """
        return await self._requests("GET", url="/user/spot/active_orders", params={"pair": self.symbol})


    async def _fetch_order_info(self, order_id: int) -> dict:
        """
        注文の情報を取得します(単品)
        主に非アクティブな情報を取得することに使います
        """
        return await self._requests("GET", url="/user/spot/order", params={"pair": self.symbol, "order_id": order_id})


    async def _fetch_orders_info(self, order_ids: list) -> dict:
        """
        注文の情報を取得します(複数)
        主に非アクティブな情報を取得することに使います
        """
        return await self._requests("POST", url="/user/spot/orders_info", data={"pair": self.symbol, "order_ids": order_ids})


    async def fetch_trades_history(self) -> dict:
        """
        自分の約定履歴を取得します
        """
        return await self._requests("GET", url="/user/spot/trade_history", params={"pair": self.symbol})


    async def fetch_open_orders(self) -> list:
        """
        注文中の全てのorder idを取得します
        :return:
        """
        try:
            open_orders = await self._fetch_active_order()
            return [open_orders["orders"][i]["order_id"]
                    for i in range(len(open_orders["orders"]))
                    if open_orders["orders"][i]["status"] == "UNFILLED"
                    or open_orders["orders"][i]["status"] == "PARTIALLY_FILLED"]
        except Exception as e:
            self.log_exception("API request failed in fetch_open_order.")
            raise e


    async def fetch_my_position(self) -> str:
        """
        ポジション数を取得します
        実行すると取得系APIを一度に2回消費します
        """
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

            return str((position + remaining_amount).normalize())
        except RequestException as e:
            raise e
        except Exception as e:
            self.logger.exception("API request failed in fetch_my_position.")
            raise e


    async def cancel_and_fetch_position(self) -> float:
        """
        ポジション数を取得します
        実行すると取得系APIを2回,注文系を1回消費します
        注文をキャンセルしつつポジション数を取得したいときに使います
        """
        try:
            await self.cancel_all_orders()
            symbol = self.symbol.replace("_jpy", "")
            balance = await self.fetch_balance()
            for i in range(1, len(balance["assets"])):
                if balance["assets"][i]["asset"].startswith(symbol):
                    return float(balance["assets"][i]["free_amount"])

        except RequestException as e:
            raise e
        except Exception as e:
            self.logger.exception("API request failed in cancel_and_fetch_position.")
            raise e


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
                raise e
        except RequestException as e:
            raise e


    async def cancel_all_orders(self):
        """
        全ての注文をキャンセルします
        取得系APIを1回,注文系APIを1回消費します
        """
        try:
            open_orders = await self.fetch_open_orders()
            return await self._cancel_any_orders(open_orders)
        except RequestException as e:
            if str(e).startswith("40014", 35):
                return None
        except Exception as e:
            self.logger.exception("API request failed in cancel_all_orders.")
            raise e


    async def exchange_status(self) -> dict:
        """
        サーバーの状態を見たいときに使います
        """
        try:
            return await self._requests("GET", url="/spot/status")
        except Exception as e:
            self.log_exception("API request failed in exchange status")
            raise e