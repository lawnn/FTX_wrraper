import asyncio
import pybotters
from typing import Literal
from .time_util import now_jst
from .base import BotBase
from .exceptions import RequestException


class bitflyer(BotBase):
    def __init__(self, config: str, symbol: str):
        super().__init__(config)
        # 通貨ペア
        self.symbol = symbol
        # APIを呼ぶ回数
        self.api_call_count_from_private = 0    #5分間で500回まで
        self.api_call_count_from_order = 0      #5分間で300回まで
        # API keyの設定
        self.key = {"bitflyer": self.config["bitflyer"]}
        # 何かしらのエラーがでたときに繰り返す回数
        self.retry_count = 3
        # 発注履歴ファイルを保存するファイルのパラメータ
        self.columns = {
            "order_no": "オーダーNo.",
            "order_id": "オーダーID",
            "timestamp": "オーダー時刻",
            "order_kind": "オーダー種別",
            "size": "実際にオーダーしたサイズ",
            "price": "実際にオーダーした価格",
            "current_position": "現在ポジション",
        }


    async def stop(self):
        """
        ボットを停止します.
        """
        self.log_debug("bitflyer stop start")
        super().stop()
        await self._cancel_and_liquidate()
        self.close_order_history_files()
        self.log_debug("bitflyer stop end")


    async def _cancel_and_liquidate(self, moq = 0.01):
        """
        全ての注文をキャンセルした後、ポジションを成行で反対売買してクローズします.
        :param moq(Minimum Order Quantity)　最小ロットです
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
        if abs(position["size"]) >= moq:
            self.log_info(f"Liquidating current position {position} lot.")
            order_datetime = now_jst()
            if position["side"] == "BUY":
                order = await self.market_order("SELL", position["size"])
            else:
                order = await self.market_order("BUY", position["size"])
            order_history = {
                "order_no": "",
                "order_id": order["child_order_acceptance_id"],
                "timestamp": order_datetime.timestamp(),
                "order_kind": "Bot Stop Liquidation",
                "size": position["side"],
                "price": 0,
                "current_position": position["size"]
            }
            self.write_order_history(order_history)
        self.log_debug("_cancel_and_liquidate end.")


    async def _requests(self, method: str, url: str, params=None, data=None):
        async with pybotters.Client(apis=self.key, base_url='https://api.bitflyer.com') as client:
            return await client.request(method, url=url, params=params, data=data)


    async def _replace_order(self, side: str, size: float, order_type: str, price: any = None,
                             minute_to_expire: int = 43200, time_in_force: str = "GTC"):
        request = {
            "product_code": self.symbol,
            "child_order_type": order_type,
            "side": side,
            "size": size,
            "minute_to_expire": minute_to_expire,
            "time_in_force": time_in_force
        }

        if order_type == "LIMIT":
            request["price"] = price

        response = await self._requests('POST', url="/v1/me/sendchildorder", data=request)

        self.api_call_count_from_private += 1
        self.api_call_count_from_order += 1

        if not str(response.status).startswith('2'):
            if str(response.status).startswith("4"):
                raise RequestException(f"{response.status} Error {await response.json()}")
            else:
                raise RequestException(f"{response.status} Internal Server Error")
        return  await response.json()


    async def market_order(self, side: str, size: float) -> dict:
        """
        成行注文です
        :param side: buy or sell
        :param size: 数量
        """
        return await self._replace_order(side, size, "MARKET")


    async def limit_order(self, side: Literal["BUY", "SELL"], size: float, price: any,
                          minute_to_expire: int = 43200, time_in_force: Literal["GTC", "IOC", "FOK"] = "GTC") -> dict:
        """
        指値注文です
        :param side: BUY or SELL
        :param size: 数量
        :param price: 値段
        :param minute_to_expire: 有効期限 分単位
        :param time_in_force:  執行数量条件 "GTC", "IOC", "FOK"のいずれか
        """
        return await self._replace_order(side, size, "LIMIT", price, minute_to_expire, time_in_force)


    async def cancel_order(self, child_order_acceptance_id):
        """
        注文をキャンセルします
        :param child_order_acceptance_id
        """
        data = {
            "product_code": self.symbol,
            "child_order_acceptance_id": child_order_acceptance_id
        }

        await self._requests('POST', url="/v1/me/cancelchildorder", data=data)

        self.api_call_count_from_private += 1


    async def cancel_all_orders(self):
        """
        全ての注文をキャンセルします
        """
        data = {
            "product_code": self.symbol,
        }

        await self._requests('POST', url="/v1/me/cancelallchildorders", data=data)

        self.api_call_count_from_private += 1
        self.api_call_count_from_order += 1


    async def _fetch_position(self):
        response = await self._requests("GET", url="/v1/me/getpositions", params={"product_code": self.symbol})
        self.api_call_count_from_private += 1
        if not str(response.status).startswith('2'):
            if str(response.status).startswith("4"):
                raise RequestException(f"{response.status} Error {await response.json()}")
            else:
                raise RequestException(f"{response.status} Internal Server Error")
        return  await response.json()


    async def fetch_my_position(self) -> dict:
        """
        ポジションを取得します.
        :return {"side": side, "size": size}
        """
        response = await self._fetch_position()
        if not response:
            return {}
        else:
            size = sum([r["size"] for r in response])
            side = response[0]["side"]
            return {"side": side, "size": size}
