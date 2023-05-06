import asyncio
import pybotters
import time
from .time_util import now_jst, now_jst_str
from decimal import Decimal
from traceback import format_exc
from .base import BotBase
from .logfile import OrderHistory
from .exceptions import APIException, RequestException


class BitBank(BotBase):
    def __init__(self, config: str, symbol: str):
        super().__init__(config)
        # 通貨ペア
        self.symbol = symbol
        # APIを呼ぶ回数
        self.total_api_call_count = 0
        # 今呼んでいるAPI key _requestメソッドでカウントしています.
        self.current_key_index = 0
        # API keyの設定
        try:
            self.keys = self.config["bitbank_keys"]
            self.key = {"bitbank": self.keys[self.current_key_index]}
            self.check_keys = True
        except KeyError:
            self.key = {"bitbank": self.config["bitbank"]}
            self.check_keys = False
        self.stop_flag = False
        # 何かしらのエラーがでたときに繰り返す回数
        self.retry_count = 3
        # 発注履歴ファイルを保存するファイルのパラメータ
        try:
            self.order_history_dir = self.config["log_dir"]
        except KeyError:
            self.order_history_dir = 'log'
        self.columns = {
            "order_no": "オーダーNo.",
            "order_id": "オーダーID",
            "timestamp": "オーダー時刻",
            "order_kind": "オーダー種別",
            "size": "実際にオーダーしたサイズ",
            "price": "実際にオーダーした価格",
            "current_position": "現在ポジション",
        }
        self.order_history_file_name_base = f"{self.exchange_name}_{self.bot_name}_order_history"
        self.order_history_files = {}
        self.order_history_file_class = OrderHistory


    async def start(self):
        """
        ボットを起動します.
        """
        await self._run_logic()
        self.log_info("Bot started.")


    async def stop(self):
        """
        ボットを停止します.
        """
        self.stop_flag = True
        self.log_info("Logic threads has been stopped.")
        await self._cancel_and_liquidate()
        self.close_order_history_files()


    async def _run_logic(self):
        """
        ロジック部分です. 子クラスで実装します.
        """
        raise NotImplementedError()


    def write_order_history(self, order_history):
        """
        発注履歴を出力します.
        :param order_history: ログデータ.
        """
        self.get_or_create_order_history_file().write_row_by_dict(order_history)


    def get_or_create_order_history_file(self):
        """
        現在時刻を元に発注履歴ファイルを取得します.
        ファイルが存在しない場合、新規で作成します.
        :return: 発注履歴ファイル.
        """
        today_str = now_jst_str("%y%m%d")
        order_history_file_name = self.order_history_file_name_base + f"_{today_str}.csv"
        full_path = self.order_history_dir + "/" + order_history_file_name
        if today_str not in self.order_history_files:
            self.order_history_files[today_str] = self.order_history_file_class(full_path, self.columns)
            self.order_history_files[today_str].open()
        return self.order_history_files[today_str]


    def close_order_history_files(self):
        """
        発注履歴ファイルをクローズします.
        """
        for order_history_file in self.order_history_files.values():
            order_history_file.close()


    async def _cancel_and_liquidate(self):
        """
        全ての注文をキャンセルした後、ポジションを成行で反対売買してクローズします.
        """
        self.log_debug("_cancel_and_liquidate start.")
        self.log_info("Canceling all open orders.")
        # 全てキャンセル.
        await self.cancel_all_orders()
        time.sleep(5)
        # 5秒置いてさらに全てキャンセル.
        await self.cancel_all_orders()
        time.sleep(5)
        position = await self.fetch_my_position()
        if position >= 0.0001:
            self.log_info(f"Liquidating current position {position} lot.")
            order_datetime = now_jst()
            order = await self.market_order("sell", position)
            order_history = {
                "order_no": "",
                "order_id": order["order_id"],
                "timestamp": order_datetime.timestamp(),
                "order_kind": "Bot Stop Liquidation",
                "size": position,
                "price": 0,
                "current_position": 0
            }
            self.write_order_history(order_history)
        self.log_debug("_cancel_and_liquidate end.")


    async def _requests(self, method: str, url: str, params=None, data=None):
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
        実行すると取得系APIを一度に2回消費します
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
        実行すると取得系APIを2回,注文系を1回消費します
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
        取得系APIを1回,注文系APIを1回消費します
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