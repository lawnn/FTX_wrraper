from .notify import Notify

class BotBase(Notify):
    def __init__(self, path):
        super().__init__(path)
        self.stop_flag = False
        # 発注履歴ファイルを保存するファイルのパラメータ
        try:
            self.order_history_dir = self.config["log_dir"]
        except KeyError:
            self.order_history_dir = 'log'
        self.columns = {}
        # csvファイルを書き込む場所
        self.target_csv_file = f"{self.exchange_name}_{self.bot_name}_order_history.csv"

    async def start(self):
        """
        ボットを起動します.
        """
        await self._run_logic()
        self.log_info("Bot started.")

    def stop(self):
        """
        ボットを停止します.
        """
        self.stop_flag = True
        self.log_info("Logic has been stopped.")

    async def _run_logic(self):
        """
        ロジック部分です. 子クラスで実装します.
        """
        raise NotImplementedError()
