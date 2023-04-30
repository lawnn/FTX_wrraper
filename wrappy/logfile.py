import os
import csv


class LogBase:
    """
    ログファイルの基底クラス.
    """

    def __init__(self, full_path, as_new=False, encoding="shift_jis", with_header=True, delimiter=",",
                 line_terminator="\n"):
        """
        コンストラクタです.
        :param full_path: ログファイルのフルパス.
        :param as_new: 既存ファイルがある場合、新規で作成するか否か.
        :param encoding: ログファイルのエンコーディング.
        :param with_header: 新規で作成する際、ヘッダをつけるか否か.
        :param delimiter: カラムの区切り文字.
        :param line_terminator: 行の終端を表す文字.
        """
        self.full_path = full_path
        self.as_new = as_new
        self.encoding = encoding
        self.with_header = with_header
        self.delimiter = delimiter
        self.line_terminator = line_terminator
        self.columns = {}
        self.default_values = {}
        self.file = None
        self.writer = None

    def __enter__(self):
        """
        withブロックに入ったときに呼ばれる処理です.
        ログファイルをオープンし、writerを作成します.
        """
        self._initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        withブロックから出たときに呼ばれる処理です.
        ログファイルをクローズします.
        """
        if self.file is not None:
            self.file.close()

    def open(self):
        """
        ログファイルをオープンします.
        """
        self._initialize()
        self.writer = csv.writer(self.file, lineterminator=self.line_terminator)

    def close(self):
        """
        ログファイルをクローズします.
        """
        if self.file is not None:
            self.file.close()

    def _initialize(self):
        """
        初期化処理を行います.
        """
        if os.path.exists(self.full_path) and self.as_new:
            os.remove(self.full_path)

        is_new_file = not os.path.exists(self.full_path)
        self.file = open(self.full_path, mode="a", encoding=self.encoding, newline="")
        if is_new_file:
            self.writer = csv.writer(self.file, lineterminator=self.line_terminator)
            if self.with_header:
                self._write_header()

    def _write_header(self):
        """
        ヘッダを書き込みます.
        """
        if self.writer is None:
            raise Exception("Writer is not initialized.")
        headers = self.get_headers()
        self.writer.writerow(headers)

    def write_row(self, row: list):
        """
        リスト形式で行を書き込みます.
        :param row: 行データ.
        """
        if self.writer is None:
            raise Exception("writer is not initialized.")
        self.writer.writerow(row)

    def write_row_by_dict(self, row_dict: dict):
        """
        辞書形式で行を書き込みます. 辞書内にないカラ
        ムは空文字列とします.
        :param row_dict: 行データ.
        """
        if self.writer is None:
            raise Exception("Writer is not initialized.")
        row = [row_dict.get(key, '') for key in self.get_headers()]
        self.write_row(row)

    def get_full_path(self):
        """
        ログファイルのフルパスを取得します.
        :return: ログファイルのフルパス.
        """
        return self.full_path

    def get_headers(self):
        """
        ヘッダーを取得.
        :return: ヘッダー.
        """
        return self.columns.keys()

    def get_columns(self):
        """
        カラムを取得.
        :return: カラム.
        """
        return self.columns

    def get_columns_num(self):
        """
        カラムの数を取得します.
        :return: カラム数.
        """
        return len(self.get_columns())

    def get_new_record(self):
        """
        新しいレコードを取得します.
        デフォルト値が設定されている場合、デフォルト値を入れます.
        :return: 新しいレコード.
        """
        return {item: self.default_values[item] if item in self.default_values else None for item in self.get_headers()}


class OrderHistory(LogBase):
    """
    注文履歴ログ.
    """

    def __init__(self, full_path, columns):
        super().__init__(full_path, as_new=False, encoding="shift_jis", with_header=True)
        self.columns = columns
