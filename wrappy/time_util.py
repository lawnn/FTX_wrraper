import datetime


def now_jst():
    """
    現在時刻をJSTで取得.
    :return: datetime.
    """
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9), 'JST'))


def now_jst_str(date_format="%Y-%m-%d %H:%M:%S"):
    """
    現在時刻を日本時間の文字型で出力します
    :return:
    """
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9), 'JST')).strftime(date_format)


def now_gmt():
    """
    現在時刻をGMTで取得.
    :return: datetime
    """
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=0), 'GMT'))


def now_gmt_str(date_format="%Y-%m-%d %H:%M:%S"):
    """
    現在時刻をGMT時間の文字型で出力します
    :return:
    """
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=0), 'GMT')).strftime(date_format)


def now_utc():
    """
    現在時刻をUTCで取得.
    :return: datetime
    """
    return datetime.datetime.now(datetime.timezone.utc)


def now_utc_str(date_format="%Y-%m-%d %H:%M:%S"):
    """
    現在時刻をUTC時間の文字型で出力します
    :return:
    """
    return datetime.datetime.now(datetime.timezone.utc).strftime(date_format)
