import requests
from .log import Log

class Notify(Log):
    def __init__(self, path):
        super().__init__(path)
        self._initialize_logger()

        # ラインに稼働状況を通知
        try:
            self.line_notify_token = self.config["line_notify_token"]
        except KeyError:
            pass
        # Discordに稼働状況を通知するWebHook
        try:
            self.discordWebhook = self.config["discordWebhook"]
        except KeyError:
            # 設定されていなければNoneにしておく
            self.discordWebhook = None

    def lineNotify(self, message, fileName=None):
        payload = {'message': message}
        headers = {'Authorization': 'Bearer ' + self.line_notify_token}
        if fileName is None:
            try:
                requests.post('https://notify-api.line.me/api/notify', data=payload, headers=headers)
                self.log_info(message)
            except Exception as e:
                self.log_error(e)
                raise e
        else:
            try:
                files = {"imageFile": open(fileName, "rb")}
                requests.post('https://notify-api.line.me/api/notify', data=payload, headers=headers, files=files)
            except Exception as e:
                self.log_error(e)
                raise e

    # config.json内の[discordWebhook]で指定されたDiscordのWebHookへの通知
    def discordNotify(self, message, fileName=None):
        payload = {"content": " " + message + " "}
        if fileName is None:
            try:
                requests.post(self.discordWebhook, data=payload)
                self.log_info(message)
            except Exception as e:
                self.log_error(e)
                raise e
        else:
            try:
                files = {"imageFile": open(fileName, "rb")}
                requests.post(self.discordWebhook, data=payload, files=files)
            except Exception as e:
                self.log_error(e)
                raise e

    def statusNotify(self, message, fileName=None):
        # config.json内に[discordWebhook]が設定されていなければLINEへの通知
        if self.discordWebhook is None:
            self.lineNotify(message, fileName)
        else:
            # config.json内に[discordWebhook]が設定されていればDiscordへの通知
            self.discordNotify(message, fileName)

