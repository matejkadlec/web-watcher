import telebot
import time


class TelegramBot:
    def __init__(self, chat_id):
        self.bot = telebot.TeleBot(chat_id)

    def send_error_message(self, url, error):
        # send message on telegram
        self.bot.send_message(-1001731120154, f"URL {url} wasn't parsed successfully.\n\nError message: {error}")
        time.sleep(3)

    def send_url_changed_message(self, key, url, difference):
        # send message on telegram
        self.bot.send_message(-1001731120154, f"{key.capitalize()} of URL {url} changed.\n\n{difference}")
        time.sleep(3)

    def send_sitemap_changed_message(self, sitemap, new, missing):
        # send message on telegram
        if new != "" and missing != "":
            self.bot.send_message(-1001731120154, f"Sitemap {sitemap} changed.\n\nNEW:\n{new}\nMISSING:\n{missing}")
        elif new != "":
            self.bot.send_message(-1001731120154, f"Sitemap {sitemap} changed.\n\nNEW:\n{new}")
        else:
            self.bot.send_message(-1001731120154, f"Sitemap {sitemap} changed.\n\nMISSING:\n{missing}")
        time.sleep(3)
