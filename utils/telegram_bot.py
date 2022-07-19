import telebot
import time


class TelegramBot:
    def __init__(self, chat_id):
        self.chat_id = chat_id
        self.bot = telebot.TeleBot(token='5038704908:AAEAAkRdrWtJs384RPIGqEUiX0xPSp5IrM8')

    def send_error_message(self, url, error, attempt):
        # send message on telegram
        self.bot.send_message(self.chat_id, f"URL {url} wasn't parsed successfully.\n\nAttempt: {attempt}\n\n "
                                            f"Error message: {error}")
        time.sleep(3)

    def send_url_changed_message(self, key, url, difference):
        # send message on telegram
        self.bot.send_message(self.chat_id, f"{key.capitalize()} of URL {url} changed.\n\n{difference}")
        time.sleep(3)

    def send_sitemap_changed_message(self, sitemap, new, missing):
        # send message on telegram
        if new != "" and missing != "":
            self.bot.send_message(self.chat_id, f"Sitemap {sitemap} changed.\n\nNEW:\n{new}\nMISSING:\n{missing}")
        elif new != "":
            self.bot.send_message(self.chat_id, f"Sitemap {sitemap} changed.\n\nNEW:\n{new}")
        else:
            self.bot.send_message(self.chat_id, f"Sitemap {sitemap} changed.\n\nMISSING:\n{missing}")
        time.sleep(3)
