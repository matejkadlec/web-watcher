import telebot
import time

bot = telebot.TeleBot('5038704908:AAEAAkRdrWtJs384RPIGqEUiX0xPSp5IrM8')


def send_error_message(url, error):
    # send message on telegram
    bot.send_message(-1001731120154, f"URL {url} wasn't parsed successfully.\n\nError message: {error}")
    time.sleep(3)


def send_changed_message(key, url, difference):
    # send message on telegram
    bot.send_message(-1001731120154, f"{key.capitalize()} of URL {url} changed.\n\n{difference}")
    time.sleep(3)
