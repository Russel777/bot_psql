# -*- coding: utf-8 -*-
# created by Ruslan Nguen 24/09/2018

import config
import logging
import psycopg2
from telegram.ext import CommandHandler, Filters, MessageHandler, Updater
from typing import AnyStr

# Database consts
DB_NAME = config.DB_NAME
HOST = config.HOST
PASSWORD = config.PASSWORD

# Bot token
TOKEN = config.TOKEN

# Interval for callback sending informations about tables from DB
TIME_INTERVAL = 30

# Proxy parameters
REQUEST_KWARGS = {
    'proxy_url': config.PROXY_URL
}

# logging level
LOG_LEVEL = logging.INFO


class DatabaseBot:
    """
    DatabaseBot class allowed to create and run Telegram bot which worked with Database
    """
    def __init__(self):
        logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.updater = Updater(token=TOKEN, request_kwargs=REQUEST_KWARGS)
        self.job = self.updater.job_queue  # create job for sending tables' sizes info

        # Create handlers
        start_handler = CommandHandler('start', self.command_start)
        get_size_handler = CommandHandler('get_size', self.command_get_size)
        text_get_query_handler = MessageHandler(Filters.text, self.text_get_query)

        # Add handlers to Handlers list
        self.updater.dispatcher.add_handler(start_handler)
        self.updater.dispatcher.add_handler(get_size_handler)
        self.updater.dispatcher.add_handler(text_get_query_handler)

    def start(self):
        logging.info('Start DatabaseBot')
        self.updater.start_polling()

    def _db_query(self, query: AnyStr):
        """
        Get string with SQL query and response result of SQL query

        :param query: string with SQL query
        :return: dictionary with tables and their sizes
        """
        try:
            connect_str = "dbname='{}' host='{}' password='{}'".format(DB_NAME, HOST, PASSWORD)
            conn = psycopg2.connect(connect_str)
            cursor = conn.cursor()
            cursor.execute(query)
            response = cursor.fetchall()
            return response
        except Exception as e:
            logging.ERROR(e)
            raise

    def _get_size_all_tables(self):
        """
        Get size of all tables

        :return: all tables' size
        """
        query = """SELECT schemaname||'.'||tablename AS full_tname,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_usage
            FROM pg_catalog.pg_tables WHERE schemaname = 'public'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"""
        tables_size = self._db_query(query)
        return tables_size

    def _get_size_job(self, bot, job):
        """
        Function for 'get size' job

        :param bot: bot data
        :param job: job data
        :return:
        """
        tables_size = self._get_size_all_tables()
        bot.sendMessage(chat_id=job.context, text=tables_size)

    def command_start(self, bot, update):
        """
        Start command and repeating job

        :param bot: bot data
        :param update: user's info after update
        """
        logging.info('Start job to send tables\' sizes')
        bot.sendMessage(chat_id=update.message.chat_id, text="Start job to send tables' sizes!")
        self.job.run_repeating(self._get_size_job, interval=TIME_INTERVAL, first=0, context=update.message.chat_id)

    def command_get_size(self, bot, update):
        """
        Get_size command

        :param bot: bot data
        :param update: user's info after update
        """
        logging.info('Get size')
        bot.sendMessage(chat_id=update.message.chat_id, text=self._get_size_all_tables())

    def text_get_query(self, bot, update):
        """
        Get text query and request to database

        :param bot: bot data
        :param update: user's info after update
        :return:
        """
        logging.info('Query to Database')
        bot.send_message(chat_id=update.message.chat_id, text=self._db_query(update.message.text))

if __name__ == "__main__":
    dialog_bot = DatabaseBot()
    dialog_bot.start()
