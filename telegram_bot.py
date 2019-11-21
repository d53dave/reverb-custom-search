import logging
import os
import pickle

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from telegram import ParseMode

from typing import Set, Dict

_PICKLE_FILE = 'chats_and_terms.db'


class Bot():
    def __init__(self,
                 token: str,
                 pickle_path=os.path.dirname(os.path.realpath(__file__))
                 ) -> None:
        logging.info('Creating bot')
        self.token = token
        self.pickle_path = pickle_path
        self.chat_ids: Dict[str, str] = self._read_picked_data()

        self.updater = Updater(token, use_context=True)
        self.dispatcher = self.updater.dispatcher
        self.bot = self.updater.bot

        self.dispatcher.add_handler(CommandHandler('start', self._start))
        self.dispatcher.add_handler(
            MessageHandler(Filters.text, self._register))

        if len(self.chat_ids) > 0:
            for user in self.chat_ids.keys():
                self.bot.send_message(
                    text=
                    'Hi there, just wanted to say I\'m still on the lookout for your gear. Have a nice day :)',
                    chat_id=user,
                    parse_mode=ParseMode.MARKDOWN)

        self.updater.start_polling()

    def get_terms(self) -> Set[str]:
        return set(self.chat_ids.values())

    def _register(self, update, context) -> None:
        self.chat_ids[update.message.chat_id] = update.message.text
        update.message.reply_markdown(
            f'You will receive updates for *{update.message.text}*')
        self._update_pickled_data()

    def _start(self, update, context) -> None:
        self.chat_ids[update.message.chat_id] = ''
        update.message.reply_markdown(
            "# Hi\nI'm the friendly reverb search bot! Send me a seach query" +
            " and I will notify you of new listings for used electric guitars matching your query 🎸🎸🎸"
        )
        self._update_pickled_data()

    def _read_picked_data(self) -> Dict[str, str]:
        try:
            with open(os.path.join(self.pickle_path, _PICKLE_FILE),
                      'rb') as handle:
                d = pickle.load(handle)
                return d if d is not None else {}
        except FileNotFoundError:
            logging.warn(
                'No pickled data found. This is normal on the first start.')
            return {}

    def _update_pickled_data(self) -> None:
        with open(os.path.join(self.pickle_path, _PICKLE_FILE),
                  'wb') as handle:
            pickle.dump(self.chat_ids,
                        handle,
                        protocol=pickle.HIGHEST_PROTOCOL)

    def send_update(self, term, listing_messages) -> None:
        for user in self.chat_ids.keys():
            if term in self.chat_ids[user]:
                text = f'There are new listings for your search \'{term}\'! \n'
                text += '\n'.join(listing_messages)
                self.bot.send_message(text=text,
                                      chat_id=user,
                                      parse_mode=ParseMode.MARKDOWN)
