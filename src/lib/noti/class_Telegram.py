import telegram
import asyncio


class Telegram():
    def __init__(self, bot_token, group_id):
        self.bot_token = bot_token
        self.group_id = group_id
        self.bot = telegram.Bot(token=bot_token)

    # send message
    def send_message(self, _message):
        self.bot.send_message(
            chat_id=self.group_id,
            text=_message
        )

    # send file
    async def send_file(self,  _caption, _file_path):
        self.bot.send_document(
            chat_id = self.group_id,
            caption = _caption,
            document = open(_file_path,'rb')
        )
