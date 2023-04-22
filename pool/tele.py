import asyncio
import telegram

async def send_winning():    
    bot = telegram.Bot("TOKEN")    
    async with bot:   
          await bot.send_sticker(chat_id='chat_id', sticker=open('image', 'rb'))
