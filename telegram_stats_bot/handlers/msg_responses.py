import telegram
from telegram import MessageEntity, Update
from telegram.ext import ContextTypes, filters

from telegram_stats_bot import global_vars
from telegram_stats_bot.handlers.decorator import message

@message(filters.Entity(MessageEntity.MENTION))
async def responses(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    ff_text = 'Você já ouviu falar do aclamado MMORPG Final Fantasy XIV? Com uma versão gratuita expandida, você pode jogar todo o conteúdo de A Realm Reborn e a premiada expansão Stormblood até o nível 70 gratuitamente, sem restrições de tempo de jogo.'
    if not message or not message.entities:
        return

    bot_username = context.bot.username
    for entity in message.entities:
        if entity.type != MessageEntity.MENTION:
            continue

        assert message.text != None
        mentioned_username = message.text[entity.offset:entity.offset + entity.length]
        if mentioned_username != f"@{bot_username}":
            continue

        if any(kw in message.text for kw in ["ffxiv", 'ff14']):
            _ = await message.reply_text(ff_text)

        elif 'raqueta' in message.text:
            raq_path = os.path.join(global_vars.other_path, 'raqueta.txt')
            with open(raq_path, 'r') as file:
                raqueta = file.readlines()
            _ = await message.reply_text(
                text = f"```{raqueta}```",
                parse_mode = telegram.constants.ParseMode.MARKDOWN_V2
            )
