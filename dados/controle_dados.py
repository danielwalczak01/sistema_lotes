import os
from dotenv import load_dotenv
load_dotenv()

telegram_token = os.getenv("TELEGRAM_TOKEN")
telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")


Dados_Automacao = {

    "configuracao_sistema": {
        "telegram_token": telegram_token,
        "telegram_chat_id": telegram_chat_id
    }
        
    }