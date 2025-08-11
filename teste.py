from config.database import conectar_postgres, conectar_mysql
from services.transferencia import transferir
from services.queries import obter_query_lote, obter_query_agencia, obter_query_liberacao
from utils.helpers import obter_data_sincronizacao
from dados.controle_dados import Dados_Automacao
from telegram_notifier import *
import time

#Configurações Gerais
processamento_atual = "configuracao_sistema"
config_atual = Dados_Automacao[processamento_atual]
telegram_token = config_atual["telegram_token"]
telegram_chat_id = config_atual["telegram_chat_id"]

telegram = TelegramNotifier(telegram_token, telegram_chat_id)

telegram.notify("Bancos de Dados sincronizados com sucesso !")

time.sleep(2)

telegram.notify("mensagem 2")