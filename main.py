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

def main():
    ultima_data = obter_data_sincronizacao()

    pg_conn, pg_cursor = conectar_postgres()

    mysql_conn, mysql_cursor = conectar_mysql()

    telegram.notify("Bancos de Dados sincronizados com sucesso !")
    time.sleep(2)

    telegram.notify("Iniciando envio de dados ⚠️")

    try:
        transferir(
            pg_cursor, mysql_cursor,
            "lote",
            ["cd_lote", "cidade_origem", "cidade_destino", "dt_lote", "frete_empresa", "frete_motorista",
             "pagador", "dt_fechamento", "cd_produto", "cd_unidade", "qt_produto", "agencia", "cliente",
             "ds_observacao", "dt_inicio", "dt_termino", "situacao", "dt_atualizacao", "saldo", "rota",
             "valor", "criado_alterado", "ds_observacao_interna", "vl_credito", "vl_saldo", "nr_telefone"],
            obter_query_lote(ultima_data),
            chave_primaria="cd_lote"
        )

        telegram.notify("Dados enviados para a tabela lote ✅")
        time.sleep(2)

        transferir(
            pg_cursor, mysql_cursor,
            "agencia",
            ["cd_agencia", "nm_agencia"],
            obter_query_agencia(),
            chave_primaria="cd_agencia",
            modo_update=False
        )

        telegram.notify("Dados enviados para a tabela agencia ✅")
        time.sleep(2)

        transferir(
            pg_cursor, mysql_cursor,
            "liberacao",
            ["agencia_lote", "agencia", "cd_lote"],
            obter_query_liberacao(ultima_data),
            chave_primaria="agencia_lote"
        )

        telegram.notify("Dados enviados para a tabela liberacao ✅")
        time.sleep(2)

        mysql_conn.commit()
        telegram.notify("Transferência finalizada com sucesso 👍")

    except Exception as e:
        mysql_conn.rollback()
        telegram.notify(f"Erro durante a transferência: {e}")

    finally:
        pg_cursor.close()
        pg_conn.close()
        mysql_cursor.close()
        mysql_conn.close()

if __name__ == "__main__":
    main()