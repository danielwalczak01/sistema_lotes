from config.database import conectar_postgres, conectar_mysql
from services.transferencia import transferir
from services.queries import obter_query_lote, obter_query_agencia, obter_query_liberacao
from utils.helpers import obter_data_sincronizacao

def main():
    ultima_data = obter_data_sincronizacao()

    pg_conn, pg_cursor = conectar_postgres()
    mysql_conn, mysql_cursor = conectar_mysql()

    try:
        transferir(
            pg_cursor, mysql_cursor,
            "lote",
            ["cd_lote", "cidade_origem", "cidade_destino", "dt_lote", "frete_empresa", "frete_motorista",
             "pagador", "dt_fechamento", "cd_produto", "cd_unidade", "qt_produto", "agencia", "cliente",
             "ds_observacao", "dt_inicio", "dt_termino", "situacao", "dt_atualizacao", "saldo", "rota",
             "valor", "criado_alterado", "ds_observacao_interna"],
            obter_query_lote(ultima_data),
            chave_primaria="cd_lote"
        )

        transferir(
            pg_cursor, mysql_cursor,
            "agencia",
            ["cd_agencia", "nm_agencia"],
            obter_query_agencia(),
            chave_primaria="cd_agencia",
            modo_update=False
        )

        transferir(
            pg_cursor, mysql_cursor,
            "liberacao",
            ["agencia_lote", "agencia", "cd_lote"],
            obter_query_liberacao(ultima_data),
            chave_primaria="agencia_lote"
        )

        mysql_conn.commit()
        print("Transferência finalizada com sucesso.")

    except Exception as e:
        mysql_conn.rollback()
        print(f"Erro durante a transferência: {e}")

    finally:
        pg_cursor.close()
        pg_conn.close()
        mysql_cursor.close()
        mysql_conn.close()

if __name__ == "__main__":
    main()
