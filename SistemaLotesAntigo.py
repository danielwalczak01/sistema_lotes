import psycopg2
import mysql.connector
from datetime import datetime, timedelta

def conectar_postgres():
    try:
        pg_conn = psycopg2.connect(
            dbname="anderle",
            user="anderle_ro",
            password="soT9AWbF0",
            host="bdbi.atua.com.br",
            port="5432"
        )
        pg_cursor = pg_conn.cursor()
        print("Conexão com PostgreSQL estabelecida com sucesso.")
        return pg_conn, pg_cursor
    except Exception as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None, None

def conectar_mysql():
    try:
        mysql_conn = mysql.connector.connect(
            user="anderletranspo",
            password="E4OKhg2Gc35d2Ad",
            host="191.6.211.147",
            database="anderletranspo",
            port=3306  
        )
        mysql_cursor = mysql_conn.cursor()
        
        print("Permissões concedidas com sucesso.")
        print("Conexão com MySQL estabelecida com sucesso.")
        return mysql_conn, mysql_cursor

    except mysql.connector.Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None, None

def transferir_dados(pg_cursor, mysql_cursor, tabela, colunas, query=None):
    try:
        if query:
            pg_cursor.execute(query)
        else:
            pg_cursor.execute(f"SELECT {', '.join(colunas)} FROM {tabela}")

        rows = pg_cursor.fetchall()
        print(f"{len(rows)} registros encontrados na tabela {tabela} do PostgreSQL.")

        for row in rows:
            cd_lote = row[0]  # Assumindo que a primeira coluna é cd_lote

            # Verifica se o lote já existe no MySQL
            select_query = f"SELECT COUNT(*) FROM {tabela} WHERE cd_lote = %s"
            mysql_cursor.execute(select_query, (cd_lote,))
            exists = mysql_cursor.fetchone()[0]
            
            if exists:
                # Se o registro existe, faz um UPDATE
                set_clause = ', '.join([f"{col}=%s" for col in colunas[1:]])  # Pula o cd_lote
                update_query = f"UPDATE {tabela} SET {set_clause} WHERE cd_lote = %s"
                mysql_cursor.execute(update_query, (*row[1:], cd_lote))
            else:
                # Se o registro não existe, faz um INSERT
                placeholders = ', '.join(['%s'] * len(row))
                insert_query = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({placeholders})"
                mysql_cursor.execute(insert_query, row)

        print(f"Transferência de dados da tabela {tabela} concluída.")
    except Exception as e:
        print(f"Erro ao transferir dados da tabela {tabela}: {e}")

def transferir_agencias(pg_cursor, mysql_cursor, tabela, colunas, query=None):
    try:
        if query:
            pg_cursor.execute(query)
        else:
            pg_cursor.execute(f"SELECT {', '.join(colunas)} FROM {tabela}")

        rows = pg_cursor.fetchall()
        print(f"{len(rows)} registros encontrados na tabela {tabela} do PostgreSQL.")

        for row in rows:
            cd_agencia = row[0]  # Assumindo que a primeira coluna é cd_agencia

            # Verifica se a agência já existe no MySQL
            select_query = f"SELECT COUNT(*) FROM {tabela} WHERE cd_agencia = %s"
            mysql_cursor.execute(select_query, (cd_agencia,))
            exists = mysql_cursor.fetchone()[0]
            
            if exists:
                # Se a agência existe, não faz nada
                print(f"Agência {cd_agencia} já existe. Nenhuma ação necessária.")
            else:
                # Se a agência não existe, faz um INSERT
                placeholders = ', '.join(['%s'] * len(row))
                insert_query = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({placeholders})"
                mysql_cursor.execute(insert_query, row)
                print(f"Agência {cd_agencia} inserida com sucesso.")

        print(f"Transferência de dados da tabela {tabela} concluída.")
    except Exception as e:
        print(f"Erro ao transferir dados da tabela {tabela}: {e}")

def transferir_liberacao(pg_cursor, mysql_cursor, tabela, colunas, query=None):
    try:
        if query:
            pg_cursor.execute(query)
        else:
            pg_cursor.execute(f"SELECT {', '.join(colunas)} FROM {tabela}")

        rows = pg_cursor.fetchall()
        print(f"{len(rows)} registros encontrados na tabela {tabela} do PostgreSQL.")

        for row in rows:
            agencia_lote = row[0]  # Assumindo que a primeira coluna é agencia_lote

            # Verifica se a liberação já existe no MySQL
            select_query = f"SELECT COUNT(*) FROM {tabela} WHERE agencia_lote = %s"
            mysql_cursor.execute(select_query, (agencia_lote,))
            exists = mysql_cursor.fetchone()[0]
            
            if exists:
                # Se a liberação já existe, faz um UPDATE
                set_clause = ', '.join([f"{col}=%s" for col in colunas[1:]])  # Pula o agencia_lote
                update_query = f"UPDATE {tabela} SET {set_clause} WHERE agencia_lote = %s"
                mysql_cursor.execute(update_query, (*row[1:], agencia_lote))
            else:
                # Se a liberação não existe, faz um INSERT
                placeholders = ', '.join(['%s'] * len(row))
                insert_query = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({placeholders})"
                mysql_cursor.execute(insert_query, row)
                print(f"Liberação {agencia_lote} inserida com sucesso.")

        print(f"Transferência de dados da tabela {tabela} concluída.")
    except Exception as e:
        print(f"Erro ao transferir dados da tabela {tabela}: {e}")

def main():
    # Obter a data e hora atual e calcular a última sincronização com base no intervalo de 1 hora
    agora = datetime.now()
    ultima_data_sincronizacao = (agora - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')

    # Conectando ao PostgreSQL
    pg_conn, pg_cursor = conectar_postgres()
    if pg_conn is None or pg_cursor is None:
        return

    # Conectando ao MySQL
    mysql_conn, mysql_cursor = conectar_mysql()
    if mysql_conn is None or mysql_cursor is None:
        pg_conn.close()
        return

    # Consulta complexa para lote_carregamento
    lote_query = f"""
           SELECT DISTINCT lc.cd_lote,
           c.nm_cidade AS cidade_origem,
           c2.nm_cidade AS cidade_destino,
           lc.dt_lote,
           lcv.vl_tarifa_empresa as "Frete Empresa",
           lcv.vl_tarifa_motorista as "Frete Motorista",
           p2.nm_pessoa as "Pagador",
           lc.dt_fechamento,
           lcp.cd_produto,
           lcp.cd_unidade,
           lcp.qt_produto,
           a.nm_agencia,
           p2.nm_pessoa AS cliente,
           CASE
               WHEN lc.ds_lote IS NULL THEN 'Sem Observações'
               ELSE lc.ds_lote
           END AS ds_observacao,
           CASE
               WHEN coc.dt_inicio IS NULL THEN '9999-12-31'
               ELSE coc.dt_inicio
           END AS dt_inicio,
           CASE
               WHEN coc.dt_termino IS NULL THEN '9999-12-31'
               ELSE coc.dt_termino
           END AS dt_termino,
           CASE
               WHEN lc.id_situacao = 2 THEN 'Em Andamento'
               WHEN lc.id_situacao = 1 THEN 'Suspenso'
               WHEN lc.id_situacao = 3 THEN 'Suspenso para Cte'
               WHEN lc.id_situacao = 0 THEN 'Encerrado'
           END AS situacao,
           CASE
               WHEN lc.dt_atualizacao IS NULL THEN lc.dt_lote
               ELSE lc.dt_atualizacao
           END AS dt_atualizacao,
           lc.ds_observacao_lote_carregamento AS saldo,
           lc.ds_observacao_ordem_carregamento as rota,
           lcv.vl_tarifa_empresa as valor,
           p3.nm_pessoa as criado_alterado,
           lc.ds_observacao_interna
        FROM lote_carregamento lc
        LEFT JOIN lote_carregamento_produto lcp ON lcp.cd_lote = lc.cd_lote
        LEFT JOIN lote_carregamento_agencia lca ON lca.cd_lote = lc.cd_lote
        LEFT JOIN lote_carregamento_valor lcv ON lcv.cd_lote = lc.cd_lote 
        LEFT JOIN pessoa p ON p.cd_pessoa = lc.cd_pessoa_filial
        LEFT JOIN pessoa p2 ON p2.cd_pessoa = lc.cd_pessoa_pagador
        LEFT JOIN pessoa p3 ON p3.cd_pessoa = lc.cd_pessoa_usuario 
        LEFT JOIN cliente_observacao_ctrc coc ON coc.cd_pessoa = p2.cd_pessoa
        LEFT JOIN cidade c ON c.cd_cidade = lc.cd_cidade_origem
        LEFT JOIN cidade c2 ON c2.cd_cidade = lc.cd_cidade_destino
        INNER JOIN agencia a ON a.cd_agencia = lc.cd_agencia
        WHERE 
            CASE
                WHEN lc.dt_atualizacao IS NULL THEN lc.dt_lote
                ELSE lc.dt_atualizacao
            END >= '{ultima_data_sincronizacao}'
            AND (
                lc.id_situacao IN (1, 2, 3) -- Suspenso, Em Andamento, Suspenso para Cte
                OR (lc.id_situacao = 0 AND (lc.dt_fechamento IS NULL OR lc.dt_fechamento >= '2024-08-01'))
                )
            """

    # Transferir dados da consulta complexa
    transferir_dados(pg_cursor, mysql_cursor, "lote",[
        "cd_lote", "cidade_origem", "cidade_destino", 
        "dt_lote", "frete_empresa", "frete_motorista",
        "pagador", "dt_fechamento", "cd_produto", 
        "cd_unidade", "qt_produto", "agencia", 
        "cliente", "ds_observacao", "dt_inicio", "dt_termino",
        "situacao","dt_atualizacao", "saldo", "rota", "valor", "criado_alterado", "ds_observacao_interna"
    ], lote_query)

    # Transferir dados da tabela de agência
    agencia_query = "SELECT cd_agencia, nm_agencia FROM agencia"
    transferir_agencias(pg_cursor, mysql_cursor, "agencia", ["cd_agencia", "nm_agencia"], agencia_query)

    # Transferir dados da tabela liberacao
    lca_query = f"""SELECT DISTINCT CONCAT(lca.cd_agencia, '-', lca.cd_lote) AS agencia_lote ,
                    lca.cd_agencia as agencia,
                    lca.cd_lote
                    FROM lote_carregamento_agencia lca
                    LEFT JOIN lote_carregamento lc ON lc.cd_lote = lca.cd_lote
                    WHERE lc.dt_atualizacao >= '{ultima_data_sincronizacao}'"""
    
    transferir_liberacao(pg_cursor, mysql_cursor, "liberacao", [
        "agencia_lote", "agencia", "cd_lote"  # Ajuste as colunas conforme necessário
    ], lca_query)

    # Confirmar as mudanças no MySQL
    try:
        mysql_conn.commit()
        print("Alterações confirmadas no MySQL.")
    except Exception as e:
        print(f"Erro ao confirmar alterações no MySQL: {e}")

    # Fechar conexões
    try:
        pg_cursor.close()
        pg_conn.close()
        mysql_cursor.close()
        mysql_conn.close()
        print("Conexões fechadas com sucesso.")
    except Exception as e:
        print(f"Erro ao fechar conexões: {e}")

if __name__ == "__main__":
    main()
