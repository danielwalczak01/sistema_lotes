def transferir(pg_cursor, mysql_cursor, tabela, colunas, query, chave_primaria, modo_update=True):
    pg_cursor.execute(query)
    registros = pg_cursor.fetchall()

    for row in registros:
        chave_valor = row[0]
        mysql_cursor.execute(
            f"SELECT COUNT(*) FROM {tabela} WHERE {chave_primaria} = %s", (chave_valor,)
        )
        existe = mysql_cursor.fetchone()[0]

        if existe and modo_update:
            set_clause = ', '.join([f"{col}=%s" for col in colunas[1:]])
            update_query = f"UPDATE {tabela} SET {set_clause} WHERE {chave_primaria} = %s"
            mysql_cursor.execute(update_query, (*row[1:], chave_valor))
        elif not existe:
            placeholders = ', '.join(['%s'] * len(row))
            insert_query = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({placeholders})"
            mysql_cursor.execute(insert_query, row)
