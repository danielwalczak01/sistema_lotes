import os
import psycopg2
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

def conectar_postgres():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT")
        )
        return conn, conn.cursor()
    except Exception as e:
        raise Exception(f"Erro ao conectar no PostgreSQL: {e}")

def conectar_mysql():
    try:
        conn = mysql.connector.connect(
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            host=os.getenv("MYSQL_HOST"),
            database=os.getenv("MYSQL_DB"),
            port=int(os.getenv("MYSQL_PORT"))
        )
        return conn, conn.cursor()
    except mysql.connector.Error as e:
        raise Exception(f"Erro ao conectar no MySQL: {e}")
