import psycopg2

def get_pg_conn(dbname="redshiftdb", user="redshift", host="host", port="1234", password="pwd"):
    conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port)
    return conn