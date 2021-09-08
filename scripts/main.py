import io
import os
import pandas
import importlib
from datetime import datetime, timedelta

from connections.postgres import get_pg_conn
from connections.redshift import get_rs_conn
from connections.s3 import get_s3_conn

AWS_ACCESS_KEY_ID = "<access_key_id>" 
AWS_SECRET_ACCESS_KEY = "<secret_access_key>"
AWS_BUCKET_S3 = "s3://tinybytes"

def get_data_from_postgres(pg_conn, filter_date=None):
    filter = "" 
    if filter_date is not None:
        filter = "where '{column_name} = '{value}'"
        filter = filter.format(
            column_name=filter_date["filter_column"],
            value=filter_date["filter_value"]   
        )

    sql_query = f"""
        select *
        from LOGINS
        {filter}
    """     
    df = pandas.read_sql_query(sql=sql_query, con=pg_conn)
    return df

def get_s3_key(table_name, filter_date):
    date_partition = ""
    if filter_date is not None:
        date_partition = filter_date["filter_column"] + "=" + filter_date["filter_value"]

    return os.path.join(table_name.lower(), date_partition, "data.csv")

def load_data_into_s3(s3_conn, df, table_name, filter_date=None):
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer)
        response = s3_conn.put_object(
            Bucket=AWS_BUCKET_S3,
            Key=get_s3_key(filter_date),
            Body=csv_buffer.getvalue()
        )

def load_data_into_redshift(rs_conn, table_name, filter_date=None, overwrite_table=True):  
    # DROP TABLE FOR OVERWRINTING
    if overwrite_table:
        sql_query = f"TRUNCATE {table_name}"
        rs_conn.execute(sql_query)

    # COPY DATA S3 INTO REDSHIFT
    s3_path = os.path.join(AWS_BUCKET_S3, get_s3_key(filter_date))
    sql_query = f"""
        COPY {table_name}
        FROM '{s3_path}'
        credentials 'aws_access_key_id={AWS_ACCESS_KEY_ID};aws_secret_access_key={AWS_SECRET_ACCESS_KEY}'
        CSV
    """
    rs_conn.execute(sql_query)
    rs_conn.commit()


def postgres_to_redshift_ingestion(
    postgres_table_name, 
    pg_conn,
    s3_conn,
    rs_conn,
    filter_date=None,
    is_incremental_ingestion=False
):
    df = get_data_from_postgres(
        pg_conn=pg_conn, filter_date=filter_date
    )
    load_data_into_s3(
        s3_conn=s3_conn, 
        df=df, 
        table_name=postgres_table_name.lower(), 
        filter_date=filter_date
    )
    load_data_into_redshift(
        rs_conn=rs_conn, 
        table_name=postgres_table_name.lower(),
        filter_date=filter_date,
        overwrite_table= not is_incremental_ingestion
    )
   
def redshift_create_table(rs_conn, table_name):
    query_path = "utils.create_tables.get_create_{table_name}_query"
    get_create_table_query = importlib.import_module(query_path)
    sql_query = get_create_table_query()
    rs_conn.execute(sql_query)
    rs_conn.commit()

def redshift_calculate_table(rs_conn, table_name):
    query_path = "utils.calculate_table.get_calculate_{table_name}_query"
    get_calculate_table_query = importlib.import_module(query_path)
    sql_query = get_calculate_table_query()
    rs_conn.execute(sql_query)
    rs_conn.commit()

if __name__ == "__main__":
    pg_conn= get_pg_conn()
    s3_conn= get_s3_conn()
    rs_conn= get_rs_conn()

    WHOLE_INGESTION_TABLES = ["USER_COUNTRY", "USER_INFO"]
    INCREMENTAL_DAILY_INGESTION_TABLES = [
        {
            "table_name": "LOGINS", 
            "date_column": "login_ts"
        }        
    ]
    
    # # # INGESTION - FROM POSTGRES TO REDSHIFT # # # 
    # OVERWRITE COMPLETE TABLE INGESTION (USER_COUNTRY and USER_INFO)
    for table in WHOLE_INGESTION_TABLES:
        redshift_create_table(table)
        postgres_to_redshift_ingestion(
            postgres_table_name=table,
            pg_conn= pg_conn,
            s3_conn= s3_conn, 
            rs_conn= rs_conn
        )
   
    # INCREMENTAL DAILY INGESTION (LOGINS)
    for infos in INCREMENTAL_DAILY_INGESTION_TABLES:
        redshift_create_table(infos["table_name"])
        postgres_to_redshift_ingestion(
            postgres_table_name=infos["table_name"],
            filter_date= {
                "filter_value": infos.get( # Day-1 Ingestion
                    "date",
                    (datetime.utcnow() - timedelta(days=1)).date().isoformat()
                ),
                "filter_column": infos["date_column"]
            },
            pg_conn= pg_conn,
            s3_conn= s3_conn,
            rs_conn= rs_conn,
            is_incremental_ingestion=True
        )

    # # # TRANSFORMATIONS - REDSHIFT PROCCESS # # #
    CREATA_AND_CALCULATE_TABLES = ["users_login_info", "logins_date_summarize", "first_login_summarize"]
    for table in CREATA_AND_CALCULATE_TABLES:
        redshift_create_table(rs_conn=rs_conn, table_name=table)
        redshift_calculate_table(rs_conn=rs_conn, table_name=table)

