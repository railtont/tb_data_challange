import psycopg2
#Amazon Redshift connect string 
conn_string = "dbname='***' port='5439' user='***' password='***' host='mycluster.***.redshift.amazonaws.com'"  
#connect to Redshift (database should be open to the world)
con = psycopg2.connect(conn_string);
sql="""COPY %s FROM '%s' credentials 
      'aws_access_key_id=%s; aws_secret_access_key=%s'
       delimiter '%s' FORMAT CSV %s %s; commit;""" % 
      (to_table, fn, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,delim,quote,gzip)

cur = con.cursor()
cur.execute(sql)
con.close() 

def get_rs_conn():
    pass

# https://aws.amazon.com/pt/blogs/big-data/using-the-amazon-redshift-data-api-to-interact-with-amazon-redshift-clusters/