#Snowflake connector
import snowflake.connector


#integrate data into snowflake table
con = snowflake.connector.connect(
    user="bgarvin@arrivelogistics.com",
    authenticator='externalbrowser',
    account="arrive.east-us-2.azure",
    warehouse="DATA_READER_WH",
    database="CORE_DATA",
    schema="CORE"
    )

cur = con.cursor()

try:
    cur.execute('SELECT * FROM LOADS LIMIT 1000').fetch_pandas_all().to_csv("table.csv")
finally:
    cur.close()
con.close()