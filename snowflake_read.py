import snowflake.connector

conn = snowflake.connector.connect(
    user="egonzalez@arrivelogistics.com",
    authenticator='externalbrowser',
    account="arrive.east-us-2.azure",
    warehouse="DATA_READER_WH",
    database="CORE_DATA",
    schema="CORE"
    )

cur = conn.cursor()

try:
    cur.execute("select top 1000 * from CORE_DATA.CORE.LOADS").fetch_pandas_all().to_csv("snowflakeData.csv")
finally:
    cur.close()
conn.close()