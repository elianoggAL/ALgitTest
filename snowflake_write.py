import snowflake.connector
import pandas as pd


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
    cur.execute("SELECT cast(created_on as date),SUM(TOP_SPEND), COUNT(*) FROM CORE_DATA.CORE.LOADS WHERE cast(created_on as date ) between '2023-01-01' and '2023-01-31' GROUP BY cast(created_on as date) order by cast(created_on as date) asc;").fetch_pandas_all().to_csv("snowflakeDataSUM.csv")
finally:
    cur.close()
conn.close()