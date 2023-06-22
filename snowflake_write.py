import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd


conn = snowflake.connector.connect(
    #user = "egonzalez@arrivelogistics.com",
    user="bgarvin@arrivelogistics.com",
    authenticator='externalbrowser',
    account="arrive.east-us-2.azure",
    warehouse="DATA_READER_WH",
    database="CORE_DATA",
    schema="CORE"
    )

con_interns = snowflake.connector.connect(
    #user = "egonzalez@arrivelogistics.com",
    user="bgarvin@arrivelogistics.com",
    authenticator='externalbrowser',
    account="arrive.east-us-2.azure",
    warehouse="ADMIN_WH",
    database="DAPL_RAW_DEV",
    schema="DE_INTERNS"
)

cur = conn.cursor()
table = "JAN_EXPORTS_AND_TOPSPEND"

try:
    cur.execute("SELECT cast(created_on as date) AS Load_Date,SUM(TOP_SPEND) AS Total_Top_Spend, COUNT(*) AS Number_Of_Loads FROM CORE_DATA.CORE.LOADS WHERE cast(created_on as date ) between '2023-01-01' and '2023-01-31' GROUP BY cast(created_on as date) order by cast(created_on as date) asc;").fetch_pandas_all().to_csv("snowflakeDataSUM.csv")
    snowflakeData = pd.read_csv('snowflakeDataSUM.csv')
    write_pandas(con_interns, snowflakeData, table, auto_create_table=True)
    query = f"select * from {table};"
    cur.execute(query)
    opt = cur.fetch_pandas_all()
finally:
    cur.close()
conn.close()