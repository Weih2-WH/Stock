import os
import psycopg2
from dotenv import load_dotenv
from flask import Flask,request
from datetime import datetime, timezone
import yfinance as yf
#read the .env variables
load_dotenv()

app = Flask(__name__)
#DB connect
dburl = os.getenv("DATABASE_URL")
connection = psycopg2.connect(dburl)

#SQL
CREATE_HISTORY_TABLE = """create table if not exists history(id SERIAL PRIMARY KEY, company TEXT, opendate TIMESTAMP, open real,
                            high real, low real, close real,diff real, volume real);
                        """
INSERT_HISTORY_TABLE = "INSERT INTO history (company,opendate,open,high,low,close,diff,volume) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);"
DELETE_HISTORY_ROW = "DELETE FROM history where company = %s and opendate = %s;"
SELECT_HISTORY_TABLE = ("""select company,opendate,open,high,low,close,diff,volume from history where company = %s and opendate = date(%s) """)

@app.get("/")
def home():
    return "Hello, Flask!"

#get the company stock
@app.get("/api/get")
def select_data():
    date = request.args.get("date")
    company = request.args.get("company")
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_HISTORY_TABLE,(company,date))
            result = cursor.fetchone()
    return {"company":company,"date":date,"result":result,"message": "Select success."}, 201

#insert stock history
@app.post("/api/insert")
def insert_history():
    data = request.get_json()
    company = data["company"]
    tmp = yf.Ticker(company).history('5d')
    #handle the missing value
    tmp.fillna(0)
    for ind in tmp.index:
        date = datetime.strptime(str(ind)[:10], "%Y-%m-%d")
        Open = float(tmp['Open'][ind])
        high = float(tmp['High'][ind])
        low = float(tmp['Low'][ind])
        close = float(tmp['Close'][ind])
        diff = high - low
        volume = float(tmp['Volume'][ind])
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(DELETE_HISTORY_ROW,(company,date))
                cursor.execute(INSERT_HISTORY_TABLE,(company,date,Open,high,low,close,diff,volume))
    return {"message": "5 days data insert success"}, 201

