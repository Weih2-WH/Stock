import os
import psycopg2
from dotenv import load_dotenv
from flask import Flask,request
from datetime import datetime, timezone
import yfinance as yf
#import numpy as np
#import time
from flask_apscheduler import APScheduler
from apscheduler.triggers.interval import IntervalTrigger

#read the .env variables
load_dotenv()
app = Flask(__name__)
scheduler = APScheduler()

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
CREATE_CLIENT_TABLE = """create table if not exists transaction(id SERIAL PRIMARY KEY, client INT, tDate TIMESTAMP, company TEXT,
                        status char(1),price real, volume real); """
INSERT_TRANSAC_TABLE = "INSERT INTO transaction (client,tDate,company,status,price,volume) VALUES (%s,%s,%s,%s,%s,%s);"

"""
    After the system started, which will load the 3 days price for each company and upsert into DB every 24 hrs.
    Parameter: 
                company : list : The target companies.
"""

def upsert_history():
    company = ['GOOG','MSFT','META','NOW']
    his = yf.download(company, period='3d',group_by='ticker')
    #handle the missing value
    his.fillna(0)
    for c in company:
        data = his[c]
        for ind in data.index:
            date = datetime.strptime(str(ind)[:10], "%Y-%m-%d")
            Open = float(data['Open'][ind])
            high = float(data['High'][ind])
            low = float(data['Low'][ind])
            close = float(data['Close'][ind])
            diff = high - low
            volume = float(data['Volume'][ind])
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(DELETE_HISTORY_ROW,(c,date))
                    cursor.execute(INSERT_HISTORY_TABLE,(c,date,Open,high,low,close,diff,volume))
    return {"message": "3 days data insert success"}, 201



@app.get("/")
def home():
    return "Hello, Flask!"

"""
    Getting stock price
    GET:
    Parameter:
            date : datetime
            company : str
"""
@app.get("/api/get")
def select_data():
    date = request.args.get("date")
    company = request.args.get("company")
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_HISTORY_TABLE,(company,date))
            result = cursor.fetchone()
    return {"company":company,"date":date,"result":result,"message": "Select success."}, 201

"""
    Record client's transaction (buy/sell stock).
    POST:
    Parameter:
            client : int
            tDate : datetime
            company : str
            status : char
            price : decimal(8,2)
            volume : int
"""
@app.post("/api/transac")
def insert_transac():
    data = request.get_json()
    client  = float(data['client'])
    tDate  = datetime.strptime(data['tDate'], "%Y-%m-%d %H:%M:%S")
    company = data["company"]
    status = data["status"]
    price  = data["price"]
    volume  = data["volume"]

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_TRANSAC_TABLE,(client,tDate,company,status,price,volume))
    return {"message": "Transaction Successful"}, 201



if __name__ == '__main__':
     # trigger every minute for testing the func
     # To run on everyday 6pm -> replace the trigger CronTrigger(hour=18)
    trigger = IntervalTrigger(minutes=1)
    scheduler.add_job(id = "upsert_history",func=upsert_history,trigger=trigger,replace_existing=True) 
    scheduler.start()
    app.run(debug=False)