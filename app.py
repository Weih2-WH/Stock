import os
import psycopg2
from dotenv import load_dotenv
from flask import Flask,request
from datetime import datetime, timedelta,timezone
import yfinance as yf
import numpy as np
#import time
from flask_apscheduler import APScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from dateutil.relativedelta import relativedelta
from sklearn.metrics import mean_squared_error
from decimal import Decimal

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
SELECT_PRE_HISTORY_TABLE = ("""select company,opendate,open,high,low,close,diff,volume from history where company = %s and opendate > %s """)
SELECT_HISTORY_TABLE = ("""select company,opendate,open,high,low,close,diff,volume from history where company = %s and opendate = %s """)


CREATE_TRANSAC_TABLE = """create table if not exists transaction(id SERIAL PRIMARY KEY, client INT, tDate TIMESTAMP, company TEXT,
                        status char(1),price real, volume real); """
INSERT_TRANSAC_TABLE = "INSERT INTO transaction (client,tDate,company,status,price,volume) VALUES (%s,%s,%s,%s,%s,%s);"
SELECT_TRANSAC_TABLE = "Select * from transaction where client = %s ;"

CREATE_TRANSAC_TABLE = """create table if not exists predict(id SERIAL PRIMARY KEY, create_date TIMESTAMP, company TEXT,
                        price real); """
INSERT_PREDICT_TABLE = "INSERT INTO predict (create_date,company,price) VALUES (%s,%s,%s);"
SELECT_PREDICT_TABLE = "select * from  predict where company = %s and create_date = %s;"
DELETE_PREDICT_TABLE = "DELETE FROM predict where company = %s and create_date = %s;"

"""
    The functions needed to be run daily.
"""
def daily():
    upsert_history()
    training_model()


"""
    After the system started, which will load the 3 days price for each company and upsert into DB everyday 6pm.
    Parameter: 
                company : list : The target companies.
    * To limit the storage usage, only included 4 companyies
"""
def upsert_history():
    company = ['GOOG','MSFT','META','NOW','AMZN']
    his = yf.download(company, period='3d',group_by='ticker')
    #handle the missing value
    his.fillna(0)
    for c in company:
        data = his[c]
        for ind in data.index:
            date = datetime.strptime(str(ind)[:10], "%Y-%m-%d")
            Open = Decimal(data['Open'][ind])
            high = Decimal(data['High'][ind])
            low = Decimal(data['Low'][ind])
            close = Decimal(data['Close'][ind])
            diff = high - low
            volume = int(data['Volume'][ind])
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(DELETE_HISTORY_ROW,(c,date))
                    cursor.execute(INSERT_HISTORY_TABLE,(c,date,Open,high,low,close,diff,volume))
    return {"message": "3 days data insert success"}, 201

"""
    Train model and predict the price.
    Parameter: 
                company : list : The target companies.
    * To limit the storage usage, only included 4 companyies
"""
def training_model():
    threeMonth = datetime.now() + relativedelta(months=-6)
    company = ['GOOG','MSFT','META','NOW','AMZN']
    for c in company:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(SELECT_PRE_HISTORY_TABLE,(c,threeMonth))
                result = cursor.fetchall()

        # Parameters for predict
        pre = result.pop(-1)

        # Prepare the data for training the model(Use yesterday's data to predict today's data)
        X = np.array([[Decimal(data[2]), Decimal(data[3]),Decimal(data[4]),Decimal(data[6]),int(data[7])] for data in result])
        Y = np.array([Decimal(data[5]) for data in result[1:]])
        Y = np.concatenate((Y, np.array([Decimal(pre[5])])))


        # Split the data into training and testing sets
        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

        # Train the linear regression model
        model = LinearRegression()
        model.fit(X_train, Y_train)
        Y_pred = model.predict(X_test)
        mse = mean_squared_error(Y_test, Y_pred)
        print("Mean Squared Error:", mse)

        # predict
        X_pre = np.array([[Decimal(pre[2]), Decimal(pre[3]),Decimal(pre[4]),Decimal(pre[6]),int(pre[7])]])
        Y_pre = model.predict(X_pre)

        # insert predict price to predict table
        insert_predict(datetime.now().strftime('%Y-%m-%d'),c,Y_pre.item())

    return {"message": "Train model and predict price success"}, 201

"""
    Record predict price.
    Parameter:
            tDate : datetime
            company : str
            price : decimal(8,2)
"""
def insert_predict(tDate,company,price):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(DELETE_PREDICT_TABLE,(company,tDate))
            cursor.execute(INSERT_PREDICT_TABLE,(tDate,company,price))
    return {"message": "Predict Successful"}, 201


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
@app.get("/api/getStock")
def getStock():
    date = request.args.get("date")
    company = request.args.get("company")
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_HISTORY_TABLE,(company,date))
            result = cursor.fetchone()
    return {"company":company,"date":date,"result":result,"message": "Select success."}, 201


"""
    Getting predict
    GET:
    Parameter:
            date : datetime
            client : int
"""
@app.get("/api/getPredict")
def getPredict():
    date = request.args.get("date")
    company = request.args.get("company")
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_PREDICT_TABLE,(company,date))
            result = cursor.fetchone()
    return {"date":date,"company":company,"result":result,"message": "Select success."}, 201


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
@app.post("/api/insert_transac")
def insert_transac():
    data = request.get_json()
    client  = data['client']
    tDate  = datetime.strptime(data['tDate'], "%Y-%m-%d %H:%M:%S")
    company = data["company"]
    status = data["status"]
    price  = data["price"]
    volume  = data["volume"]

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_TRANSAC_TABLE,(client,tDate,company,status,price,volume))
    return {"message": "Transaction Successful"}, 201

"""
    Getting transac
    GET:
    Parameter:
            date : datetime
            client : int
"""
@app.get("/api/getTransac")
def getTransac():
    client = request.args.get("client")
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_TRANSAC_TABLE,(client))
            result = cursor.fetchone()
    return {"client":client,"result":result,"message": "Select success."}, 201


if __name__ == '__main__':
     # trigger every minute for testing the func
     # To run on everyday 6pm -> replace the trigger CronTrigger(hour=18)
    trigger = IntervalTrigger(minutes=1)
    scheduler.add_job(id = "daily",func=daily,trigger=trigger,replace_existing=True) 
    scheduler.start()
    app.run(debug=False)

