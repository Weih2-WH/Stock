import os
import psycopg2
from dotenv import load_dotenv
from flask import Flask,request
from datetime import datetime, timezone
import yfinance as yf
import joblib
import time
import numpy as np
import datetime

# Load the trained machine learning model
model = joblib.load("predict.pkl")

company = 'MSFT'

while True:
    # Fetch the latest stock data point
    latest_data = yf.download(company, start=datetime.date.today(), end=datetime.date.today())
    latest_data = latest_data.tail(1)  # Keep only the latest data point

    # Transform the latest data point 
    transformed_data = [[latest_data.iloc[0]['Open'],latest_data.iloc[0]['High'],latest_data.iloc[0]['Low'],latest_data.iloc[0]['High']-latest_data.iloc[0]['Low'],latest_data.iloc[0]['Volume']]]

    # Predict the next day's opening price using the machine learning model
    predicted_price = model.predict(transformed_data)

    # Print the predicted price
    print("Predicted Price for", company, ":", predicted_price)

    time.sleep(30)  # Sleep for 24 hours 86400
