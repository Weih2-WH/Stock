import os
import psycopg2
from dotenv import load_dotenv
from flask import Flask
#read the .env variables
load_dotenv()

app = Flask(__name__)
dburl = os.getenv("DATABASE_URL")
connection = psycopg2.connect(dburl)

#SQL
CREATE_HISTORY_TABLE = """create table if not exists history(id SERIAL PRIMARY KEY, company TEXT, opendate TIMESTAMP, open real,
                            high real, low real, close real, volume real);
                        """
INSERT_HISTORY_TABLE = "INSERT INTO history (company,opendate,open,high,low,close,volume) VALUES (%s,%s,%s,%s,%s,%s,%s);"

SELECT_HISTORY_TABLE = ("""select company,opendate,open,high,low,close,volume from history where company = 'MSFT' """)

@app.get("/")
def home():
    return "Hello, Flask!"


@app.post("/api/create")
def create_table():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_HISTORY_TABLE)
    return {"message": "Table created."}, 201

@app.get("/api/create")
def create_table():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_HISTORY_TABLE)
    return {"message": "Table created."}, 201
