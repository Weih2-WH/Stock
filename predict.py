import psycopg2
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import joblib
import numpy as np

#DB connect
connection = psycopg2.connect('postgres://xvawgvoc:vUUjoLexRS_qcARx3VGJmE50uDospJwY@rajje.db.elephantsql.com/xvawgvoc')
select_query = """
SELECT * FROM history;
"""
with connection.cursor() as cursor:
    cursor.execute(select_query)
    data_points = cursor.fetchall()

# Prepare the data for training the model
X = np.array([[data[3], data[4],data[5],data[7],data[8]] for data in data_points])
y = np.array([data[6] for data in data_points])

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train the linear regression model
model = LinearRegression()
model.fit(X_train, y_train)

# Evaluate the model
y_pred = model.predict(X_test)
print(y_pred)
mse = mean_squared_error(y_test, y_pred)

print("Mean Squared Error:", mse)

# Save the trained model
joblib.dump(model, "predict.pkl")
