import csv
import mysql.connector

connection = mysql.connector.connect(
    host="localhost",
    port=3307,
    user="root",
    password="airflow",
    database="olympic_dataset"
)

cursor = connection.cursor()

with open("athlete_event_results.csv", "r", encoding="utf-8") as file:
    csv_reader = csv.reader(file)
    next(csv_reader)  # skip header

    for row in csv_reader:
        cursor.execute("""
            INSERT INTO athlete_event_results
            (id, name, sex, age, team, noc, year, season, city, sport, event, medal)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, row)

connection.commit()
cursor.close()
connection.close()

print("✔ Import completed!")
