# Create Database
sqlite3 travels_db

CREATE TABLE "Users" (
user_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
user_name VARCHAR
);

CREATE TABLE "Cities" (
city_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
city_name VARCHAR
);

CREATE TABLE "Travels" (
travel_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
description VARCHAR, 
travel_from_id VARCHAR,
travel_to_id VARCHAR,
travel_date DATE,
total_distance FLOAT,
avg_speed FLOAT,
driver_id VARCHAR,
FOREIGN KEY (travel_from_id) REFERENCES "Cities"(city_id),
FOREIGN KEY (travel_to_id) REFERENCES "Cities"(city_id),
FOREIGN KEY (driver_id) REFERENCES "Users" (user_id)
);

CREATE TABLE "Stages" (
stage_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
travel_id INTEGER,
stage_city_from_id VARCHAR,
stage_city_to_id VARCHAR,
stage_order INTEGER,
stage_distance FLOAT,
stage_time FLOAT,
stage_avg_speed FLOAT,
other_passengers VARCHAR,
passenger1_id VARCHAR,
passenger2_id VARCHAR,
passenger3_id VARCHAR,
passenger4_id VARCHAR,
FOREIGN KEY (travel_id) REFERENCES "Travels" (travel_id),
FOREIGN KEY (stage_city_from_id) REFERENCES "Cities" (city_id),
FOREIGN KEY (stage_city_to_id) REFERENCES "Cities" (city_id),
FOREIGN KEY (passenger1_id) REFERENCES "Users" (user_id),
FOREIGN KEY (passenger2_id) REFERENCES "Users" (user_id),
FOREIGN KEY (passenger3_id) REFERENCES "Users" (user_id),
FOREIGN KEY (passenger4_id) REFERENCES "Users" (user_id)
);