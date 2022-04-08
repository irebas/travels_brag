from variables import *
import pandas as pd
import sqlite3
import numpy as np


def read_csv(path):
    """
    :param path: path to the csv file
    :type path: string
    :return: 4 lists - travels, stages, users and cities
    :rtype: list
    """

    # read csv
    df = pd.read_csv(path, sep='|')
    # create rank of sections. Equivalent of SQL Window Function MAX(section_order)
    # OVER (PARTITION BY travel_description, travel_date)
    df['rank'] = df.groupby(['travel_description', 'travel_date'])['section_order'].transform(
        lambda x: x == np.amax(x)).astype(bool)
    # new columns representing start_city and end_city of travel
    df['start_city'] = np.where(df['section_order'] == 1, df['city_from'], '0')
    df['end_city'] = np.where(df['rank'] == True, df['city_to'], '0')
    # data frames grouping start_city and end_city, renaming columns and joining to basic data frame
    df_start_city = df.groupby(['travel_description', 'travel_date'])['start_city'].transform('max').reset_index()
    df_end_city = df.groupby(['travel_description', 'travel_date'])['end_city'].transform('max').reset_index()
    df_start_city = df_start_city.rename(columns={'start_city': 'travel_from'}, inplace=False)
    df_end_city = df_end_city.rename(columns={'end_city': 'travel_to'}, inplace=False)
    df = df.join(df_start_city['travel_from'])
    df = df.join(df_end_city['travel_to'])

    # DataFrame for table Travels - unique routes
    df_travels = pd.DataFrame(df, columns=['travel_description', 'travel_date', 'travel_from', 'travel_to',
                                           'travel_avg_speed_km_h', 'travel_total_distance_km', 'driver_name'],
                              )

    df_travels = df_travels.drop_duplicates()
    list_travels = df_travels.values.tolist()

    # Data frame for table Cities - unique cities
    df_cities = pd.concat([pd.DataFrame(df['city_from']).rename(columns={'city_from': 'city'}),
                           pd.DataFrame(df['city_to']).rename(columns={'city_to': 'city'})]).drop_duplicates(
        ignore_index=True)
    list_cities = df_cities['city'].values.tolist()

    # Data frame for table Users
    # Split passengers into several columns (max = 4)
    df_users = df['other_passengers'].str.split(',', expand=True, n=4)
    # all users list - list of lists of users
    all_users_lists = []
    for col in df_users:
        all_users_lists.append(df_users[col].values.tolist())
    all_users_lists.append(df['driver_name'].values.tolist())

    # comprehensive list
    list_users = list(set([user for users_list in all_users_lists for user in users_list if user is not None]))

    # DataFrame for table Stages
    list_stages = pd.DataFrame(df.join(df_users)).values.tolist()

    return list_cities, list_users, list_travels, list_stages


def create_connection(db_name):
    """
    :param db_name: name of the database
    :type db_name: string
    :return: connection to database
    :rtype: sqlite3.Connection
    """
    conn = None
    try:
        conn = sqlite3.connect(db_name)
    except Exception as e:
        print(e)

    return conn


def query_rows(conn, sql, col_nb):
    """
    :param conn: connection to database
    :type conn: sqlite3 Connection
    :param sql: sql query
    :type sql: string
    :param col_nb: number of column from which data should be retrieved
    :type col_nb: integer
    :return: result of sql query
    :rtype: list
    """
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    result_list = [row[col_nb] for row in rows]
    return result_list


def insert_rows(conn, sql):
    """
    :param conn: connection to database
    :type conn: sqlite3 Connection
    :param sql: sql query
    :type sql: string
    :return: None
    :rtype: None
    """
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def main():
    conn = create_connection(DB_NAME)
    cities_list_db = query_rows(conn, SQL_CITIES, 1)
    users_list_db = query_rows(conn, SQL_USERS, 1)
    results_csv = read_csv(PATH)
    cities_csv = results_csv[0]
    users_csv = results_csv[1]
    travels_csv = results_csv[2]
    stages_csv = results_csv[3]
    new_cities_list = [city for city in cities_csv if city not in cities_list_db]
    new_users_list = [user for user in users_csv if user not in users_list_db]

    # We are adding new cities/users to appropriate tables
    for city in new_cities_list:
        insert_rows(conn, f'''INSERT INTO Cities(city_name) VALUES ("{city}");''')
        print(f"{city} added to Cities table")
    for user in new_users_list:
        insert_rows(conn, f'''INSERT INTO Users(user_name) VALUES ("{user}");''')
        print(f"{user} added to Users table")

    for travel in travels_csv:
        description = travel[0]
        travel_date = travel[1]
        travel_from = travel[2]
        travel_to = travel[3]
        total_distance = travel[4]
        avg_speed = travel[5]
        driver_name = travel[6]
        sql_driver_id = f'''SELECT * FROM Users WHERE user_name = "{driver_name}"'''
        sql_travel_from_id = f'''SELECT * FROM Cities WHERE city_name = "{travel_from}"'''
        sql_travel_to_id = f'''SELECT * FROM Cities WHERE city_name = "{travel_to}"'''
        driver_id = query_rows(conn, sql_driver_id, 0)[0]
        travel_from_id = query_rows(conn, sql_travel_from_id, 0)[0]
        travel_to_id = query_rows(conn, sql_travel_to_id, 0)[0]
        sql = f'''INSERT INTO Travels(description, travel_date, travel_from_id, travel_to_id, 
        total_distance, avg_speed, driver_id) VALUES ("{description}", "{travel_date}", {travel_from_id}, 
        {travel_to_id}, {total_distance}, {avg_speed}, {driver_id})'''
        insert_rows(conn, sql)
        print(f"{description} added to Travels table")

    for stage in stages_csv:
        description = stage[0]
        travel_date = stage[1]
        stage_order = stage[2]
        city_from = stage[3]
        city_to = stage[4]
        sql_city_from_id = f'''SELECT * FROM Cities WHERE city_name = "{city_from}"'''
        sql_city_to_id = f'''SELECT * FROM Cities WHERE city_name = "{city_to}"'''
        city_from_id = query_rows(conn, sql_city_from_id, 0)[0]
        city_to_id = query_rows(conn, sql_city_to_id, 0)[0]
        stage_distance = stage[5]
        stage_time = stage[6]
        stage_avg_speed = stage[7]
        other_passengers = stage[11]
        passenger1_id = 0
        passenger2_id = 0
        passenger3_id = 0
        passenger4_id = 0
        try:
            passenger1_id = query_rows(conn, f'''SELECT * FROM Users WHERE user_name = "{stage[17]}"''', 0)[0]
            passenger2_id = query_rows(conn, f'''SELECT * FROM Users WHERE user_name = "{stage[18]}"''', 0)[0]
            passenger3_id = query_rows(conn, f'''SELECT * FROM Users WHERE user_name = "{stage[19]}"''', 0)[0]
            passenger4_id = query_rows(conn, f'''SELECT * FROM Users WHERE user_name = "{stage[20]}"''', 0)[0]
        except IndexError:
            pass
        sql_travel_id = f'''SELECT * FROM Travels WHERE description = "{description}" AND 
        travel_date = "{travel_date}"'''
        travel_id = query_rows(conn, sql_travel_id, 0)[0]
        sql = f'''INSERT INTO Stages(travel_id, stage_city_from_id, stage_city_to_id, stage_order,
               stage_distance, stage_avg_speed, stage_time, other_passengers, passenger1_id, passenger2_id, 
               passenger3_id, passenger4_id)
               VALUES ({travel_id}, {city_from_id}, {city_to_id}, {stage_order}, {stage_distance},
               {stage_avg_speed}, {stage_time}, "{other_passengers}", {passenger1_id}, {passenger2_id},  
                {passenger3_id}, {passenger4_id})'''
        insert_rows(conn, sql)
        print(f"Stage {stage_order} from travel {description} added to Stages table")


if __name__ == '__main__':
    main()
