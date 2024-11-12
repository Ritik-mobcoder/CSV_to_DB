import pandas as pd
from sqlalchemy import create_engine, text

def database():
    NAME = "Ritik_db"
    HOST = "localhost"
    USER = "postgres"
    PASSWORD = "sa123"
    engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}/{NAME}")
    return engine


def csv_to_sql():
    try:
        engine = database()
        data = pd.read_csv("tips.csv", index_col="total_bill")
        df = pd.DataFrame(data)
        df.to_sql(name="tips", con=engine, if_exists="replace")
        print("Data Send successfully into DB")
    except Exception as e:
        print("Error", e)


# csv_to_sql()
def days():
    try:
        engine = database()
        connection = engine.connect()
        query = text("CREATE TABLE if not exists days (id INT PRIMARY KEY, day VARCHAR(255))")
        connection.execute(query)
        connection.commit()
        query_insert = text(
            "INSERT INTO days if not exists (id, day) VALUES (1, 'Sun'), (2, 'Mon'), (3, 'Tue'), (4, 'Wed'), (5, 'Thur'), (6, 'Fri'), (7, 'Sat')"
        )
        connection.execute(query_insert)
        connection.commit()
        connection.close()
        print("Table created and data inserted")
    except Exception as e:
        print("Error", e)


def alter_table():
    try:
        engine = database()
        connection = engine.connect()
        query = text("ALTER TABLE gender ADD PRIMARY KEY (ID)")
        connection.execute(query)
        connection.commit()
        query = text(
            "ALTER TABLE result ADD foreign KEY (sex_id) references gender(ID)"
        )
        connection.execute(query)
        connection.commit()
        print(" Gender Table updated Successfully")
        # for time table
        query = text("ALTER TABLE times ADD PRIMARY KEY (ID)")
        connection.execute(query)
        connection.commit()
        query = text(
            "ALTER TABLE result ADD foreign KEY (time_id) references times(ID)"
        )
        connection.execute(query)
        connection.commit()
        print("Time Table updated Successfully")
        # for day table
        query = text("ALTER TABLE result ADD foreign KEY (day_id) references days(ID)")
        connection.execute(query)
        connection.commit()
        print("Day Table updated Successfully")
        connection.close()
    except Exception as e:
        print("Error", e)


def main():
    try:
        # DB connection
        engine = database()
        connection = engine.connect()
        query = text("Drop table IF EXISTS result")
        connection.execute(query)
        connection.commit()

        # read the CSV
        data = pd.read_csv("tips.csv")
        df = pd.DataFrame(data)
        df.set_index("total_bill")

        # converting the total_bill ito paisa if null insert mean value
        mean_value = df["total_bill"].mean()
        df["total_bill"] = df["total_bill"].fillna(mean_value) * 100
        # print(df["total_bill"])

        # converting the  tip ito paisa if null insert mean value
        mean_value = df["tip"].mean()
        df["tip"] = df["tip"].fillna(mean_value) * 100

        # uniqe value for sex  coorect the code fot this
        unique_gender = df["sex"].unique()
        id = [i for i in range(1, len(unique_gender) + 1)]
        gender_data = [[a, b] for a, b in zip(id, unique_gender)]
        columns = ["id", "gender"]
        df_gender = pd.DataFrame(gender_data, columns=columns)
        # print(df_gender)
        df_gender.to_sql(name="gender", con=engine, if_exists="replace")
        print("Data sent successfully into DB table gender")

        # for the Day
        days()

        # uniqe value for time  coorect the code fot this
        unique_time = df["time"].unique()
        id = [i for i in range(1, len(unique_time) + 1)]
        time = [[a, b] for a, b in zip(id, unique_time)]
        columns = ["id", "time"]
        df_time = pd.DataFrame(time, columns=columns)
        # print(df_time)
        df_time.to_sql(name="times", con=engine, if_exists="replace")
        print("Data sent successfully into DB table time")

        day_df = pd.read_sql("days", con=engine)
        print(day_df)
        day_dict = day_df.to_dict()["day"]
        day_dict = {val: key + 1 for key, val in day_dict.items()}
        print(day_dict)

        time_df = pd.read_sql("times", con=engine)
        print(time_df)
        time_dict = time_df.to_dict()["time"]
        time_dict = {val: key + 1 for key, val in time_dict.items()}
        print(time_dict)

        #  Create a result
        result_data = pd.DataFrame(
            {
                "total_bill": df["total_bill"],
                "tip": df["tip"],
                "smoker": df["smoker"],
                "sex_id": df["sex"].map(df_gender.set_index("gender")["id"]),
                "day_id": df["day"].map(day_dict),
                "time_id": df["time"].map(time_dict),
                "size": df["size"],
            }
        )
        print("result", result_data)
        result_data.to_csv("Result.csv", index=0)
        print("Data sent successfully into csv ")
        # print(df)

        result_data.to_sql(name="result", con=engine, if_exists="replace")

        print("Data sent successfully into DB")
        print(df)

    except Exception as e:
        print("Error:", e)


main()
# updating the table set the primary key in gender, times table and foreign key in times, gender, days table
alter_table()
