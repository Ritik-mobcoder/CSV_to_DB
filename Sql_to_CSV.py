import pandas as pd
from sqlalchemy import create_engine, text


def database():
    NAME = "Ritik_db"
    HOST = "localhost"
    USER = "postgres"
    PASSWORD = "sa123"
    engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}/{NAME}")
    return engine


def main():
    try:
        engine = database()
        connection = engine.connect()
        query = text(
            "SELECT r.total_bill, r.tip, g.gender, r.smoker, days.day, times.time, r.size FROM result AS r JOIN gender AS g ON r.sex_id = g.id join days on r.day_id = days.id join times on r.time_id = times.id")
        result = connection.execute(query)
        result = list(result)
        df = pd.DataFrame(result)
        df.to_csv("result_2.csv", index=0)
        connection.commit()
        connection.close()
    except Exception as e:
        print("Error", e)
main()
