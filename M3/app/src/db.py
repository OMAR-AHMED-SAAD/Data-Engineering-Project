from sqlalchemy import create_engine
import pandas as pd
import time

engine = create_engine("postgresql://root:root@pgdatabase:5432/fintech_db")


def save_to_db(df: pd.DataFrame, table_name: str) -> None:
    tries = 0
    connection = False
    while True:
        try:
            connection = engine.connect()
            break
        except Exception as e:
            print(
                f"Error connecting to Database: {str(e)} will retry again in 5 seconds"
            )
            tries += 1
            if tries > 3:
                print("Max retries reached. Exiting.")
                raise ValueError("Max retries reached. Exiting.")
            time.sleep(5)

    if connection:
        print("Connected to Database")
        try:
            print(f"Trying to save {table_name} to database")
            df.to_sql(table_name, con=engine, if_exists="replace")
            print(f"{table_name} saved to database")
        except ValueError:
            print(f"{table_name} already exists in the database")
        except Exception as ex:
            print(ex)
    else:
        print("Failed to connect to Database")
