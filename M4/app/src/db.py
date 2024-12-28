from sqlalchemy import create_engine
import pandas as pd
import time


CLEANED_DATA_DB_TABLE = "fintech_data_MET_P1_52_4509_clean"

engine = create_engine("postgresql://root:root@pgdatabase:5432/fintech_db")

def load_to_db(transformed_data_path: str) -> None:
    df = pd.read_csv(transformed_data_path)

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
            print(f"Trying to save {CLEANED_DATA_DB_TABLE} to database")
            df.to_sql(CLEANED_DATA_DB_TABLE, con=engine, if_exists="replace")
            print(f"{CLEANED_DATA_DB_TABLE} saved to database")
        except ValueError:
            print(f"{CLEANED_DATA_DB_TABLE} already exists in the database")
        except Exception as ex:
            print(ex)
    else:
        print("Failed to connect to Database")