import psycopg2
from psycopg2 import sql
import pandas as pd 
import numpy as np
import io
import os
import json
from datetime import datetime
import shutil
import argparse
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def run_sql_query(table_name, query):
    conn = get_connection()
    cur = conn.cursor()
    query = sql.SQL(query).format(table_name)
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def create_stage_table():

    table_name = sql.Identifier("stage", "event_data")

    query = """
        DROP TABLE IF EXISTS stage.event_data CASCADE;
        CREATE TABLE stage.event_data (
            raw_data         JSONB,
            source_file_name TEXT,
            load_ts          TIMESTAMPTZ DEFAULT NOW()
        );      
    """
    run_sql_query(table_name, query)
    print("Stage table created successfully")


def data_preprocess(df, file_path):

    df = df.replace({np.nan: None}) # Replace NaN with None. PostgreSQL only accepts null for missing JSON fields, not NaN

    json_records = [json.dumps(row) for row in df.to_dict(orient="records")]

    stage_df = pd.DataFrame({
        "raw_data": json_records, 
        "source_file_name": Path(file_path).stem, # Keeps the file name after the final slash without extension
        "load_ts": datetime.now(), 
    })

    return stage_df


def load_to_stage(df):
    conn = get_connection() # Establish connection to PostgreSQL database
    cur = conn.cursor() # Create a cursor object to execute SQL commands

    cur.execute("SET search_path TO stage;") # Set the path to stage. PostgreSQL will look for tables inside stage
    conn.commit() # Saves this setting for the current session. When you run SQL commands, those changes are made in temp state called a transaction. Without commiting, all changes will be lost if the connection closes or if there is an error

    buffer = io.StringIO() # Creates a 'fake file' in your computer's memory, all stored in RAM, not on hard drive 
    df.to_csv(buffer, index=False, header=True) # This writes the df into that 'fake file' in CSV format 
    buffer.seek(0) # When you write to a 'fake file', the 'cursor' moves to the end. This command moves the cursor back to the start, so when you read from this buffer, it reads from the beginning

    cur.copy_expert("COPY event_data FROM STDIN WITH CSV HEADER", buffer) # This copy_expert command requires a file-like object to read CSV. This stops you saving directly on hard drive, faster and cleaner processing

    conn.commit()
    cur.close()
    conn.close()
    print("Table populated successfully")


def transfer_to_archive(file, dest_base):
    shutil.copy(file, dest_base) # copy the current file into the archived folder 
    timestamp_now = datetime.now().strftime("%Y%m%d%H%M%S") # get the current timestamp, which we'll append to the file in the archived folder 
    file_new = dest_base + '/' + Path(file).stem # split the base so the extension is excluded from the file name
    file_rename = file_new + "_" + timestamp_now + ".csv" # add the timestamp to the base 
    os.rename(file_new + ".csv", file_rename) # rename the transfered file with this new file name 
    shutil.rmtree(src_base, ignore_errors=True) # Delete everything in the original folder


def incremental_refresh():
    file = src_base + '/' + os.listdir("data/raw_data")[0]
    df = pd.read_csv(file)
    df_processed = data_preprocess(df, file)
    load_to_stage(df_processed)
    transfer_to_archive(file, dest_base)


def full_refresh(): 

    table_name = sql.Identifier('stage', 'event_data')
    query = "TRUNCATE TABLE {} RESTART IDENTITY CASCADE"
    run_sql_query(table_name, query)

    for file in os.listdir(dest_base):
        
        df = pd.read_csv(dest_base + '/' + file)
        file_name_processed = "_".join(file.split("_")[:2]) 
        df_processed = data_preprocess(df, file_name_processed)

        load_to_stage(df_processed)


if __name__ == "__main__":

    src_base = 'data/raw_data'
    dest_base = 'data/archived_data'

    parser = argparse.ArgumentParser(description="Manage stage table pipeline")
    parser.add_argument(
        "action",
        choices = ["create_stage_table", "incremental", "full_refresh"],
        help="Choose action: create table, run incremental load or full refresh"
    )
    args = parser.parse_args()

    actions = {
        "create_stage_table": create_stage_table, 
        "incremental": incremental_refresh,
        "full_refresh": full_refresh
    }

    actions[args.action]()









