import pandas as pd
import sqlite3

def read_file(path, file_type="csv"):
    try:
        if file_type == "csv":
            data = pd.read_csv(path)
        elif file_type == "excel":
            data = pd.read_excel(path, engine="openpyxl")
        else:
            raise ValueError("Unsupported file type. Use 'csv' or 'excel'.")
        print(f"File successfully read: {path}")
        return data
    except FileNotFoundError as e:
        print(f"File Not Found: {e}")
        return None
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
        return None

def create_table(conn, create_table_query):
    try:
        conn.execute(create_table_query)
        print("Table created successfully!")
    except sqlite3.OperationalError as e:
        print(f"An error occurred while creating the table: {e}")

def insert_data(data, conn, table_name):
    try:
        data.to_sql(table_name, conn, if_exists="append", index=False)
        print(f"Data successfully inserted into the table '{table_name}'!")
    except Exception as e:
        print(f"An error occurred while inserting data: {e}")

if __name__ == "__main__":
    path_ball = r"D:\python_scripts\project\Data_analytical\Cricket Analysis\Ball.csv"
    path_matches = r"D:\python_scripts\project\Data_analytical\Cricket Analysis\Matches.csv"

    ball_data = read_file(path_ball, file_type="csv")
    matches_data = read_file(path_matches, file_type="csv")

    db_path = "Cricket.db"
    conn = sqlite3.connect(db_path)

    create_table_query_matches = """
    CREATE TABLE IF NOT EXISTS ipl_matches_2008_2022 (
        id INTEGER PRIMARY KEY,
        city TEXT,
        match_date DATE,
        season TEXT,
        match_number TEXT,
        team1 TEXT,
        team2 TEXT,
        venue TEXT,
        toss_winner TEXT,
        toss_decision TEXT,
        superover TEXT,
        winning_team TEXT,
        won_by TEXT,
        margin TEXT,
        method TEXT,
        player_of_match TEXT,
        umpire1 TEXT,
        umpire2 TEXT
    );
    """
    create_table(conn, create_table_query_matches)

    if matches_data is not None:
        matches_data = matches_data.drop_duplicates(subset=["id"])  # Remove duplicate IDs
        insert_data(matches_data, conn, "ipl_matches_2008_2022")

    create_table_query_ball = """
    CREATE TABLE IF NOT EXISTS ipl_ball_by_ball_2008_2022 (
        id INTEGER NOT NULL,
        innings INTEGER,
        overs INTEGER,
        ball_number INTEGER,
        batter TEXT,
        bowler TEXT,
        non_striker TEXT,
        extra_type TEXT,
        batsman_run INTEGER,
        extras_run INTEGER,
        total_run INTEGER,
        non_boundary INTEGER,  -- Corrected column name
        iswicket_delivery INTEGER,
        player_out TEXT,
        dismisal_kind TEXT,
        fielders_involved TEXT,
        batting_team TEXT,
        PRIMARY KEY (id, innings, overs, ball_number)
    );
    """
    create_table(conn, create_table_query_ball)

    if ball_data is not None:
        ball_data = ball_data.rename(columns={"non_boundary": "non_boundary"})
        insert_data(ball_data, conn, "ipl_ball_by_ball_2008_2022")

    conn.close()
