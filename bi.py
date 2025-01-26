import pandas as pd
import sqlite3

def connect_db(path,query):
    try:
        conn = sqlite3.connect(path)
        data = pd.read_sql_query(query,conn)
        conn.close()
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Sql File not Found {e}")
        return None
    

if __name__ == "__main__":
    path = r"D:\python_scripts\project\Data_analytical\Cricket Analysis\Cricket.db"
    query = "SELECT * FROM ipl_matches_2008_2022 ;"
    ball_table = connect_db(path,query)
    query2 = "SELECT * FROM ipl_ball_by_ball_2008_2022 ;"
    match_table = connect_db(path,query2)
    