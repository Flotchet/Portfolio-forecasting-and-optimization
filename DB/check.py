import sqlite3
import pandas as pd
import os

#check what percentage of the tickers have been scraped
def check() -> None:
    """
    Check what percentage of the tickers have been scraped

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    #connect to the db
    conn = sqlite3.connect("DB/data.db")

    #get the size in GB of the db
    size = os.path.getsize("DB/data.db") / 1e9

    #get the tickers table
    df = pd.read_sql_query("SELECT * FROM tickers", conn)

    #get the number of lines in the table
    n = len(df)

    #get the number of tickers where got == True
    m = len(df[df["got"] == True])

    #get the percentage
    p = m / n

    #print the percentage
    print(p*100, "%", sep = "")

    #print the estimate final size of the db
    final = size * (1 / p)
    print("Final size: ", final, "Gb", sep = "")

    #get the number of table of the db
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = c.fetchall()

    #print the number of tables
    print("Number of tables: ", len(tables))

    #p table 
    p = (len(tables) - 1) / m 

    #print the estimate final size of the db

    final = final * (1 / p) 

    print("Final size: ", final, "Gb", sep = "")

    #close the connection
    conn.close()

    return None


if __name__ == "__main__":
    check()