import sqlite3
import pandas as pd
from tqdm import tqdm



class DB:

    """
Create a database to store the data

first table
    tickers with their symbol, company, name, url and got 

second table
    ticker_data with the date, open, high, low, close, volume, and adj_close
"""

    def __init__(self, db_name: str) -> None:
        """
        Parameters
        ----------
        db_name: str
            name of the database

        Returns
        -------
        None
        """
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)

    def create_ticker_table(self) -> None:
        """
        Create the ticker table

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        c = self.conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS tickers (
            symbol text,
            company text,
            url text,
            got boolean
        )""")
        self.conn.commit()

    def create_ticker_data_table(self) -> None:
        """
        Create the ticker_data table

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        c = self.conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS ticker_data (
            symbol text,
            date text,
            open real,
            high real,
            low real,
            close real,
            volume real,
            adj_close real
        )""")
        self.conn.commit()

    def insert_ticker(self, symbol: str, company: str, url: str, got: bool) -> None:
        """
        Insert ticker into the ticker table

        Parameters
        ----------
        symbol: str
            ticker symbol
        company: str
            company name
        url: str
            url of the ticker
        got: bool
            if the ticker data has been scraped

        Returns
        -------
        None
        """
        c = self.conn.cursor()
        c.execute("INSERT INTO tickers VALUES (?, ?, ?, ?)", (symbol, company, url, got))
        self.conn.commit()

    def insert_ticker_data(self, symbol: str, date: str, open: float, high: float, low: float, close: float, volume: float, adj_close: float) -> None:
        """
        Insert ticker data into the ticker_data table

        Parameters
        ----------
        symbol: str
            ticker symbol
        date: str
            date of the data
        open: float
            open price
        high: float
            high price
        low: float
            low price
        close: float
            close price
        volume: float
            volume
        adj_close: float
            adjusted close price

        Returns
        -------
        None
        """
        c = self.conn.cursor()
        c.execute("INSERT INTO ticker_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (symbol,date, open, high, low, close, volume, adj_close))   
        self.conn.commit()

    def get_tickers(self) -> list:
        """
        Get all the tickers from the ticker table

        Parameters
        ----------
        None

        Returns
        -------
        list
            list of tuples with the symbol and company name
        """
        c = self.conn.cursor()
        c.execute("SELECT * FROM tickers")
        return c.fetchall()
    
    def get_ticker_data(self, symbol: str) -> list:
        """
        Get all the ticker data from the ticker_data table

        Parameters
        ----------
        symbol: str
            ticker symbol

        Returns
        -------
        list
            list of tuples with the date, open, high, low, close, volume, and adj_close
        """
        c = self.conn.cursor()
        c.execute("SELECT * FROM ticker_data WHERE symbol = ?", (symbol,))
        return c.fetchall()
    
    def get_ticker_data_df(self, symbol: str) -> pd.DataFrame:
        """
        Get all the ticker data from the ticker_data table

        Parameters
        ----------
        symbol: str
            ticker symbol

        Returns
        -------
        pd.DataFrame
            dataframe with the date, open, high, low, close, volume, and adj_close
        """
        c = self.conn.cursor()
        c.execute("SELECT * FROM ticker_data WHERE symbol = ?", (symbol,))
        return pd.DataFrame(c.fetchall(), columns=["symbol", "date", "open", "high", "low", "close", "volume", "adj_close"])
    

    def get_ticker_data_df_by_date(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get all the ticker data from the ticker_data table

        Parameters
        ----------
        symbol: str
            ticker symbol
        start_date: str
            start date
        end_date: str
            end date

        Returns
        -------
        pd.DataFrame
            dataframe with the date, open, high, low, close, volume, and adj_close
        """
        c = self.conn.cursor()
        c.execute("SELECT * FROM ticker_data WHERE symbol = ? AND date BETWEEN ? AND ?", (symbol, start_date, end_date))
        return pd.DataFrame(c.fetchall(), columns=["symbol", "date", "open", "high", "low", "close", "volume", "adj_close"])
    
    def update_ticker(self, url :str):
        """
        Update the ticker table

        Parameters
        ----------
        url: str
            url of the ticker

        Returns
        -------
        None
        """
        c = self.conn.cursor()
        c.execute("UPDATE tickers SET got = True WHERE url = ?", (url,))
        self.conn.commit()

    def check_ticker(self, url: str) -> bool:
        """
        Check if the ticker has been scraped

        Parameters
        ----------
        url: str
            url of the ticker

        Returns
        -------
        bool
            if the ticker has been scraped
        """
        c = self.conn.cursor()
        c.execute("SELECT got FROM tickers WHERE url = ?", (url,))
        return c.fetchone()[0]
    
    

def ticker_csv_to_db(csv_path : str = "data/tickerc.csv", db_name : str = "data.db") -> DB:
    """
    Convert the ticker csv file to a database

    Parameters
    ----------
    csv_path: str
        path to the csv file
    db_name: str
        name of the database

    Returns
    -------
    None
    """
    df = pd.read_csv(csv_path)
    db = DB(db_name)
    db.create_ticker_table()
    for _, row in tqdm(df.iterrows(), total=len(df)):
        url = "https://finance.yahoo.com/quote/" + str(row["Symbol"]) + "/history?p=" + str(row["Symbol"])
        db.insert_ticker(str(row["Symbol"]), str(row["Company"]), url, False)
    return db

if __name__ == "__main__":
    db = ticker_csv_to_db()