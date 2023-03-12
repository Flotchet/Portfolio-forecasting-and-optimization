#-I-QOF-----------------------------------------------------------------------------------------
import warnings
#-I-OS------------------------------------------------------------------------------------------
from time import sleep
import os
import psutil
import gc
#-I-PERF----------------------------------------------------------------------------------------
from multiprocessing import Pool
#-I-DS------------------------------------------------------------------------------------------
import numpy as np
import pandas as pd
#-I-WEB-----------------------------------------------------------------------------------------
from selenium.common.exceptions import WebDriverException
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib3
#-I-DB------------------------------------------------------------------------------------------
import sqlite3
#-I-QOF-----------------------------------------------------------------------------------------
from tqdm import tqdm
from DB.db import DB
#-----------------------------------------------------------------------------------------------

#Single page scraper
def get_data(url: str, db: DB) -> None:
    """
    Get the data from the url and update the database

    Parameters
    ----------
    url: str
        url to scrape

    db: DB
        database to store the data

    Returns
    -------
    None
    """

    #check if got == True
    if db.check_ticker(url):
        return
    
    #driver is firefox
    driver = webdriver.Firefox(executable_path="/home/flotchet/Code/geckodriver")
    driver.get(url)

    #wait for the table to load
    WebDriverWait(driver, 10_000_000).until(lambda d: d.find_element(By.XPATH, "/html/body/div/section[2]/div/div[2]/table/tbody"))

    #Get the table element
    table = driver.find_element(By.XPATH, "/html/body/div/section[2]/div/div[2]/table/tbody")

    #scroll to the bottom of the page to load all the data multiple times
    for i in range(25):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        sleep(0.2)

    #extract the ticker from the url form = "https://finance.yahoo.com/quote/TICKER/history"
    ticker = url.split("/")[-2]

    #save the table into the database into the data table corresponding to the ticker
    #insert_ticker_data(self, symbol: str, date: str, open: float, high: float, low: float, close: float, volume: float, adj_close: float)
    #order in the table is                 Date	       Open        High         Low         Close*         Adj Close**   Volume
    for row in table.find_elements(By.TAG_NAME, "tr"):
        cells = row.find_elements(By.TAG_NAME, "td")
        db.insert_ticker_data(ticker, cells[0].text, cells[1].text, cells[2].text, cells[3].text, cells[4].text, cells[6].text, cells[5].text)



    #close the driver
    driver.close()

    #update the database
    db.update_ticker(url)


if __name__ == "__main__":
    #get the urls
    db = DB()
    urls = db.get_ticker_urls()
    db.close()

    #get the data
    with Pool(16) as p:
        p.starmap(get_data, zip(urls, [db]*len(urls)))
    db.close()