"""

This file should scrape effective height, Wins above bubble, Torvik power rating, Experience, and Talent from barttorvik.com

"""

from selenium import webdriver

from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup

import codecs

import re

from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import mysql.connector
import requests
import json
import time

url1 = "https://barttorvik.com/team-tables_each.php?year="
url2 = "&top=0&conlimit=All"

#driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--headless")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

cnx  = mysql.connector.connect(
        host="34.68.250.121",
        user="wsa",
        password="LeBron>MJ!",
        database="F24-CollegeBBall"
    )

cursor = cnx.cursor()    

query = """
    UPDATE team_stats
    SET 
        `BT Power` = %s,
        `BT Eff Height` = %s,
        `BT Experience` = %s,
        `BT Talent` = %s
    WHERE team_id = %s;
"""

query2 = """
    UPDATE team_stats
    SET 
        `BT WAB` = %s
    WHERE team_id = %s;
"""

for i in range (2015, 2025):
   # print(str(i) + '\n')
   url = url1 + str(i) + url2
   driver.get(url)
   wait = WebDriverWait(driver, 10)
   wait.until(EC.url_to_be(url))
   time.sleep(20)
   if driver.current_url == url:
      table = driver.page_source

   soup = BeautifulSoup(table, 'html.parser')
   # print(table)

   
   # Initialize an empty dictionary to store the data
   data_list = []

   # Find all <tr> elements in the <tbody>
   rows = soup.select('tbody tr')

   # Iterate through each row
   for row in rows:
      # Find all <td> elements in the current row
      cells = row.find_all('td')
    
      # Ensure there are enough elements to access the required indices
      if len(cells) >= 35:
         team_name = cells[1].get_text()
         if team_name == "IU Indy":
            team_name = "IUPUI"
         team_id = team_name + "_" + str(i)
         # Extract the 2nd, 5th, 30th, 31st, and 35th elements as values
         value = (
            cells[4].get_text(),   # 5th element (index 4)
            cells[29].get_text(),  # 30th element (index 29)
            cells[30].get_text(),  # 31st element (index 30)
            cells[34].get_text(),  # 35th element (index 34)
            team_id
         )
        
         # Add to the list
         data_list.append(value)
   print(data_list)
   cursor.executemany(query, data_list)
   cnx.commit()
   

url3 = "https://barttorvik.com/trank.php?year="

for i in range (2015, 2025):
   url = url3 + str(i) + "#"
   driver.get(url)
   wait = WebDriverWait(driver, 10)
   wait.until(EC.url_to_be(url))
   time.sleep(20)
   if driver.current_url == url:
      table = driver.page_source

   soup = BeautifulSoup(table, 'html.parser')

   # Initialize an empty dictionary to store the data
   data_list = []

   # Find all <tr> elements in the <tbody>
   rows = soup.select('tbody tr')

   # Iterate through each row
   for row in rows:
      # Find all <td> elements in the current row
      cells = row.find_all('td')

      # Ensure there are enough elements to access the required indices
      if len(cells) >= 24:
         team_name = cells[1].get_text().split('\xa0')[0]
         if team_name == "IU Indy":
            team_name = "IUPUI"
         team_id = team_name + "_" + str(i)
         WAB = cells[23].get_text()
         if WAB.startswith('+'):
            WAB = WAB[1:]
         
         value = (
            WAB,   # 24th element (index 23)
            team_id
         )
        
         # Add to the list
         data_list.append(value)

   print(data_list)
   cursor.executemany(query2, data_list)
   cnx.commit()
