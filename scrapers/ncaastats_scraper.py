# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 09:51:36 2024

@author: djmcd
"""

import requests
from bs4 import BeautifulSoup
import datetime
import mysql.connector

# Constants for WSA server
HOST = "34.68.250.121"
USER = "wsa"
PASSWORD = "LeBron>MJ!"
DB = "F24-CollegeBBall"

# Scraping headers
HEADERS = {
    'referer': 'https://www.scrapingcourse.com/ecommerce/',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    'accept-encoding': 'gzip, deflate, br',
    'sec-ch-device-memory': '8',
    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-platform': "Windows",
    'sec-ch-ua-platform-version': '"10.0.0"',
    'sec-ch-viewport-width': '792',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  }

# Name conversions
VALID_NAMES = ["Abilene Christian","Air Force","Akron","Alabama","Alabama A&M","Alabama St.","Albany","Alcorn St.","American","Appalachian St.","Arizona","Arizona St.","Arkansas","Arkansas Pine Bluff","Arkansas St.","Army","Auburn","Austin Peay","Ball St.","Baylor","Bellarmine","Belmont","Bethune Cookman","Binghamton","Boise St.","Boston College","Boston University","Bowling Green","Bradley","Brown","Bryant","Bucknell","Buffalo","Butler","BYU","Cal Baptist","Cal Poly","Cal St. Bakersfield","Cal St. Fullerton","Cal St. Northridge","California","Campbell","Canisius","Central Arkansas","Central Connecticut","Central Michigan","Charleston","Charleston Southern","Charlotte","Chattanooga","Chicago St.","Cincinnati","Clemson","Cleveland St.","Coastal Carolina","Colgate","Colorado","Colorado St.","Columbia","Connecticut","Coppin St.","Cornell","Creighton","Dartmouth","Davidson","Dayton","Delaware","Delaware St.","Denver","DePaul","Detroit Mercy","Drake","Drexel","Duke","Duquesne","East Carolina","East Tennessee St.","Eastern Illinois","Eastern Kentucky","Eastern Michigan","Eastern Washington","Elon","Evansville","Fairfield","Fairleigh Dickinson","FIU","Florida","Florida A&M","Florida Atlantic","Florida Gulf Coast","Florida St.","Fordham","Fresno St.","Furman","Gardner Webb","George Mason","George Washington","Georgetown","Georgia","Georgia Southern","Georgia St.","Georgia Tech","Gonzaga","Grambling St.","Grand Canyon","Green Bay","Hampton","Harvard","Hawaii","High Point","Hofstra","Holy Cross","Houston","Houston Christian","Howard","Idaho","Idaho St.","Illinois","Illinois Chicago","Illinois St.","Incarnate Word","Indiana","Indiana St.","Iona","Iowa","Iowa St.","IUPUI","Jackson St.","Jacksonville","Jacksonville St.","James Madison","Kansas","Kansas St.","Kennesaw St.","Kent St.","Kentucky","La Salle","Lafayette","Lamar","Le Moyne","Lehigh","Liberty","Lindenwood","Lipscomb","Little Rock","LIU","Long Beach St.","Longwood","Louisiana","Louisiana Monroe","Louisiana Tech","Louisville","Loyola Chicago","Loyola Marymount","Loyola MD","LSU","Maine","Manhattan","Marist","Marquette","Marshall","Maryland","Maryland Eastern Shore","Massachusetts","McNeese St.","Memphis","Mercer","Merrimack","Miami FL","Miami OH","Michigan","Michigan St.","Middle Tennessee","Milwaukee","Minnesota","Mississippi","Mississippi St.","Mississippi Valley St.","Missouri","Missouri St.","Monmouth","Montana","Montana St.","Morehead St.","Morgan St.","Mount St. Mary's","Murray St.","N.C. State","Navy","Nebraska","Nebraska Omaha","Nevada","New Hampshire","New Mexico","New Mexico St.","New Orleans","Niagara","Nicholls St.","NJIT","Norfolk St.","North Alabama","North Carolina","North Carolina A&T","North Carolina Central","North Dakota","North Dakota St.","North Florida","North Texas","Northeastern","Northern Arizona","Northern Colorado","Northern Illinois","Northern Iowa","Northern Kentucky","Northwestern","Northwestern St.","Notre Dame","Oakland","Ohio","Ohio St.","Oklahoma","Oklahoma St.","Old Dominion","Oral Roberts","Oregon","Oregon St.","Pacific","Penn","Penn St.","Pepperdine","Pittsburgh","Portland","Portland St.","Prairie View A&M","Presbyterian","Princeton","Providence","Purdue","Purdue Fort Wayne","Queens","Quinnipiac","Radford","Rhode Island","Rice","Richmond","Rider","Robert Morris","Rutgers","Sacramento St.","Sacred Heart","Saint Francis","Saint Joseph's","Saint Louis","Saint Mary's","Saint Peter's","Sam Houston St.","Samford","San Diego","San Diego St.","San Francisco","San Jose St.","Santa Clara","Seattle","Seton Hall","Siena","SIU Edwardsville","SMU","South Alabama","South Carolina","South Carolina St.","South Dakota","South Dakota St.","South Florida","Southeast Missouri St.","Southeastern Louisiana","Southern","Southern Illinois","Southern Indiana","Southern Miss","Southern Utah","St. Bonaventure","St. John's","St. Thomas","Stanford","Stephen F. Austin","Stetson","Stonehill","Stony Brook","Syracuse","Tarleton St.","TCU","Temple","Tennessee","Tennessee Martin","Tennessee St.","Tennessee Tech","Texas","Texas A&M","Texas A&M Commerce","Texas A&M Corpus Chris","Texas Southern","Texas St.","Texas Tech","The Citadel","Toledo","Towson","Troy","Tulane","Tulsa","UAB","UC Davis","UC Irvine","UC Riverside","UC San Diego","UC Santa Barbara","UCF","UCLA","UMass Lowell","UMBC","UMKC","UNC Asheville","UNC Greensboro","UNC Wilmington","UNLV","USC","USC Upstate","UT Arlington","UT Rio Grande Valley","Utah","Utah St.","Utah Tech","Utah Valley","UTEP","UTSA","Valparaiso","Vanderbilt","VCU","Vermont","Villanova","Virginia","Virginia Tech","VMI","Wagner","Wake Forest","Washington","Washington St.","Weber St.","West Virginia","Western Carolina","Western Illinois","Western Kentucky","Western Michigan","Wichita St.","William & Mary","Winthrop","Wisconsin","Wofford","Wright St.","Wyoming","Xavier","Yale","Youngstown St."]
CONVERT_NAME = {
    "Northern Ariz.": "Northern Arizona",
    "McNeese": "McNeese St.",
    "N.C. A&T": "North Carolina A&T",
    "Greensboro": "UNC Greensboro",
    "Alcorn": "Alcorn St.",
    "South Fla.": "South Florida",
    "Fla. Atlantic": "Florida Atlantic",
    "Grambling": "Grambling St.",
    "Eastern Mich.": "Eastern Michigan",
    "Charleston So.": "Charleston Southern",
    "Southern U.": "Southern",
    "Northern Ky.": "Northern Kentucky",
    "ETSU": "East Tennessee St.",
    "Eastern Ky.": "Eastern Kentucky",
    "UNI": "Northern Iowa",
    "ULM": "Louisiana Monroe",
    "Southern Miss.": "Southern Miss",
    "Southeast Mo. St.": "Southeast Missouri St.",
    "Bethune-Cookman": "Bethune Cookman",
    "Southern California": "USC",
    "NC State": "N.C. State",
    "A&M-Corpus Christi": "Texas A&M Corpus Chris",
    "FDU": "Fairleigh Dickinson",
    "CSUN": "Cal St. Northridge",
    "Central Ark.": "Central Arkansas",
    "UMES": "Maryland Eastern Shore",
    "CSU Bakersfield": "Cal St. Bakersfield",
    "Middle Tenn.": "Middle Tennessee",
    "Army West Point": "Army",
    "Ole Miss": "Mississippi",
    "St. John's (NY)": "St. John's",
    "Miami (OH)": "Miami OH",
    "Prairie View": "Prairie View A&M",
    "Ark.-Pine Bluff": "Arkansas Pine Bluff",
    "UConn": "Connecticut",
    "UIW": "Incarnate Word",
    "UTRGV": "UT Rio Grande Valley",
    "Nicholls": "Nicholls St.",
    "UIC": "Illinois Chicago",
    "N.C. Central": "North Carolina Central",
    "Western Caro.": "Western Carolina",
    "Central Mich.": "Central Michigan",
    "Miami (FL)": "Miami FL",
    "Gardner-Webb": "Gardner Webb",
    "Seattle U": "Seattle",
    "Loyola Maryland": "Loyola MD",
    "UAlbany": "Albany",
    "Eastern Wash.": "Eastern Washington",
    "Southeastern La.": "Southeastern Louisiana",
    "Ga. Southern": "Georgia Southern",
    "Southern Ill.": "Southern Illinois",
    "SIUE": "SIU Edwardsville",
    "Lamar University": "Lamar",
    "Western Ky.": "Western Kentucky",
    "Boston U.": "Boston University",
    "Northern Colo.": "Northern Colorado",
    "Central Conn. St.": "Central Connecticut",
    "UT Martin": "Tennessee Martin",
    "Omaha": "Nebraska Omaha",
    "Col. of Charleston": "Charleston",
    "IU Indy": "IUPUI",
    "Western Ill.": "Western Illinois",
    "SFA": "Stephen F. Austin",
    "Eastern Ill.": "Eastern Illinois",
    "Sam Houston": "Sam Houston St.",
    "App State": "Appalachian St.",
    "LMU (CA)": "Loyola Marymount",
    "Western Mich.": "Western Michigan",
    "Mississippi Val.": "Mississippi Valley St.",
    "UNCW": "UNC Wilmington",
    "Saint Mary's (CA)": "Saint Mary's",
    "FGCU": "Florida Gulf Coast",
    "NIU": "Northern Illinois"
}

def scrapeSeason(seasonid: int, year: int):
    """Scrapes the stats.ncaa.org MBB scorecards for a given season"""
    sid = str(seasonid)
    
    # Create SQL command and list to hold records
    command = (
        "INSERT INTO game_results "
        "(game_id, Datetime, `Team1 ID`, `Team2 ID`, `Team1 Score`, `Team2 Score`, `Team1 Home`, `Team2 Home`) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    )
    
    values = []
    
    # Create lists of year/month combos and days for that season
    yearmonth = [("11", str(year - 1)), ("12", str(year - 1)), ("01", str(year)), ("02", str(year)), ("03", str(year)), ("04", str(year))]
    days = []
    for i in range(1, 32):
        d = str(i)
        if (len(d) == 1):
            d = "0" + d
        days.append(d)
            
        
    for ym in yearmonth:
        print("Now at:", ym)
        for dy in days:
            print("Now at day:", dy)
            mn = ym[0]
            yr = ym[1]
            link = f"https://stats.ncaa.org/season_divisions/{sid}/livestream_scoreboards?game_date={mn}%2F{dy}%2F{yr}"
            
            html = requests.get(link, headers = HEADERS)
            
            soup = BeautifulSoup(html.text, "html.parser")
            scorecards = soup.find_all("div", attrs={"class": "col-md-auto p-0"})
            for sc in scorecards:
                # Parse scorecard
                game = sc.find("div", attrs={"class": "table-responsive"})
                rows = game.find_all("tr")
                        
                # Neutral site games have an extra row, account for that
                offset = 0 if len(rows) == 6 else 1
                
                date = rows[0].find("div", attrs={"class": "col-6 p-0"}).text
                                
                game_id = rows[1 + offset].get('id')[8:]
                t1_name = rows[1 + offset].find_all("td")[1].text
                t1_score = rows[1 + offset].find_all("td")[-1].text
                                
                # # Deal with COVID canceled games causing error
                # if t1_score.strip() == "Canceled" or t1_score.strip() == "":
                #     continue
                                
                t2_name = rows[4 + offset].find_all("td")[1].text
                
                try:
                    t2_score = rows[4 + offset].find_all("td")[2].text
                except:
                    # Skip over games that were cancelled or postponed, causing table to read differently
                    print(f"Cancelled or postponed game {game_id} on {date.strip()}: Skipping to next game.")
                    continue
                t1_home = False
                t2_home = len(rows) == 6 # 7 rows if neutral site game
                
                
                # Remove white space
                date = date.strip()
                t1_name = t1_name.strip()
                t1_score = t1_score.strip()
                t2_score = t2_score.strip()
                t2_name = t2_name.strip()
                                
                # Check to see if it's a date with games or not
                if date[:10] != (mn + "/" + dy + "/" + yr):
                    continue
                
                # Started adding records and seeds after team names in 2020 season
                # Check if numbers are there and remove records if so
                if (any(ch.isdigit() for ch in t1_name)):
                    t1_name = t1_name[:t1_name.rfind(" ")]
                    
                if (any(ch.isdigit() for ch in t2_name)):
                    t2_name = t2_name[:t2_name.rfind(" ")]                    
                    
                # Check if number sign is there (seed) and remove if so
                if (t1_name[0] == "#"):
                    t1_name = t1_name[t1_name.find(" ") + 1:]
                    
                if (t2_name[0] == "#"):
                    t2_name = t2_name[t2_name.find(" ") + 1:]
                    
                if not t1_name in VALID_NAMES:
                    if not t1_name in CONVERT_NAME:
                        continue
                    else:
                        t1_name = CONVERT_NAME[t1_name]
                if not t2_name in VALID_NAMES:
                    if not t2_name in CONVERT_NAME:
                        continue
                    else:
                        t2_name = CONVERT_NAME[t2_name]
                        
                # Format team ID
                t1_id = t1_name + "_" + str(year)
                t2_id = t2_name + "_" + str(year)
                
                # Format date
                date = datetime.datetime(int(yr), int(mn), int(dy))
                
                # Handle random case where "Final" is included in box score
                if t1_score == "Final":
                    t1_score = rows[1 + offset].find_all("td")[-2].text
                    t1_score = t1_score.strip()
                
                game_values = (game_id, date, t1_id, t2_id, t1_score, t2_score, int(t1_home), int(t2_home))
                values.append(game_values)
                
                # print(game_id, date, t1_id, t1_score, t1_home, t2_id, t2_score, t2_home)
         
    # Connect to database
    cnx  = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DB
        )
    
    cursor = cnx.cursor()
    
    # Execute
    print(values)
    cursor.executemany(command, values)
    cnx.commit()
    
    # Close connection
    cursor.close()
    cnx.close()