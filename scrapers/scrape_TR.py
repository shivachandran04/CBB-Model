import requests
from bs4 import BeautifulSoup
import mysql.connector
import pandas as pd
import json
import time

cnx = mysql.connector.connect(user = "wsa", host = "34.68.250.121", database = "F24-CollegeBBall", password = "LeBron>MJ!")
cursor = cnx.cursor(buffered = True)

url = requests.get('https://www.teamrankings.com/ncb/team-stats/')
soup = BeautifulSoup(url.text, 'html.parser')
soup.prettify()
table = soup.find('div', attrs = {'class' : 'main-wrapper clearfix has-left-sidebar'}).find('main')
links = table.find_all('a')
# Each link contains a webpage with rankings for a stat for a year


otherlinks = ["https://www.teamrankings.com/ncaa-basketball/ranking/predictive-by-other", "https://www.teamrankings.com/ncaa-basketball/ranking/home-by-other","https://www.teamrankings.com/ncaa-basketball/ranking/away-by-other",
              "https://www.teamrankings.com/ncaa-basketball/ranking/neutral-by-other","https://www.teamrankings.com/ncaa-basketball/ranking/schedule-strength-by-other"]
othernames = ["Predictive Power Rating", "Home Power Rating", "Away Power Rating", "Neutral Site Power Rating", "Strength of Schedule Power Rating"]
# "other" refers to the advanced stats, housed in a different spot on the website


TRstats = ["Points per Game","Opponent Points per Game", "Shooting %",
          "Opponent Shooting %", "Two Point %" , "Opponent Two Point %" , 
          "Three Point %", "Opponent Three Point %", "Free Throw %", 
          "Opponent Free Throw %", "True Shooting %", "Effective Field Goal %",
          "Two Point Rate", "Three Point Rate", "Field Goals Attempted per Game",
          "Opponent Field Goals Attempted per Game", "Free Throws Attempted per Game",
          "Opponent Free Throws Attempted per Game", "Three Pointers Attempted per Game",
          "Opponent Three Pointers Attempted per Game", "Personal Fouls per Game",
          "Opponent Personal Fouls per Game", "Offensive Rebounding %", "Defensive Rebounding %",
          "Block %", "Opponent Block %", "Steals per Game", "Opponent Steals per Game",
          "Assists per Game", "Opponent Assists per Game","Turnovers per Game",
           "Opponent Turnovers per Game", "Possessions per Game" , "Predictive Power Rating", "Home Power Rating",
           "Away Power Rating", "Neutral Site Power Rating", "Strength of Schedule Power Rating"]
# Contains the columns we want from TeamRankings


def convertteam(team):
    if(team == "Abl Christian"):
        return "Abilene Christian"
    elif(team == "Alab A&M"):
        return "Alabama A&M"
    elif(team == "App State"):
        return "Appalachian St."
    elif(team == "Ark Pine Bl"):
        return "Arkansas Pine Bluff"
    elif(team == "Beth-Cook"):
        return "Bethune Cookman"
    elif(team == "Boston Col"):
        return "Boston College"
    elif(team == "Boston U"):
        return "Boston University"
    elif(team == "Bowling Grn"):
        return "Bowling Green"
    elif(team == "CS Bakersfld"):
        return "Cal St. Bakersfield"
    elif(team == "CS Fullerton"):
        return "Cal St. Fullerton"
    elif(team == "Cal St Nrdge"):
        return "Cal St. Northridge"
    elif(team == "Central Ark"):
        return "Central Arkansas"
    elif(team == "Central Conn"):
        return "Central Connecticut"
    elif(team == "Central Mich"):
        return "Central Michigan"
    elif(team == "Col Charlestn"):
        return "Charleston"
    elif(team == "Charl South"):
        return "Charleston Southern"
    elif(team == "Coastal Car"):
        return "Coastal Carolina"
    elif(team == "Uconn"):
        return "Connecticut"
    elif(team == "Detroit"):
        return "Detroit Mercy"
    elif(team == "E Carolina"):
        return "East Carolina"
    elif(team == "E Tenn St"):
        return "East Tennessee St."
    elif(team == "E Illinois"):
        return "Eastern Illinois"
    elif(team == "E Kentucky"):
        return "Eastern Kentucky"
    elif(team == "E Michigan"):
        return "Eastern Michigan"
    elif(team == "E Washingtn"):
        return "Eastern Washington"
    elif(team == "F Dickinson"):
        return "Fairleigh Dickinson"
    elif(team == "Florida Intl"):
        return "FIU"
    elif(team == "Fla Atlantic"):
        return "Florida Atlantic"
    elif(team == "Fla Gulf Cst"):
        return "Florida Gulf Coast"
    elif(team == "Gard-Webb"):
        return "Gardner Webb"
    elif(team == "Geo Mason"):
        return "George Mason"
    elif(team == "Geo Wshgtn"):
        return "George Washington"
    elif(team == "GA Southern"):
        return "Georgia Southern"
    elif(team == "GA Tech"):
        return "Georgia Tech"
    elif(team == "Grd Canyon"):
        return "Grand Canyon"
    elif(team == "WI-Grn Bay"):
        return "Green Bay"
    elif(team == "Hsn Christian"):
        return "Houston Christian"
    elif(team == "IL-Chicago"):
        return "Illinois Chicago"
    elif(team == "Incar Word"):
        return "Incarnate Word"
    elif(team == "IU Indy"):
        return "IUPUI"
    elif(team == "Jksnville St"):
        return "Jacksonville St."
    elif(team == "James Mad"):
        return "James Madison"
    elif(team == "AR Lit Rock"):
        return "Little Rock"
    elif(team == "Lg Beach St"):
        return "Long Beach St."
    elif(team == "UL Monroe"):
        return "Louisiana Monroe"
    elif(team == "LA Tech"):
        return "Louisiana Tech"
    elif(team == "Loyola-Chi"):
        return "Loyola Chicago"
    elif(team == "Loyola Mymt"):
        return "Loyola Marymount"
    elif(team == "Loyola-MD"):
        return "Loyola MD"
    elif(team == "Maryland ES"):
        return "Maryland Eastern Shore"
    elif(team == "U Mass"):
        return "Massachusetts"
    elif(team == "Miami"):
        return "Miami FL"
    elif(team == "Miami (OH)"):
        return "Miami OH"
    elif(team == "Middle Tenn"):
        return "Middle Tennessee"
    elif(team == "WI-Milwkee"):
        return "Milwaukee"
    elif(team == "Ole Miss"):
        return "Mississippi"
    elif(team == "Miss State"):
        return "Mississippi St."
    elif(team == "Miss Val St"):
        return "Mississippi Valley St."
    elif(team == "Mt St Marys"):
        return "Mount St. Mary's"
    elif(team == "NC State"):
        return "N.C. State"
    elif(team == "Neb Omaha"):
        return "Nebraska Omaha"
    elif(team == "N Hampshire"):
        return "New Hampshire"
    elif(team == "N Mex State"):
        return "New Mexico St."
    elif(team == "Nicholls"):
        return "Nicholls St."
    elif(team == "N Alabama"):
        return "North Alabama"
    elif(team == "N Carolina"):
        return "North Carolina"
    elif(team == "NC A&T"):
        return "North Carolina A&T"
    elif(team == "NC Central"):
        return "North Carolina Central"
    elif(team == "N Dakota St"):
        return "North Dakota St."
    elif(team == "N Florida"):
        return "North Florida"
    elif(team == "Northeastrn"):
        return "Northeastern"
    elif(team == "N Arizona"):
        return "Northern Arizona"
    elif(team == "N Colorado"):
        return "Northern Colorado"
    elif(team == "N Illinois"):
        return "Northern Illinois"
    elif(team == "N Iowa"):
        return "Northern Iowa"
    elif(team == "N Kentucky"):
        return "Northern Kentucky"
    elif(team == "NW State"):
        return "Northwestern St."
    elif(team == "U Penn"):
        return "Penn"
    elif(team == "Prairie View"):
        return "Prairie View A&M"
    elif(team == "IPFW"):
        return "Purdue Fort Wayne"
    elif(team == "Rob Morris"):
        return "Robert Morris"
    elif(team == "Sac State"):
        return "Sacramento St."
    elif(team == "Sacred Hrt"):
        return "Sacred Heart"
    elif(team == "St Josephs"):
        return "Saint Joseph's"
    elif(team == "S Alabama"):
        return "South Alabama"
    elif(team == "St Marys"):
        return "Saint Mary's"
    elif(team == "St Peters"):
        return "Saint Peter's"
    elif(team == "Sam Hous St"):
        return "Sam Houston St."
    elif(team == "SIU Edward"):
        return "SIU Edwardsville"
    elif(team == "S Florida"):
        return "South Florida"
    elif(team == "SE Missouri"):
        return "Southeast Missouri St."
    elif(team == "SE Louisiana"):
        return "Southeastern Louisiana"
    elif(team == "S Illinois"):
        return "Southern Illinois"
    elif(team == "S Indiana"):
        return "Southern Indiana"
    elif(team == "St Bonavent"):
        return "St. Bonaventure"
    elif(team == "S Mississippi"):
        return "Southern Miss"
    elif(team == "S Utah"):
        return "Southern Utah"
    elif(team == "St. Bonavent"):
        return "St. Bonaventure"
    elif(team == "St Johns"):
        return "St. John's"
    elif(team == "Ste F Austin"):
        return "Stephen F. Austin"
    elif(team == "TX Christian"):
        return "TCU"
    elif(team == "TN Martin"):
        return "Tennessee Martin"
    elif(team == "TN State"):
        return "Tennessee St."
    elif(team == "TN Tech"):
        return "Tennessee Tech"
    elif(team == "TX A&M-Com"):
        return "Texas A&M Commerce"
    elif(team == "TX A&M-CC"):
        return "Texas A&M Corpus Chris"
    elif(team == "TX Southern"):
        return "Texas Southern"
    elif(team == "Citadel"):
        return "The Citadel"
    elif(team == "UCSB"):
        return "UC Santa Barbara"
    elif(team == "UCSD"):
        return "UC San Diego"
    elif(team == "Mass Lowell"):
        return "UMass Lowell"
    elif(team == "Maryland BC"):
        return "UMBC"
    elif(team == "Kansas City"):
        return "UMKC"
    elif(team == "NC-Asheville"):
        return "UNC Asheville"
    elif(team == "NC-Grnsboro"):
        return "UNC Greensboro"
    elif(team == "NC-Wilmgton"):
        return "UNC Wilmington"
    elif(team == "SC Upstate"):
        return "USC Upstate"
    elif(team == "TX-Arlington"):
        return "UT Arlington"
    elif(team == "TX-Pan Am"):
        return "UT Rio Grande Valley"
    elif(team == "TX El Paso"):
        return "UTEP"
    elif(team == "VA Tech"):
        return "Virginia Tech"
    elif(team == "Wash State"):
        return "Washington St."
    elif(team == "W Virginia"):
        return "West Virginia"
    elif(team == "W Carolina"):
        return "Western Carolina"
    elif(team == "W Illinois"):
        return "Western Illinois"
    elif(team == "W Kentucky"):
        return "Western Kentucky"
    elif(team == "W Michigan"):
        return "Western Michigan"
    elif(team == "Wm & Mary"):
        return "William & Mary"
    elif(team == "Youngs St"):
        return "Youngstown St."
    elif(team == "St Fran (PA)"):
        return "Saint Francis"
    elif(team == "S Methodist"):
        return "SMU"
    elif(team == "S Car State"):
        return "South Carolina St."
    elif(team == "S Carolina"):
        return "South Carolina"
    elif(team == "S Dakota St"):
        return "South Dakota St."
    elif(team.endswith("St")):
        return team + "."
    else:
        return team
#Standardizing names to kenpom

query = """
    UPDATE team_stats
    SET 
        `Pts/Gm` = %s,
        `Opp. Pts/Gm` = %s,
        `FG%` = %s,
        `Opp. FG%` = %s,
        `2Pt%` = %s,
        `Opp. 2Pt%` = %s,
        `3Pt%` = %s,
        `Opp. 3Pt%` = %s,
        `FT%` = %s,
        `Opp. FT%` = %s,
        `TS%` = %s,
        `EffFG%` = %s,
        `%from2` = %s,
        `%from3` = %s,
        `FGAtt/Gm` = %s,
        `Opp. FGAtt/Gm` = %s,
        `FTAtt/Gm` = %s,
        `Opp. FTAtt/Gm` = %s,
        `3PtAtt/Gm` = %s,
        `Opp. 3PtAtt/Gm` = %s,
        `Fouls/Gm` = %s,
        `Opp. Fouls/Gm` = %s,
        `OReb%` = %s,
        `DReb%` = %s,
        `Blk%` = %s,
        `Opp. Blk%` = %s,
        `Stls/Gm` = %s,
        `Opp. Stls/Gm` = %s,
        `TOs/Gm` = %s,
        `Opp. TOs/Gm` = %s,
        `Asts/Gm` = %s,
        `Opp. Asts/Gm` = %s,
        `Pos/Gm` = %s,
        `TR Power` = %s,
        `TR Home Power` = %s,
        `TR Away Power` = %s,
        `TR Neutral Power` = %s,
        `TR SOS` = %s
    WHERE team_id = %s;
"""


dates = ['2015-05-01','2016-05-01','2017-05-01','2018-05-01','2019-05-01','2020-05-01','2021-05-01','2022-05-01','2023-05-01','2024-05-01']
teams = {}
for date in dates:
    for link in links:
        linkstring = link.get('href')
        if linkstring == '#':
            # These are the different label rows (Shooting, Scoring, etc.)
            continue
        else:
            tablename = link.get_text(strip=True)
            # We also have a var tablename that contains the name of the current statistic
            if tablename not in TRstats:
                continue
        year = date[:4]
        # Finally we have a year variable. These are also useful for the naming conventions for SQL
        urltext = f"https://www.teamrankings.com{linkstring}?date={date}"
        url = requests.get(urltext)
        soup = BeautifulSoup(url.text, 'html.parser')
        table = soup.find('table')
        if table is None:
            print(tablename)
            print(date)
            continue
        tableRows = table.find_all('tr')
        for row in tableRows:
            columns = row.find_all("td")
            if len(columns) > 0:     
                a_tag = columns[1].find('a')
                if a_tag:
                    # Extract the text if 'a' tag is present
                    team = a_tag.text
                else:
                    # Handle the case where 'a' tag is not present
                    team = columns[1].text
                team = convertteam(team)
                value = columns[2].text
                # value contains our value for the statistic in this link
                teamid = team + "_" + year
                if value.endswith("%"):
                    value = value[:-1]
                if(value == "--"):
                    value = 0
                else:
                    value = float(value)
                if teamid in teams:
                    teams[teamid].append([tablename, value])
                else:
                    teams[teamid] = [[tablename, value]]
    for i in range(len(otherlinks)):
        year = date[:4]
        urltext = str(otherlinks[i]) + f"?date={date}"
        url = requests.get(urltext)
        soup = BeautifulSoup(url.text, 'html.parser')
        table = soup.find('table')
        tablename = othernames[i]
        if table is None:
            print(tablename)
            print(date)
            continue
        tableRows = table.find_all('tr')
        for row in tableRows:
            columns = row.find_all("td")
            if len(columns) > 0:     
                a_tag = columns[1].find('a')
                if a_tag:
                    # Extract the text if 'a' tag is present
                    team = a_tag.text
                else:
                    # Handle the case where 'a' tag is not present
                    team = columns[1].text
                team = convertteam(team)
                value = columns[2].text
                teamid = team + "_" + year
                # we are getting multiple data points of interest from this single column so we must split it
                # This time we will have each team as the teamid thing instead
                if(value == "--"):
                    value = 0
                else:
                    value = float(value)
                if teamid in teams:
                    teams[teamid].append([tablename, value])
                else:
                    teams[teamid] = [[tablename, value]]
                    
    print(date + ' complete')
    if date == '2018-05-01' or date == '2021-05-01':
        time.sleep(300)       
        # had issues with website blocking
data = []
# Preparing to transform data into execute many format
for teamid, stats in teams.items():
    # iterate through the teamids - each of these accounts for one row in the database
    row = [0.5789023] * len(TRstats)
    # Pre-initializing is important because the stats are in different orders - more detail on this later
    for i in range(len(stats)):
        # iterate through the stats in each row 
        # Our problem is that these arrays contain the stats in one order, where as our SQL databse has another order
        # The solution is to find the index of our stat in TRstats, which has the stats named how TeamRankings calls them
        # This original stats arrays does align with our database, so now we can map whatever value we got to the correct index
        statindex = TRstats.index(stats[i][0])
        
        # Place our statistic at the index we found
        row[statindex] = stats[i][1]
    row.append(teamid)
    data.append(row)     
cursor.executemany(query, data)
print(cursor.statement)
cnx.commit()


