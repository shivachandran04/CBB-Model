# -*- coding: utf-8 -*-

import csv
import mysql.connector

# Constants for WSA server
HOST = "34.68.250.121"
USER = "wsa"
PASSWORD = "LeBron>MJ!"
DB = "F24-CollegeBBall"

def makeTables():
    """Initialize the schema and insert tables into MySQL"""
    cnx  = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DB
        )
    
    cursor = cnx.cursor()
    
    # Create stats table
    cursor.execute(
        "CREATE TABLE `team_stats` ("
        "  `team_id` VARCHAR(50) NOT NULL,"
        "  `team_name` VARCHAR(50) NOT NULL,"
        "  `year` YEAR NOT NULL,"
        "  `Pts/Gm` DEC(10, 3),"
        "  `Opp. Pts/Gm` DEC(10, 3),"
        "  `FG%` DEC(10, 3),"
        "  `Opp. FG%` DEC(10, 3),"
        "  `2Pt%` DEC(10, 3),"
        "  `Opp. 2Pt%` DEC(10, 3),"
        "  `3Pt%` DEC(10, 3),"
        "  `Opp. 3Pt%` DEC(10, 3),"
        "  `FT%` DEC(10, 3),"
        "  `Opp. FT%` DEC(10, 3),"
        "  `TS%` DEC(10, 3),"
        "  `EffFG%` DEC(10, 3),"
        "  `%from2` DEC(10, 3),"
        "  `%from3` DEC(10, 3),"
        "  `FGAtt/Gm` DEC(10, 3),"
        "  `Opp. FGAtt/Gm` DEC(10, 3),"
        "  `FTAtt/Gm` DEC(10, 3),"
        "  `Opp. FTAtt/Gm` DEC(10, 3),"
        "  `3PtAtt/Gm` DEC(10, 3),"
        "  `Opp. 3PtAtt/Gm` DEC(10, 3),"
        "  `Fouls/Gm` DEC(10, 3),"
        "  `Opp. Fouls/Gm` DEC(10, 3),"
        "  `OReb%` DEC(10, 3),"
        "  `DReb%` DEC(10, 3),"
        "  `Blk%` DEC(10, 3),"
        "  `Opp. Blk%` DEC(10, 3),"
        "  `Stls/Gm` DEC(10, 3),"
        "  `Opp. Stls/Gm` DEC(10, 3),"
        "  `TOs/Gm` DEC(10, 3),"
        "  `Opp. TOs/Gm` DEC(10, 3),"
        "  `Asts/Gm` DEC(10, 3),"
        "  `Opp. Asts/Gm` DEC(10, 3),"
        "  `Pos/Gm` DEC(10, 3),"
        "  `TR Power` DEC(10, 3),"
        "  `TR Home Power` DEC(10, 3),"
        "  `TR Away Power` DEC(10, 3),"
        "  `TR Neutral Power` DEC(10, 3),"
        "  `TR SOS` DEC(10, 3),"
        "  `BenchPts/Gm` DEC(10, 3),"
        "  `FastbreakPts/Gm` DEC(10, 3),"
        "  `Returning Minutes` DEC(10, 3),"
        "  `Returning Pts` DEC(10, 3),"
        "  `KenPom Eff` DEC(10, 3),"
        "  `KenPom Off Eff` DEC(10, 3),"
        "  `KenPom Def Eff` DEC(10, 3),"
        "  `KenPom AdjTempo` DEC(10, 3),"
        "  `KenPom SOS` DEC(10, 3),"
        "  `KenPom Off SOS` DEC(10, 3),"
        "  `KenPom Def SOS` DEC(10, 3),"
        "  `BT Eff Height` DEC(10, 3),"
        "  `BT WAB` DEC(10, 3),"
        "  `BT Power` DEC(10, 3),"
        "  `BT Experience` DEC(10, 3),"
        "  `BT Talent` DEC(10, 3),"
        "  PRIMARY KEY (`team_id`)"
        ")"
        )
    
    # Create results table
    cursor.execute(
        "CREATE TABLE `game_results` ("
        "  `game_id` INT NOT NULL,"
        "  `Datetime` DATETIME NOT NULL,"
        "  `Team1 ID` VARCHAR(50) NOT NULL,"
        "  `Team2 ID` VARCHAR(50) NOT NULL,"
        "  `Team1 Score` INT NOT NULL,"
        "  `Team2 Score` INT NOT NULL,"
        "  `Team1 Home` BOOL NOT NULL,"
        "  `Team2 Home` BOOL NOT NULL,"
        "  PRIMARY KEY (`game_id`)"
        ")"
        )
    
    cursor.close()
    cnx.close()

def insertTeams(filename):
    """Takes csv filename with all team names and inserts teams and IDs for each team and year into MySQL database."""
    
    # Connect to database
    cnx  = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DB
        )
    cursor = cnx.cursor()
    
    # Read in team names
    with open(filename, newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
    
    years = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]
    
    # Write insert statement
    command = (
        "INSERT INTO team_stats "
        "(team_id, team_name, year) "
        "VALUES (%s, %s, %s)"
        )
    
    values = []
    
    # Permute team and year combos
    for team in data:
        team = team[0] # Stored as 1D list, so grab the team name
        
        for year in years:            
            # Generate ID
            team_id = team + "_" + year
            
            # Insert into table
            team_values = (team_id, team, year)  
            values.append(team_values)
    
    cursor.executemany(command, values)
    cnx.commit()
    cursor.close()
    cnx.close()
    
def queryTeamTable():
    """Print all entries in team stats table"""
    
    # Connect to database
    cnx = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DB
        )
    
    cursor = cnx.cursor()
    
    # Execute query
    query = ("SELECT * FROM team_stats")
    cursor.execute(query)
    res = cursor.fetchall()
    
    for e in res:
        print(e)
    
    # Close connection
    cursor.close()
    cnx.close()
    
def generateTrainingData():
    """Generate the table of team stats and game outcomes for model training"""
    
    # Connect to database
    cnx = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DB
        )
    
    cursor = cnx.cursor()
    
    # Query to combine game result with stats of first team in table 2
    query = ("SELECT * FROM game_results LEFT JOIN team_stats ON team_stats.team_id = game_results.`Team1 ID`")
    cursor.execute(query)
    
    res = cursor.fetchall()
    
    # Get column names
    cols = [c[0] for c in cursor.description]
    
    # Write to CSV
    filepath = open('results and team 1.csv', 'w')
    file = csv.writer(filepath)
    file.writerow(cols)
    file.writerows(res)
    filepath.close()
    
    # Query to combien game result with stats of second team into table 2
    query = ("SELECT * FROM game_results LEFT JOIN team_stats ON team_stats.team_id = game_results.`Team2 ID`")
    cursor.execute(query)
    
    res = cursor.fetchall()
    
    # Get column names
    cols = [c[0] for c in cursor.description]
    
    # Write to CSV
    filepath = open('results and team 2.csv', 'w')
    file = csv.writer(filepath)
    file.writerow(cols)
    file.writerows(res)
    filepath.close()
    
    
    # Close connection
    cursor.close()
    cnx.close()