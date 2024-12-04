
# -*- coding: utf-8 -*-
import mysql.connector
import requests
import re
from bs4 import BeautifulSoup

cnx  = mysql.connector.connect(
        host="34.68.250.121",
        user="wsa",
        password="LeBron>MJ!",
        database="F24-CollegeBBall"
    )
cursor = cnx.cursor()

# cursor = cnx.cursor()
# # class="dataTable table-striped"
# # https://stats.ncaa.org/rankings/national_ranking?academic_year=2023.0&division=1.0&ranking_period=144.0&sport_code=MBB&stat_seq=1285.0
years = ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024']
data = []
query = """
    UPDATE team_stats
    SET 
        `KenPom Eff` = %s,
        `KenPom Off Eff` = %s,
        `KenPom Def Eff` = %s,
        `KenPom AdjTempo` = %s,
        `KenPom SOS` = %s,
        `KenPom Off SOS` = %s,
        `KenPom Def SOS` = %s
    WHERE team_id = %s;
"""
for year in years:
#     print(year)
#     url = requests.get(f"https://stats.ncaa.org/rankings/national_ranking?academic_year={year}.0&division=1.0&ranking_period=167.0&sport_code=MBB&stat_seq=1285.0", headers = {
#         'User-Agent': 'Popular browser\'s user-agent',
#     })
#     soup = BeautifulSoup(url.text, 'html.parser')
#     table = soup.find('table', id='rankings_table')
#     rows = soup.find('tbody').find_all('tr')
#     for row in rows:
#         team = row.find('a').text.split(" (")[0]
#         fastbreak_points = float(rows[0].find_all('td', align='right')[2].text.strip())
#         team_id = team + '_' + year
#         cursor.execute(
#             '''
#             INSERT INTO team_stats (FastbreakPts/Gm,) VALUES (?,) WHERE team_id = ?
#             ''', (team_id,)
#         )

    link = f'https://kenpom.com/index.php?y={year}'
    url = requests.get(link, headers = {
        'User-Agent': 'Popular browser\'s user-agent',
    })
    soup = BeautifulSoup(url.text, "html.parser")

    #Have to double loop here because every 40 teams is a new tbody
    tables = soup.find("div", attrs={"id": "table-wrapper"}).find("table").find_all("tbody")

    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            try:
                cols[1]
            except:
                continue
            
            #Since this is for tournament teams, have to get rid of their ranking, hence [:-2]
            team = cols[1].text.split()
            if (team[-1].isdigit()):
                team.pop()
            team_name = ' '.join([item for item in team])
            net_rating = float(cols[4].text[1:])
            o = float(cols[5].text)
            d = float(cols[7].text)
            t = float(cols[9].text)
            sos = float(cols[13].text)
            oppo = float(cols[15].text)
            oppd = float(cols[17].text)
            team_id = team_name + '_' + year
            print(team_id)
            data.append((net_rating, o, d, t, sos, oppo, oppd, team_id))
            # query = "INSERT INTO team_stats (team_id, KenPom Eff, KenPom Off Eff, KenPom Def Eff, KenPom AdjTempo, KenPom SOS, KenPom Off SOS, KenPom Def SOS VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE KenPom Eff = VALUES(KenPom Eff), KenPom Eff = VALUES(KenPom Eff), KenPom Eff = VALUES(KenPom Eff), KenPom Off Eff = VALUES(KenPom Off Eff), KenPom Def Eff = VALUES(KenPom Eff), KenPom Eff = VALUES(KenPom Eff), KenPom Eff = VALUES(KenPom Eff)"
cursor.executemany(query, data)
print(cursor.statement)




# Create stats table
# cursor.execute(
#     "CREATE TABLE `team_stats` ("
#     "  `team_id` VARCHAR(50) NOT NULL,"
#     "  `team_name` VARCHAR(50) NOT NULL,"
#     "  `year` YEAR NOT NULL,"
#     "  `Pts/Gm` DEC(10, 3),"
#     "  `Opp. Pts/Gm` DEC(10, 3),"
#     "  `FG%` DEC(10, 3),"
#     "  `Opp. FG%` DEC(10, 3),"
#     "  `2Pt%` DEC(10, 3),"
#     "  `Opp. 2Pt%` DEC(10, 3),"
#     "  `3Pt%` DEC(10, 3),"
#     "  `Opp. 3Pt%` DEC(10, 3),"
#     "  `FT%` DEC(10, 3),"
#     "  `Opp. FT%` DEC(10, 3),"
#     "  `TS%` DEC(10, 3),"
#     "  `EffFG%` DEC(10, 3),"
#     "  `%from2` DEC(10, 3),"
#     "  `%from3` DEC(10, 3),"
#     "  `FGAtt/Gm` DEC(10, 3),"
#     "  `Opp. FGAtt/Gm` DEC(10, 3),"
#     "  `FTAtt/Gm` DEC(10, 3),"
#     "  `Opp. FTAtt/Gm` DEC(10, 3),"
#     "  `3PtAtt/Gm` DEC(10, 3),"
#     "  `Opp. 3PtAtt/Gm` DEC(10, 3),"
#     "  `Fouls/Gm` DEC(10, 3),"
#     "  `Opp. Fouls/Gm` DEC(10, 3),"
#     "  `OReb%` DEC(10, 3),"
#     "  `DReb%` DEC(10, 3),"
#     "  `Blk%` DEC(10, 3),"
#     "  `Opp. Blk%` DEC(10, 3),"
#     "  `Stls/Gm` DEC(10, 3),"
#     "  `Opp. Stls/Gm` DEC(10, 3),"
#     "  `TOs/Gm` DEC(10, 3),"
#     "  `Opp. TOs/Gm` DEC(10, 3),"
#     "  `Asts/Gm` DEC(10, 3),"
#     "  `Opp. Asts/Gm` DEC(10, 3),"
#     "  `Pos/Gm` DEC(10, 3),"
#     "  `TR Power` DEC(10, 3),"
#     "  `TR Home Power` DEC(10, 3),"
#     "  `TR Away Power` DEC(10, 3),"
#     "  `TR Neutral Power` DEC(10, 3),"
#     "  `TR SOS` DEC(10, 3),"
#     "  `BenchPts/Gm` DEC(10, 3),"
#     "  `FastbreakPts/Gm` DEC(10, 3),"
#     "  `Returning Minutes` DEC(10, 3),"
#     "  `Returning Pts` DEC(10, 3),"
#     "  `KenPom Eff` DEC(10, 3),"
#     "  `KenPom Off Eff` DEC(10, 3),"
#     "  `KenPom Def Eff` DEC(10, 3),"
#     "  `KenPom AdjTempo` DEC(10, 3),"
#     "  `KenPom SOS` DEC(10, 3),"
#     "  `KenPom Off SOS` DEC(10, 3),"
#     "  `KenPom Def SOS` DEC(10, 3),"
#     "  `BT Eff Height` DEC(10, 3),"
#     "  `BT WAB` DEC(10, 3),"
#     "  `BT Power` DEC(10, 3),"
#     "  `BT Experience` DEC(10, 3),"
#     "  `BT Talent` DEC(10, 3),"
#     "  PRIMARY KEY (`team_id`)"
#     ")"
#     )

# # Create results table
# cursor.execute(
#     "CREATE TABLE `game_results` ("
#     "  `game_id` INT NOT NULL,"
#     "  `Datetime` DATETIME NOT NULL,"
#     "  `Team1 ID` VARCHAR(50) NOT NULL,"
#     "  `Team2 ID` VARCHAR(50) NOT NULL,"
#     "  `Team1 Score` INT NOT NULL,"
#     "  `Team2 Score` INT NOT NULL,"
#     "  `Team1 Home` BOOL NOT NULL,"
#     "  `Team2 Home` BOOL NOT NULL,"
#     "  PRIMARY KEY (`game_id`)"
#     ")"
#     )

