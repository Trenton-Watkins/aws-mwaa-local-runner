
import datetime 
import pandas as pd
import numpy as np
from pybaseball import *
import requests
from bs4 import BeauifulSoup
import sqlite3
import numpy as np
import pandas as pd
import gspread
import time



def run_baseball_stats():
    url = 'https://baseballmonster.com/Lineups.aspx?csv=1'

    lineups = pd.read_csv(url)

    lineups.columns = lineups.columns.str.replace(' ' ,'')

    team_abb = {"CLE" : "CLE","NYY" : "NYY","MIL" : "MIL","MIA" : "MIA","CIN" : "CIN","SD" : "SD","KC" : "KC","MIN" : "MIN","CHC" : "CHC"}
    player_names = {'JJ Bleday':'J.J. Bleday','Fernando Tatis Jr':'Fernando Tatis Jr.','Michael Harris':'Michael Harris II','Ronald Acuna':'Ronald Acuna Jr.','Jake Junis':'Jakob Junis','Lance McCullers':'Lance McCullers Jr.','Christopher Ellis':'Chris Ellis','Jackie Bradley': 'Jackie Bradley Jr.','Thomas Pham' : 'Tommy Pham', 'Michael Taylor':'Michael A. Taylor','AJ Pollock' : 'A.J. Pollock','JT Realmuto':'J.T. Realmuto','Cedric Mullins':'Cedric Mullins II','DJ Stewart':'D.J. Stewart','Kike Hernandez':'Enrique Hernandez','Luis V Garcia':'Luis Garcia','Peter Alonso':'Pete Alonso','LaMonte Wade':'LaMonte Wade Jr.','Vladimir Guerrero':'Vladimir Guerrero Jr.','Lourdes Gurriel':'Lourdes Gurriel Jr.','Shohei (H) Ohtani':'Shohei Ohtani','Nathaniel Lowe':'Nate Lowe','Jonathan Gray':'Jon Gray','Shohei (P) Ohtani':'Shohei Ohtani','J.T. Brubaker':'JT Brubaker','Mitchell White':'Mitch White','Joseph Wendle':'Joey Wendle','Albert Almora':'Albert Almora Jr.','Fernando Tatis':'Fernando Tatis Jr.','Yulieski Gurriel':'Yuli Gurriel',' Humberto Mejia':'Humberto Mejía'}

    hitters = lineups[lineups['battingorder'] != "SP"]
    pitchers = lineups[lineups['battingorder'] == "SP"]

    pitchers = pitchers.replace(team_abb)
    pitchers = pitchers.replace(player_names)

    hitters.head()

    reg = chadwick_register()
    ##reg.to_csv('Mlbreg.csv')
    ##files.download('Mlbreg.csv')

    reg['playername'] = reg['name_first'] + ' ' + reg['name_last']
    reg['Comma'] = reg['name_last'] + ',' + reg['name_first']
    current = reg[reg['mlb_played_last'] >= 2022]
    ##current.to_csv('Mlbreg.csv')
    ##files.download('Mlbreg.csv')
    current.head()

    lah = pd.read_csv('playersmlb.csv')
    # lah.to_csv('players.csv')
    ##files.download('players.csv')
    lah.head()

    players_list = pd.merge(current,lah[['BATS','THROWS','MLBID']],how = 'left',left_on='key_mlbam', right_on='MLBID')
    players_list = players_list.drop_duplicates(subset = ['key_bbref'])
    players_list.head()

    players_list['key_mlbam']

    players = pd.merge(hitters,players_list[['key_mlbam','key_fangraphs','BATS','THROWS']],how='left', left_on= 'mlbid', right_on='key_mlbam')

    players

    players = players.replace(team_abb)
    players = players.replace(player_names)



    sched ='https://baseballmonster.com/default.aspx'
    schedule = pd.read_html(sched)[2]
    schedule[['Away Team','Away TT']] = schedule['Away'].str.split(' ',n=2,expand=True)
    schedule[['Home Team','Home TT']] = schedule['Home'].str.split(' ',n=2,expand=True)
    schedule[['none','Home Team']] = schedule['Home Team'].str.split('@',n=1,expand=True)

    games =  schedule[['Away Team','Home Team']]

    slate=[]
    for i in range(len(games)):
        team1 = games.iloc[i,0]
        team2 = games.iloc[i,1]
        game =[team1,team2]
        slate.append(game)

    mlb = pd.DataFrame(slate, columns= ['Team','Opp'])

    mlb['Opp'] = mlb.Opp.str.replace('G1','')
    mlb['Opp'] = mlb.Opp.str.replace('G2','')
    mlb = mlb.drop_duplicates()


    bats = pd.merge(players,mlb,how='left',left_on='teamcode', right_on = 'Team')
    bats = pd.merge(bats,mlb, how = 'left',left_on = 'teamcode',right_on = 'Opp')
    bats['Opponent'] = np.where(bats['Opp_x'].isnull() , bats['Team_y'] ,bats['Opp_x'])

    name_replace = {'Jazz Chisholm':'Jazz Chisholm Jr.','Yoshitomo Tsutsugo':'Yoshi Tsutsugo','Dee Gordon' : 'Dee Strange-Gordon','Steven Souza':'Steven Souza Jr.','Giovanny Urshela':'Gio Urshela'}
    bats = bats.replace(name_replace)

    slate = pd.merge(bats,pitchers[['teamcode','mlbid','playername']],how='left',left_on='Opponent',right_on='teamcode')


    slate = pd.merge(slate,players_list[['key_mlbam','THROWS']],how='left',left_on='mlbid_y',right_on='key_mlbam')


    slate = slate[['teamcode_x', 'game_date', 'game_number', 'mlbid_x', 'playername_x',
        'battingorder', 'confirmed', 'position', 'key_fangraphs',
        'BATS', 'Opponent', 'mlbid_y',
        'playername_y', 'THROWS_y']]

    slate.loc[slate.playername_x=="Eddy Diaz","BATS"] = "R"
    slate.loc[slate.playername_x=="Jorge Barrosa","BATS"] = "B"
    slate.loc[slate.playername_x=="Oliver Dunn","BATS"] = "L"



    slate
    slate.head()

    pitchersvl = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2023%7C2022%7C&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=L&hfSA=&game_date_gt=&game_date_lt=&hfInfield=&team=&position=&hfOutfield=&hfRO=&home_road=&hfFlag=&hfBBT=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&min_pas=0&outputFormat=json'

    test = requests.get(pitchersvl).json()

    pvl = pd.DataFrame(test)
    pvl = pvl[['player_id', 'player_name','slg', 'xslg', 'xslgdiff','pitcher']]

    hitters = requests.get("https://www.fangraphs.com/leaders-legacy.aspx?pos=all&stats=pit&lg=all&qual=0&type=c,4,49,58,38,36,53&season=2024&month=13&season1=2023&ind=0&team=0&rost=0&age=0&filter=&players=0&startdate=&enddate=&v_cr=legacy&page=1_1000")
    # pd.DataFrame(hitters.html)
    # print(hitters)


    soup = BeautifulSoup(hitters.content, 'html.parser')
    print(soup)
    players = []
    for p1 in soup.findAll(True, {"class":["rgRow", "rgAltRow"]}):
        player = []
        hits = 0
        for n, stat in enumerate(p1.findAll("td")):
            if n != 0 and n != 2:
                s = stat.text
                if n == 21:
                    hits = float(s.replace('%',''))
                else:
                    if n >= 21 and n<= 25:
                        player.append(float(s.replace('%',''))/hits)
                    else:
                        if s[-2:] == " %":
                            s = float(s.replace('%','')[:-2])/100
                        if n != 1:
                            s = s.replace('%','')
                        player.append(s)
        players.append(player)

    p_l = pd.DataFrame(players)

    p_l.columns = ['Name','IP','SLG','Hard%','FB%','LD%','Pull%']

    p_l

    hitters = requests.get("https://www.fangraphs.com/leaders-legacy.aspx?pos=all&stats=pit&lg=all&qual=0&type=c,4,49,58,38,36,53&season=2024&month=14&season1=2023&ind=0&team=0&rost=0&age=0&filter=&players=0&page=1_1000")
    soup = BeautifulSoup(hitters.content, 'html.parser')

    players = []
    for p1 in soup.findAll(True, {"class":["rgRow", "rgAltRow"]}):
        player = []
        hits = 0
        for n, stat in enumerate(p1.findAll("td")):
            if n != 0 and n != 2:
                s = stat.text
                if n == 21:
                    hits = float(s.replace('%',''))
                else:
                    if n >= 21 and n<= 25:
                        player.append(float(s.replace('%',''))/hits)
                    else:
                        if s[-2:] == " %":
                            s = s.replace('%','')[:-2]
                        if n != 1:
                            s = s.replace('%','')
                        player.append(s)
        players.append(player)

    p_r = pd.DataFrame(players)
    p_r.columns = ['Name','IP','SLG','Hard%','FB%','LD%','Pull%']
    p_r

    hitters = requests.get("https://www.fangraphs.com/leaders-legacy.aspx?pos=all&stats=bat&lg=all&qual=0&type=c,5,38,43,45,68,73&season=2024&month=13&season1=2023&ind=0&team=0&rost=0&age=0&filter=&players=0&startdate=&enddate=&v_cr=legacy&page=1_2000")
    soup = BeautifulSoup(hitters.content, 'html.parser')

    players = []
    for p1 in soup.findAll(True, {"class":["rgRow", "rgAltRow"]}):
        player = []
        hits = 0
        for n, stat in enumerate(p1.findAll("td")):
            if n != 0 and n != 2:
                s = stat.text
                if n == 21:
                    hits = float(s.replace('%',''))
                else:
                    if n >= 21 and n<= 25:
                        player.append(float(s.replace('%',''))/hits)
                    else:
                        if s[-2:] == " %":
                            s = s.replace('%','')[:-2]/100
                        if n != 1:
                            s = s.replace('%','')
                        player.append(s)
        players.append(player)

    h_l = pd.DataFrame(players)
    h_l.columns = ['Name','AB','SLG','LD%','FB%','Pull%','Hard %']
    h_l

    hitters = requests.get("https://www.fangraphs.com/leaders-legacy.aspx?pos=all&stats=bat&lg=all&qual=0&type=c,5,38,43,45,68,73&season=2024&month=14&season1=2023&ind=0&team=0&rost=0&age=0&filter=&players=0&startdate=2023-01-01&enddate=2024-12-31&sort=3,d&page=1_2000")
    soup = BeautifulSoup(hitters.content, 'html.parser')

    players = []
    for p1 in soup.findAll(True, {"class":["rgRow", "rgAltRow"]}):
        player = []
        hits = 0
        for n, stat in enumerate(p1.findAll("td")):
            if n != 0 and n != 2:
                s = stat.text
                if n == 21:
                    hits = float(s.replace('%',''))
                else:
                    if n >= 21 and n<= 25:
                        player.append(float(s.replace('%',''))/hits)
                    else:
                        if s[-2:] == " %":
                            s = s.replace('%','')[:-2]/100
                        if n != 1:
                            s = s.replace('%','')
                        player.append(s)
        players.append(player)

    h_r = pd.DataFrame(players)
    h_r.columns = ['Name','AB','SLG','LD%','FB%','Pull%','Hard %']
    h_r

    merged = pd.merge(slate,h_r,how='left', left_on='playername_x', right_on='Name')

    merged

    merged = pd.merge(merged,h_l,how='left', left_on='playername_x', right_on='Name')

    merged.head()

    merged = pd.merge(merged,p_l,how='left', left_on='playername_y', right_on='Name')

    merged.head()

    merged = pd.merge(merged,p_r,how='left', left_on='playername_y', right_on='Name')

    merged.head()

    merged.columns =['teamcode_x', 'game_date', 'game_number', 'mlbid_x', 'playername_x',
        'battingorder', 'confirmed', 'position', 'key_fangraphs', 'bats',
        'Opponent', 'mlbid_y', 'playername_y', 'throws_y', 'Name_x1', 'AB_x',
        'SLG_x', 'LD%_x', 'FB%_x', 'Pull%_x', 'Hard %_x', 'Name_y', 'AB_y',
        'SLG_y', 'LD%_y', 'FB%_y', 'Pull%_y', 'Hard %_y', 'Name_x2', 'IP_x',
        'SLG_x', 'Hard%_x', 'FB%_x', 'LD%_x', 'Pull%_x', 'Name_y', 'IP_y',
        'SLG_y', 'Hard%_y', 'FB%_y', 'LD%_y', 'Pull%_y']

    check = merged[merged['Name_x1'].isnull()]

    merged.columns
    merged.columns = ['teamcode', 'game_date', 'game_number', 'mlbid_x', 'playername_x',
                    'battingorder', 'confirmed', 'position', 'key_fangraphs', 'bats',
                    'Opponent', 'mlbid_y', 'playername_y', 'throws_y', 'Name_x',
                    'AB_r', 'SLG_r', 'LD%_r', 'FB%_r', 'Pull%_r', 'Hard %_r', 'Name_y',
                    'AB_l', 'SLG_l', 'LD%_l', 'FB%_l', 'Pull%_l', 'Hard %_l', 'Name_x',
                    'IP_l', 'P_SLG_l', 'P_Hard%_l', 'P_FB%_l', 'P_LD%_l', 'P_Pull%_l', 'Name_y',
                    'IP_r', 'P_SLG_r', 'P_Hard%_r', 'P_FB%_r', 'P_LD%_r', 'P_Pull%_r']

    merged

    merged.columns

    merged = merged[['teamcode', 'game_date', 'game_number', 'mlbid_x', 'playername_x',
        'battingorder', 'confirmed', 'position', 'key_fangraphs', 'bats',
        'Opponent', 'mlbid_y', 'playername_y', 'throws_y',
        'AB_r', 'SLG_r', 'LD%_r', 'FB%_r', 'Pull%_r', 'Hard %_r',
        'AB_l', 'SLG_l', 'LD%_l', 'FB%_l', 'Pull%_l', 'Hard %_l',
        'IP_l', 'P_SLG_l', 'P_Hard%_l', 'P_FB%_l', 'P_LD%_l', 'P_Pull%_l',
        'IP_r', 'P_SLG_r', 'P_Hard%_r', 'P_FB%_r', 'P_LD%_r',
        'P_Pull%_r']]

    merged.head()

    p_barrels_l = "https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7C&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2023%7C2022%7C&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=L&hfSA=6%7C&game_date_gt=&game_date_lt=&hfInfield=&team=&position=&hfOutfield=&hfRO=&home_road=&hfFlag=&hfBBT=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&min_pas=0&chk_bip=on&outputFormat=json"
    p_barrels_r = "https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7C&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2023%7C2022%7C&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=R&hfSA=6%7C&game_date_gt=&game_date_lt=&hfInfield=&team=&position=&hfOutfield=&hfRO=&home_road=&hfFlag=&hfBBT=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&min_pas=0&chk_bip=on&outputFormat=json"
    b_barrels_r = "https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7C&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2023%7C2022%7C&hfSit=&player_type=batter&hfOuts=&opponent=&pitcher_throws=R&batter_stands=&hfSA=6%7C&game_date_gt=&game_date_lt=&hfInfield=&team=&position=&hfOutfield=&hfRO=&home_road=&hfFlag=&hfBBT=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&min_pas=0&chk_bip=on&outputFormat=json"
    b_barrels_l = "https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7C&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2023%7C2022%7C&hfSit=&player_type=batter&hfOuts=&opponent=&pitcher_throws=L&batter_stands=&hfSA=6%7C&game_date_gt=&game_date_lt=&hfInfield=&team=&position=&hfOutfield=&hfRO=&home_road=&hfFlag=&hfBBT=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&min_pas=0&chk_bip=on&outputFormat=json"

    p_bar_l = pd.DataFrame(requests.get(p_barrels_l).json())
    p_bar_r = pd.DataFrame(requests.get(p_barrels_r).json())
    b_bar_l = pd.DataFrame(requests.get(b_barrels_l).json())
    b_bar_r = pd.DataFrame(requests.get(b_barrels_r).json())

    p_bar_l = p_bar_l[['player_id','pitch_percent']]
    p_bar_r = p_bar_r[['player_id','pitch_percent']]
    b_bar_l = b_bar_l[['player_id','pitch_percent']]
    b_bar_r = b_bar_r[['player_id','pitch_percent']]

    p_bar_l.head()

    merged= pd.merge(merged , p_bar_l, how='left', left_on='mlbid_y',right_on='player_id')
    merged= pd.merge(merged , p_bar_r, how='left', left_on='mlbid_y',right_on='player_id')
    merged= pd.merge(merged , b_bar_l, how='left', left_on='mlbid_x',right_on='player_id')
    merged= pd.merge(merged , b_bar_r, how='left', left_on='mlbid_x',right_on='player_id')

    merged.columns

    merged.columns = ['teamcode', 'game_date', 'game_number', 'mlbid_x', 'playername_x',
        'battingorder', 'confirmed', 'position', 'key_fangraphs', 'bats',
        'Opponent', 'mlbid_y', 'playername_y', 'throws_y', 'AB_r',
        'SLG_r', 'LD%_r', 'FB%_r', 'Pull%_r', 'Hard %_r', 'AB_l', 'SLG_l',
        'LD%_l', 'FB%_l', 'Pull%_l', 'Hard %_l', 'IP_l', 'P_SLG_l', 'P_Hard%_l',
        'P_FB%_l', 'P_LD%_l', 'P_Pull%_l', 'IP_r', 'P_SLG_r', 'P_Hard%_r',
        'P_FB%_r', 'P_LD%_r', 'P_Pull%_r', 'player_id_x', 'P Barrel v L',
        'player_id_y', 'P Barrel v R', 'player_id_x', 'B Barrel v L',
        'player_id_y', 'B Barrel v R']

    merged = merged[['teamcode', 'game_date','playername_x',
        'battingorder', 'confirmed','bats',
        'Opponent','playername_y', 'throws_y', 'AB_r',
        'SLG_r', 'LD%_r', 'FB%_r', 'Pull%_r', 'Hard %_r', 'AB_l', 'SLG_l',
        'LD%_l', 'FB%_l', 'Pull%_l', 'Hard %_l', 'IP_l', 'P_SLG_l', 'P_Hard%_l',
        'P_FB%_l', 'P_LD%_l', 'P_Pull%_l', 'IP_r', 'P_SLG_r', 'P_Hard%_r',
        'P_FB%_r', 'P_LD%_r', 'P_Pull%_r', 'P Barrel v L',
            'P Barrel v R',  'B Barrel v L',
        'B Barrel v R']]

    merged['bats'] = np.where(merged.bats =='B',np.where(merged.throws_y=='L','R','L'),merged.bats)

    merged

    merged['Bat SLG'] = np.where(merged.throws_y == 'L',merged['SLG_l'],merged['SLG_r'])
    merged['Bat LD%'] = np.where(merged.throws_y == 'L',merged['LD%_l'],merged['LD%_r'])
    merged['Bat FB%'] = np.where(merged.throws_y == 'L',merged['FB%_l'],merged['FB%_r'])
    merged['Bat Pull%'] = np.where(merged.throws_y == 'L',merged['Pull%_l'],merged['Pull%_r'])
    merged['Bat Hard%'] = np.where(merged.throws_y == 'L',merged['Hard %_l'],merged['Hard %_r'])
    merged['Bat Bar%'] = np.where(merged.throws_y == 'L',merged['B Barrel v L'],merged['B Barrel v R'])
    merged['Bat AB'] = np.where(merged.throws_y == 'L',merged['AB_r'],merged['AB_l'])
    merged['P SLG'] = np.where(merged.bats == 'L',merged['P_SLG_l'],merged['P_SLG_r'])
    merged['P LD%'] = np.where(merged.bats == 'L',merged['P_LD%_l'],merged['P_LD%_r'])
    merged['P FB%'] = np.where(merged.bats == 'L',merged['P_FB%_l'],merged['P_FB%_r'])
    merged['P Pull%'] = np.where(merged.bats == 'L',merged['P_Pull%_l'],merged['P_Pull%_r'])
    merged['P Hard%'] = np.where(merged.bats == 'L',merged['P_Hard%_l'],merged['P_Hard%_r'])
    merged['P Bar%'] = np.where(merged.bats == 'L',merged['P Barrel v L'],merged['P Barrel v R'])
    merged['P IP'] = np.where(merged.bats == 'L',merged['IP_l'],merged['IP_r'])

    merged.columns

    merged.columns = ['Team', 'Date', 'Batter', 'Batting Order', 'confirmed',
        'bats', 'Opponent', 'Pitcher', 'Throws', 'AB_r', 'SLG_r',
        'LD%_r', 'FB%_r', 'Pull%_r', 'Hard %_r', 'AB_l', 'SLG_l', 'LD%_l',
        'FB%_l', 'Pull%_l', 'Hard %_l', 'IP_l', 'P_SLG_l', 'P_Hard%_l',
        'P_FB%_l', 'P_LD%_l', 'P_Pull%_l', 'IP_r', 'P_SLG_r', 'P_Hard%_r',
        'P_FB%_r', 'P_LD%_r', 'P_Pull%_r', 'P Barrel v L', 'P Barrel v R',
        'B Barrel v L', 'B Barrel v R', 'Bat SLG', 'Bat LD%', 'Bat FB%',
        'Bat Pull%', 'Bat Hard%', 'Bat Bar%', 'Bat AB', 'P SLG', 'P LD%',
        'P FB%', 'P Pull%', 'P Hard%', 'P Bar%', 'P IP']

    merged = merged[['Team', 'Date', 'Batter', 'Batting Order', 'confirmed',
        'bats', 'Team', 'Opponent', 'Pitcher', 'Throws', 'Bat SLG', 'Bat LD%', 'Bat FB%',
        'Bat Pull%', 'Bat Hard%', 'Bat Bar%', 'Bat AB', 'P SLG', 'P LD%',
        'P FB%', 'P Pull%', 'P Hard%', 'P Bar%', 'P IP']]

    merged['Bat Pull%']=pd.to_numeric(merged['Bat Pull%'], errors='coerce').fillna(0)
    merged['Bat Hard%']=pd.to_numeric(merged['Bat Hard%'], errors='coerce').fillna(0)

    merged = merged.fillna(0)

    # bryce =merged[merged['Batter'] == 'Bryce Harper']

    merged.head(120)

    cols = ['Bat SLG', 'Bat LD%', 'Bat FB%',
        'Bat Pull%', 'Bat Hard%', 'Bat Bar%', 'Bat AB', 'P SLG', 'P LD%',
        'P FB%', 'P Pull%', 'P Hard%', 'P Bar%', 'P IP']

    merged[cols] = merged[cols].apply(pd.to_numeric)

    div_cols = [ 'Bat LD%', 'Bat FB%',
        'Bat Pull%', 'Bat Hard%', 'Bat Bar%','P LD%',
        'P FB%', 'P Pull%', 'P Hard%', 'P Bar%','P SLG' ]

    merged[div_cols] = merged[div_cols] /100

    merged['Rating'] = merged['Bat SLG'] + merged['Bat LD%'] + merged['Bat FB%'] + merged['Bat Pull%'] + merged['Bat Hard%'] + merged['Bat Bar%'] + merged['P Bar%'] + merged['P FB%'] + merged['P Hard%'] + merged['P LD%'] + merged['P Pull%'] + merged['P SLG']

    merged

    merged.to_csv('hitting data.csv')

    hitters = merged


    slate.to_csv('mlbslate.csv')

    p_vs_left_last = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2023%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=L&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=name&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    p_vs_left_last = pd.DataFrame(requests.get(p_vs_left_last).json())
    p_vs_left_last.to_csv('pvllast.csv')

    p_vs_right_last = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2023%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=R&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=name&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    p_vs_right_last = pd.DataFrame(requests.get(p_vs_right_last).json())
    p_vs_right_last.to_csv('pvrlast.csv')

    p_vs_left_current = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2024%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=L&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=name&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    p_vs_left_current = pd.DataFrame(requests.get(p_vs_left_current).json())
    p_vs_left_current.to_csv('pvlcurrent.csv')

    p_vs_right_current = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2024%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=R&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=name&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    p_vs_right_current = pd.DataFrame(requests.get(p_vs_right_current).json())
    p_vs_right_current.to_csv('pvrcurrent.csv') 

    p_vs_left_last_type = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2023%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=L&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=pitch-type&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    p_vs_left_last_type = pd.DataFrame(requests.get(p_vs_left_last_type).json())

    p_vs_right_last_type = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2023%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=R&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=pitch-type&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    p_vs_right_last_type = pd.DataFrame(requests.get(p_vs_right_last_type).json())


    p_vs_left_current_type = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2024%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=L&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=pitch-type&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    p_vs_left_current_type = pd.DataFrame(requests.get(p_vs_left_current_type).json())

    p_vs_right_current_type = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2024%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=R&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=pitch-type&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    p_vs_right_current_type = pd.DataFrame(requests.get(p_vs_right_current_type).json())


    b_vs_left_last = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2023%7C&hfSit=&player_type=batter&hfOuts=&hfOpponent=&pitcher_throws=L&batter_stands=&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=name&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    b_vs_left_last = pd.DataFrame(requests.get(b_vs_left_last).json())
    b_vs_left_last.to_csv('bvllast.csv')

    b_vs_right_last = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2023%7C&hfSit=&player_type=batter&hfOuts=&hfOpponent=&pitcher_throws=R&batter_stands=&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=name&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    b_vs_right_last = pd.DataFrame(requests.get(b_vs_right_last).json())
    b_vs_right_last.to_csv('bvrlast.csv')

    b_vs_left_current = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2024%7C&hfSit=&player_type=batter&hfOuts=&hfOpponent=&pitcher_throws=L&batter_stands=&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=name&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    b_vs_left_current = pd.DataFrame(requests.get(b_vs_left_current).json())
    b_vs_left_current.to_csv('bvlcurrent.csv')


    b_vs_right_current = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2024%7C&hfSit=&player_type=batter&hfOuts=&hfOpponent=&pitcher_throws=R&batter_stands=&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=name&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    b_vs_right_current = pd.DataFrame(requests.get(b_vs_right_current).json())
    b_vs_right_current.to_csv('bvrcurrent.csv')

    b_vs_left_last_type = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2023%7C&hfSit=&player_type=batter&hfOuts=&hfOpponent=&pitcher_throws=L&batter_stands=&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=pitch-type&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    b_vs_left_last_type = pd.DataFrame(requests.get(b_vs_left_last_type).json())

    b_vs_right_last_type = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2023%7C&hfSit=&player_type=batter&hfOuts=&hfOpponent=&pitcher_throws=R&batter_stands=&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=pitch-type&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    b_vs_right_last_type = pd.DataFrame(requests.get(b_vs_right_last_type).json())


    b_vs_left_current_type = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2024%7C&hfSit=&player_type=batter&hfOuts=&hfOpponent=&pitcher_throws=L&batter_stands=&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=pitch-type&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    b_vs_left_current_type = pd.DataFrame(requests.get(b_vs_left_current_type).json())

    b_vs_right_current_type = 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2024%7C&hfSit=&player_type=batter&hfOuts=&hfOpponent=&pitcher_throws=R&batter_stands=&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=pitch-type&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&outputFormat=json'
    b_vs_right_current_type = pd.DataFrame(requests.get(b_vs_right_current_type).json())


    team = 'BOS'
    slated = slate[slate['teamcode_x'] ==team]
    pitcher_hand = slated['THROWS_y'].iloc[0]
    pitcher_name = slated['playername_y'].iloc[0]
    pitcher_id = slated['mlbid_y'].iloc[0]
    period = 'current'

    if pitcher_hand == 'L' :
        batter_stats = pd.merge(slated,b_vs_left_current,how='left',left_on='mlbid_x',right_on='player_id').fillna(0)
        batter_stats = batter_stats[['teamcode_x','playername_x','battingorder','confirmed','position','BATS','Opponent','playername_y','THROWS_y','iso','woba','xwoba','k_percent','bb_percent','hardhit_percent']]
    else:
        batter_stats = pd.merge(slated,b_vs_right_current,how='left',left_on='mlbid_x',right_on='player_id').fillna(0)
        batter_stats = batter_stats[['teamcode_x','playername_x','battingorder','confirmed','position','BATS','Opponent','playername_y','THROWS_y','iso','woba','xwoba','k_percent','bb_percent','hardhit_percent']]
    
    if period == 'current':
        pitcher_stats_l = p_vs_left_current[p_vs_left_current['player_id'] == pitcher_id] 
        
        pitcher_stats_l.columns
        pitcher_stats_l = pitcher_stats_l[['pitches', 'player_id', 'player_name', 'total_pitches', 'pitch_percent',
            'ba', 'xba', 'xbadiff','slg',
            'xslg', 'xslgdiff', 'babip',
            'iso',
            'woba',  'xwoba', 'wobadiff', 'pa', 
            'hrs',  'k_percent', 'bb_percent',
            'swing_miss_percent','hardhit_percent',
            'barrels_per_bbe_percent', 'barrels_per_pa_percent', 'barrels_total',
            'launch_speed', 'exit_velocity', 'launch_angle','spin_rate','obp', 'xobp', 'xobpdiff',  
            'p_throws', 'pitcher']]
        
        pitcher_stats_r = p_vs_right_current[p_vs_right_current['player_id'] == pitcher_id] 
        
        
        pitcher_stats_r = pitcher_stats_r[['pitches', 'player_id', 'player_name', 'total_pitches', 'pitch_percent',
            'ba', 'xba', 'xbadiff','slg',
            'xslg', 'xslgdiff', 'babip',
            'iso',
            'woba',  'xwoba', 'wobadiff', 'pa', 
            'hrs',  'k_percent', 'bb_percent',
            'swing_miss_percent','hardhit_percent',
            'barrels_per_bbe_percent', 'barrels_per_pa_percent', 'barrels_total',
            'launch_speed', 'exit_velocity', 'launch_angle','spin_rate','obp', 'xobp', 'xobpdiff',  
            'p_throws', 'pitcher']]
        
        
        
        test = pitcher_stats_l.transpose()
        test2 = pitcher_stats_r.transpose()
        
        mainpitcher = pd.merge(test,test2, left_index = True ,right_index=True)
        mainpitcher.columns = ['v_L','v_R']

    elif period =='last':
        pitcher_stats_l = p_vs_left_last[p_vs_left_last['player_id'] == pitcher_id] 
        
        pitcher_stats_l.columns
        pitcher_stats_l = pitcher_stats_l[['pitches', 'player_id', 'player_name', 'total_pitches', 'pitch_percent',
            'ba', 'xba', 'xbadiff','slg',
            'xslg', 'xslgdiff', 'babip',
            'iso',
            'woba',  'xwoba', 'wobadiff', 'pa', 
            'hrs',  'k_percent', 'bb_percent',
            'swing_miss_percent','hardhit_percent',
            'barrels_per_bbe_percent', 'barrels_per_pa_percent', 'barrels_total',
            'launch_speed', 'exit_velocity', 'launch_angle','spin_rate','obp', 'xobp', 'xobpdiff',  
            'p_throws', 'pitcher']]
        
        pitcher_stats_r = p_vs_right_last[p_vs_right_last['player_id'] == pitcher_id] 
        
        
        pitcher_stats_r = pitcher_stats_r[['pitches', 'player_id', 'player_name', 'total_pitches', 'pitch_percent',
            'ba', 'xba', 'xbadiff','slg',
            'xslg', 'xslgdiff', 'babip',
            'iso',
            'woba',  'xwoba', 'wobadiff', 'pa', 
            'hrs',  'k_percent', 'bb_percent',
            'swing_miss_percent','hardhit_percent',
            'barrels_per_bbe_percent', 'barrels_per_pa_percent', 'barrels_total',
            'launch_speed', 'exit_velocity', 'launch_angle','spin_rate','obp', 'xobp', 'xobpdiff',  
            'p_throws', 'pitcher']]
        
        
        
        test = pitcher_stats_l.transpose()
        test2 = pitcher_stats_r.transpose()
            
        mainpitcher = pd.merge(test,test2, left_index = True ,right_index=True)
        mainpitcher.columns = ['v_L','v_R']
        
    mainpitcher = mainpitcher.reset_index()

    teamlist = slate['teamcode_x'].unique().tolist()


    gc = gspread.service_account(filename = 'wnba-files-8e603d581b08.json')  
    main = gc.open_by_url('https://docs.google.com/spreadsheets/d/1VGPOayA65DmOIFPg16rEywQZxC0-x4liYST9VvVjGRI/edit')

    for name in teamlist:
    
        try:
            main.add_worksheet(title=name, rows=100, cols=20)        
        except:
            pass
        
    # sheet =gc.open_by_url('https://docs.google.com/spreadsheets/d/1VGPOayA65DmOIFPg16rEywQZxC0-x4liYST9VvVjGRI/edit').worksheet(f"{team}")

    # sheet.update('A10',mainpitcher.values.tolist())

    # sheet.update('E10',batter_stats.values.tolist())


    for team in teamlist:
            
        slated = slate[slate['teamcode_x'] ==team]
        pitcher_hand = slated['THROWS_y'].iloc[0]
        pitcher_name = slated['playername_y'].iloc[0]
        pitcher_id = slated['mlbid_y'].iloc[0]
        period = 'current'
        
        if pitcher_hand == 'L' :
            batter_stats = pd.merge(slated,b_vs_left_current,how='left',left_on='mlbid_x',right_on='player_id').fillna(0)
            batter_stats = batter_stats[['teamcode_x','playername_x','battingorder','confirmed','position','BATS','Opponent','playername_y','THROWS_y','iso','woba','xwoba','k_percent','bb_percent','hardhit_percent']]

            batter_stats_last = pd.merge(slated,b_vs_left_last,how='left',left_on='mlbid_x',right_on='player_id').fillna(0)
            batter_stats_last = batter_stats_last[['teamcode_x','playername_x','battingorder','confirmed','position','BATS','Opponent','playername_y','THROWS_y','iso','woba','xwoba','k_percent','bb_percent','hardhit_percent']]    
        else:
            batter_stats = pd.merge(slated,b_vs_right_current,how='left',left_on='mlbid_x',right_on='player_id').fillna(0)
            batter_stats = batter_stats[['teamcode_x','playername_x','battingorder','confirmed','position','BATS','Opponent','playername_y','THROWS_y','iso','woba','xwoba','k_percent','bb_percent','hardhit_percent']]
            batter_stats_last = pd.merge(slated,b_vs_right_last,how='left',left_on='mlbid_x',right_on='player_id').fillna(0)
            batter_stats_last = batter_stats_last[['teamcode_x','playername_x','battingorder','confirmed','position','BATS','Opponent','playername_y','THROWS_y','iso','woba','xwoba','k_percent','bb_percent','hardhit_percent']]
    

        pitcher_stats_l = p_vs_left_current[p_vs_left_current['player_id'] == pitcher_id] 
        
        
        pitcher_stats_l = pitcher_stats_l[['pitches', 'player_id', 'player_name', 'total_pitches', 'babip',
            'iso',
            'woba',  'xwoba', 'wobadiff',  'k_percent', 'bb_percent',
            'swing_miss_percent','hardhit_percent',
            'barrels_per_pa_percent','spin_rate', 
            'p_throws']]
        
        pitcher_stats_r = p_vs_right_current[p_vs_right_current['player_id'] == pitcher_id] 
        
        
    
        pitcher_stats_r = pitcher_stats_r[['pitches', 'player_id', 'player_name', 'total_pitches', 'babip',
            'iso',
            'woba',  'xwoba', 'wobadiff',  'k_percent', 'bb_percent',
            'swing_miss_percent','hardhit_percent',
            'barrels_per_pa_percent','spin_rate', 
            'p_throws']]
        
        
        pitcher_stats_l_last = p_vs_left_last[p_vs_left_last['player_id'] == pitcher_id] 
        
        pitcher_stats_l_last = pitcher_stats_l_last[['pitches', 'player_id', 'player_name', 'total_pitches', 'babip',
            'iso',
            'woba',  'xwoba', 'wobadiff',  'k_percent', 'bb_percent',
            'swing_miss_percent','hardhit_percent',
            'barrels_per_pa_percent','spin_rate', 
            'p_throws']]
        
        pitcher_stats_r_last = p_vs_right_last[p_vs_right_last['player_id'] == pitcher_id] 
        
        
        pitcher_stats_r_last = pitcher_stats_r_last[['pitches', 'player_id', 'player_name', 'total_pitches', 'babip',
            'iso',
            'woba',  'xwoba', 'wobadiff',  'k_percent', 'bb_percent',
            'swing_miss_percent','hardhit_percent',
            'barrels_per_pa_percent','spin_rate', 
            'p_throws']]
        
        
        
        test = pitcher_stats_l.transpose()
        test2 = pitcher_stats_r.transpose()
        
        mainpitcher = pd.merge(test,test2, left_index = True ,right_index=True)
        try:
            mainpitcher.columns = ['v_L','v_R']
        except:
            pass
            
        mainpitcher = mainpitcher.reset_index()
        
        testlast = pitcher_stats_l_last.transpose()
        test2last = pitcher_stats_r_last.transpose()
        
        mainpitcherlast = pd.merge(testlast,test2last, left_index = True ,right_index=True)
        try:
            mainpitcherlast.columns = ['v_L','v_R']
        except:
            pass
        
        mainpitcherlast = mainpitcherlast.reset_index()
        
        sheet =gc.open_by_url('https://docs.google.com/spreadsheets/d/1VGPOayA65DmOIFPg16rEywQZxC0-x4liYST9VvVjGRI/edit').worksheet(f"{team}")

        sheet.update('A12',mainpitcher.values.tolist())
        sheet.update('A32',mainpitcherlast.values.tolist())

        sheet.update('E12',batter_stats.values.tolist())
        sheet.update('E32',batter_stats_last.values.tolist())




    # base_url = 'https://docs.google.com/spreadsheets/d'
    # export_url = f'{base_url}/1VGPOayA65DmOIFPg16rEywQZxC0-x4liYST9VvVjGRI/export?format=pdf&gid=1834437148'


    # response = requests.get(export_url)

    # # Save the response content as a PDF file
    # pdf_file = 'DET.pdf'

    # with open(pdf_file, 'wb') as f:
    #     f.write(response.content)
    




    print('complete')
    return print('complete')

