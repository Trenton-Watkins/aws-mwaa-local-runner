    # -*- coding: utf-8 -*-
"""
Created on Fri May 19 09:45:03 2023

@author: trent
"""
import requests
import gspread
import copy
import random
from random import shuffle, choice
# from pulp import *
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from tqdm import tqdm
import datetime

# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import GradientBoostingRegressor
from dateutil.parser import parse

from pytz import timezone
import boto3
from io import StringIO


def run_wnba():
    minutesprojected = "Y"
    date = '2024-05-14'
    rotowire = 'https://www.rotowire.com/optimizer/api/wnba/players.php?slateID=1355'        

    cred ={
    "type": "service_account",
    "project_id": "wnba-files",
    "private_key_id": "8e603d581b08b30b3aca668cc986370ba2561f44",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCGz8/vx4IMwJv4\nJsvD3qCXnrf+xSOhNYRZIG7aEQgHYGM1azPqkf5biPKEvgZ577qziWVHZ+fnDv8B\ni/gNJDyhIpSpPyKBf1ijzXDdmLZo8idwmJ9JacYikPwwT4CL/NW2FMJnJRWxnAZS\nxpmJROAWzu0AK+NmqltPo9xHeszwxazO6Cxh8Hd9dm4xX8RD8jiTlUOTuQmBWEUn\nB7O5aTGTh9mqng9scs/Sf6MPkhimd06CjtXYS0oWVBHZt0AS9L+lVjo/6+eZrGAv\ntZeZ3u8+CSAMg54nBsB4jnFFlvFjxAZeQiv5IUxxbqkgfnfAeSd5VJ0QltrmL7n8\n4Tn+pD9NAgMBAAECggEABmamHSC4MlJP5Ym0CZ4PuntBCcIDBdPEb7XsRYLjAnSn\naUEokyXw8ZgwSbC7Hn2NPBqVOj17A5vQ7FogwkD69vytGdcjVIo/I3Hs/rySRqve\nxwgcIhB5ZalELCgONrdfz1gvfdXDMK5zl2kSH7QrPrH/C0/KKZUDpmWlnyg7xF/Q\nxzVrdKcXOz2p2rapZL3aORJDbZTksHluK+ENrIFj/bygQ/pS2WvxvVhoQkvwK+01\nMJ5XB4fOn53F5VkhjrNHnAzm7XbaL6+9lFMw0R9oGFTuI3chQYuG3QlGjWF2FpNa\nyh+zND9kEsCD0JV2FhmmcbSz+6+RpzA5VxFQ/2FGaQKBgQC52349M+8jiOOeREOb\nDtqIlk/I+RsNwm64BRQLGNgTrOUN1p5j0+YKbyEbu3UWW5MkHPaEcVHmo1h63Y+B\nXJxfEchVY8SF3u3GBzzNJUB1SHnwEkIUDMH3DJ7Z+IC4qK9e3a7BlkK+ZyDXOVWx\nyxleEO8oU0ajzwwgwE6BuEb9eQKBgQC5sJMd7EoFoPS0S0Pt27+5dN7uTqgUhQF6\nwt8mGlcJCQkD2DpsqR+bjRvMzvHsW8QsjNWy7AlwVJ0P2TWYLucroTsCjKi8se1e\n5gnB5PWUx6F0Gs/4PwUD82FMwLGmiTsArvSr5aX9PEElIVzAz8dG+4Bc6Ugf/ij2\n2ug7nxbfdQKBgEY4WLzU92Asoxsz3XsjJIwAhop/G6qaMGzUdlsu+syMQUp2MQe0\nkrCUhiTNMZLN7IzzaGxnyDLkulRJi6OrkuUUeeVROXn+3UU5jM5RacYmKPP8Yfzj\nSRGHMilWi4O5L/Eevp0joXoAytamMetnueDhcwqAVCsl2gYxwjeeoSu5AoGBAJ3b\nkJXlpF/4n08OlScGo4zj4AkzcQxQrhtQwye/SLJzrehI0BJEDIzOfw2z+FJ5COLe\nr2U16ChIpmUswLUpWnjqziGytlgD4snEQhNFs8jRlV4A1UAzQu5C3zrCrcPn8fL4\nySPBTUVlDfSk0hdQDBc6A7v4zcn05ZpGKxtUebtpAoGAdQiI/8cU2GWO8iLNoze8\n1BSS/qnwvtF7uMENtMOiSrtZacHURI8nMr9/Jm0EkqyYrCRNRSayAsaJyTebay6m\n2sZeWcTm57jLm9m/YX5LhmMckpbPta/Togqrledk9n/SYiWmuZfZBsJJpFAYttQZ\nXjpMAS4wN8ZvRll1wI1sL+U=\n-----END PRIVATE KEY-----\n",
    "client_email": "python-script@wnba-files.iam.gserviceaccount.com",
    "client_id": "108781183999293258757",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/python-script%40wnba-files.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
    }


    obj = boto3.client("s3",
                    aws_access_key_id='AKIAQRO37AJWNMC7G3OL',
                    aws_secret_access_key='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')


    def data_pull(file):
        project = obj.get_object(Bucket= 'cbbdata2023', Key= file) 
        frame = pd.read_csv(project['Body'])
        return frame



    # headers  = {
    #      'Connection': 'keep-alive',
    #      'Accept': 'application/json, text/plain, */*',
    #      'x-nba-stats-token': 'true',
    #      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
    #      'x-nba-stats-origin': 'stats',
    #      'Sec-Fetch-Site': 'same-origin',
    #      'Sec-Fetch-Mode': 'cors',
    #      'Referer': 'https://stats.nba.com/',
    #      'Accept-Encoding': 'gzip, deflate, br',
    #      'Accept-Language': 'en-US,en;q=0.9',
    #  }
    
    # check =[]

    # gameid = 1012400002

    # while gameid <= 1012400011:
    #     print(gameid)
    #     test = 'https://stats.wnba.com/stats/boxscoretraditionalv2?EndPeriod=10&EndRange=24000&GameID='+str(gameid)+'&RangeType=0&Season=2024-25&SeasonType=Pre+Season&StartPeriod=1&StartRange=1200'
    #     req = requests.get(test,headers=headers).json()
    #     gamehead = req['resultSets'][0]['headers']
    #     gamestat = req['resultSets'][0]['rowSet']
    #     table = pd.DataFrame(gamestat,columns = gamehead)
    #     check.append(table)
    #     gameid = gameid +1

    # game_logs = pd.concat(check)

    # game_logs['MIN'] =game_logs['MIN'].str[:2]



    # pgconn = psycopg2.connect(host='postgres-cbb.c4h8ukw5kz5z.us-east-1.rds.amazonaws.com',database = 'postgres',user='postgres',password='Tw236565!!')

    # pgconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    # pgcursor = pgconn.cursor()

    # pgcursor.execute("SET search_path TO wnba;")

    # engine = create_engine('postgresql+psycopg2://postgres:Tw236565!!@postgres-cbb.c4h8ukw5kz5z.us-east-1.rds.amazonaws.com/postgres')

    # game_logs.to_sql(name ='preseason_game_logs', schema = 'wnba',con = engine, if_exists='replace',index = False)


    def get_dkinfo():
            req = requests.get('https://www.draftkings.com/lobby/getcontests?sport=NBA')
            
            slate = req.json()
            
            draftgroups = slate['DraftGroups']
            
            df2 = pd.DataFrame(draftgroups)
            try:
                wnbacontest = df2[df2['ContestTypeId']==37]
                groupid = wnbacontest['DraftGroupId'].values[0]  
                

            except:
            
                wnbacontest = df2[df2['ContestStartTimeSuffix'].str.contains('(WNBA)')==True]
                groupid = wnbacontest['DraftGroupId'].values[0]
                    
            
            
            dg = 'https://api.draftkings.com/draftgroups/v1/draftgroups/' + str(groupid) +'/draftables'
            
            draft = requests.get(dg).json()
            
            draftables = draft['draftables']
            player_list = []
            for i in range(len(draftables)):
                    player = draftables[i]
                    name = player['displayName']
                    position = player['position']
                    ident = player['draftableId']
                    key = str(name) + " (" + str(ident) + ")"
                    salary = player['salary']
                    game = player['competitions'][0]['name'] + ' '+ player['competitions'][0]['startTime']
                    team = player['teamAbbreviation']
                    fppg = player['draftStatAttributes'][0]['value']
                    dkplayer= [position,key,name,ident,salary,game,team,fppg]
                    player_list.append(dkplayer)
                
            dksalaries = pd.DataFrame(player_list , columns= ['Position','Name + ID','Name','ID','Salary','Game Info','TeamAbbrev','AvgPointsPerGame'])
            dksalaries = dksalaries.drop_duplicates(subset=['Name','TeamAbbrev'])
            # dksalaries.to_csv('wnbadk.csv',index=False)
            csv_buffer = StringIO()
            
            dksalaries.to_csv(csv_buffer,index=False)
            
            obj.put_object(Bucket='cbbdata2023',Body = csv_buffer.getvalue(),Key = 'wnbadk.csv')
            
            
            
            dksalaries[['Away','@','Home','Time']] = dksalaries['Game Info'].str.split(' ',n=4,expand = True)
            
            dksalaries['Time']  = [parse(x) for x in dksalaries['Time']]
            
            dksalaries['Time'] = [x.replace(tzinfo=datetime.timezone.utc) for x in dksalaries['Time']]
                
            dksalaries['Time'] =  [x.astimezone(timezone('US/Eastern')) for x in dksalaries['Time']]      
            
            dksalaries['Time'] = [x.strftime("%m/%d/%Y %I:%M%p %Z") for x in dksalaries['Time']]
            
            dksalaries['Game Info'] = dksalaries['Away'] + '@' +dksalaries['Home'] +' ' + dksalaries['Time']
            
            dksalaries = dksalaries[['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info','TeamAbbrev', 'AvgPointsPerGame']]
            
            # gc = gspread.service_account(filename = 'wnba-files-8e603d581b08.json')
            gc = gspread.service_account_from_dict(cred)
            
            wnba = gc.open_by_url('https://docs.google.com/spreadsheets/d/1r78ZTtFK99J4HBgK-YeJncR7ruyTMycUVG6f7VogTCU/edit').sheet1
            
            wnba.batch_clear(['A2:M250'])
            wnba.update('A2', dksalaries.values.tolist())
            return wnbacontest


    def updateproj(date):
            # gc = gspread.service_account(filename = 'wnba-files-8e603d581b08.json')
            gc = gspread.service_account_from_dict(cred)
            
            todaysdate = datetime.date.today()
            season_id = ['2022','2023','2024']
            
            headers  = {
                'Connection': 'keep-alive',
                'Accept': 'application/json, text/plain, */*',
                'x-nba-stats-token': 'true',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
                'x-nba-stats-origin': 'stats',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'cors',
                'Referer': 'https://stats.nba.com/',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            
            game_log_url ='https://stats.wnba.com/stats/playergamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season='  +str(season_id) +'&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision='
            team_stats_url = 'https://stats.wnba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerMinute&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2023&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='
            gamesched = 'https://data.wnba.com/data/5s/v2015/json/mobile_teams/wnba/2024/league/10_full_schedule_tbds.json'
            teamdef ='https://stats.wnba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Opponent&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2023&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='
            schedule = pd.read_json(gamesched)
            
            mainschedule = []
            
            for i in range(len(schedule)):
                    gamestoday = schedule['lscd'][i]['mscd']['g']
                    check = pd.json_normalize(gamestoday)
                    mainschedule.append(check)
                
            fullschedule = pd.concat(mainschedule)
            roto_replace ={'Dorka Juhász':'Dorka Juhasz','Asia (AD) Durr':'Asia Durr','AD Durr': 'Asia Durr',"Azura Stevens":"Azurá Stevens","AzurÃƒÂ¡ Stevens":"Azurá Stevens", "AzurÃ¡ Stevens":"Azurá Stevens","Marine Johannès":"Marine Johannes","Li Meng":"Meng Li","Amanda Zahui B":'Amanda Zahui B.'}
            
            
            today = fullschedule[fullschedule['gdte'] ==  todaysdate ].reset_index()
            
            today = fullschedule[fullschedule['gdte'] ==  str(todaysdate)].reset_index()
            
            today = fullschedule[fullschedule['gdte'] ==  date ].reset_index()
            
            underdog = 'https://api.underdogfantasy.com/beta/v3/over_under_lines'
            
            try:    
                    req = requests.get(underdog).json()
                    dog = pd.json_normalize(req['over_under_lines'])
                    
                    main = pd.json_normalize(req['appearances'])
                    players = pd.json_normalize(req['players'])
                    players.columns
                    players['name'] = players['first_name'] + ' ' +players['last_name']
                    
                    game =pd.json_normalize(req['games'])
                    
                    
                    dog = dog[['id', 'stat_value','over_under.appearance_stat.appearance_id','over_under.appearance_stat.stat','over_under.title']]
                    
                    prop = pd.merge(dog,main[['id','player_id']],how='left',left_on='over_under.appearance_stat.appearance_id', right_on='id')
                    
                    prop = pd.merge(prop,players[['id','name','sport_id']],how='left',left_on='player_id',right_on = 'id')
                    prop.columns
                    underdogprops =  prop[['name','sport_id','stat_value','over_under.appearance_stat.stat']]
                    udprops = pd.pivot_table(underdogprops,values = 'stat_value',index=['name','sport_id'],columns='over_under.appearance_stat.stat',aggfunc = sum)
                    
                    wnbaund = udprops.reset_index()
                    wnbaund = wnbaund[wnbaund['sport_id'] =='WNBA']
                    wnbaund['three_points_made'] = wnbaund.get('three_points_made', float('NaN'))
                    wnbaund['free_throws_made'] = wnbaund.get('free_throws_made', float('NaN'))
                    wnbaund = wnbaund.replace(roto_replace)
                    wnbaund.to_csv('test.csv')
            except:
                    pass
            
            rotoinjuries = 'https://www.rotowire.com/wnba/tables/injury-report.php?team=ALL&pos=ALL'
            
            rotoinj = requests.get(rotoinjuries).json()
            
            rotoinj = pd.DataFrame(rotoinj)
            
            # rotoinj.loc[rotoinj.player=='Allisha Gray', 'status'] = 'OUT'
            # rotoinj.append(addplayer,ignore_index =True)
            # rotoinj.loc[rotoinj.player=='Lexie Hull', 'status'] = 'OUT'
            # rotoinj.loc[rotoinj.player=='Kristy Wallace', 'status'] = 'OUT'
            # rotoinj.loc[rotoinj.player=='Sophie Cunningham', 'status'] = 'OUT'
            # rotoinj.loc[rotoinj.player=='AD Durr', 'status'] = 'OUT'
            # rotoinj.loc[rotoinj.player=='Candace Parker', 'status'] = 'OUT'
            # rotoinj.loc[rotoinj.player=='Alanna Smith', 'status'] = 'OUT'
            status_replace = {'Out For Season':'OUT'}
            rotoinj = rotoinj.replace(status_replace)
            # addplayer = {'player' : 'Brittney Sykes'}
            rotoinj = rotoinj[rotoinj['status'] == 'OUT']
            rotoinj = rotoinj[['player','team' ]]
            
            injuredteams = list(rotoinj.team.unique())
            
            
            
            rotoreq = requests.get(rotowire).json()
            
            roto = pd.DataFrame(rotoreq)
            roto['player'] = roto['firstName'] + ' ' + roto['lastName']
            teamsets = pd.json_normalize(roto['team'])
            
            rotolist = pd.merge(roto, teamsets,how='left',left_index = True , right_index = True)
            minutes = rotolist[['player','abbr','minutes']]
            minutes['minutes'] = minutes['minutes'].astype(float)
            
            minutes.columns = ['Name','TeamAbbrev','min']
            
            roto_replace ={'Dorka Juhász':'Dorka Juhasz','Asia (AD) Durr':'Asia Durr','AD Durr': 'Asia Durr',"Azura Stevens":"Azurá Stevens","AzurÃƒÂ¡ Stevens":"Azurá Stevens", "AzurÃ¡ Stevens":"Azurá Stevens","Marine Johannès":"Marine Johannes","Li Meng":"Meng Li","Amanda Zahui B":'Amanda Zahui B.'}
            
            minutes = minutes.replace(roto_replace)
            minutes.to_csv('mincheck.csv')

            players = 'https://sheetdb.io/api/v1/21hb3awidwbyi'
            dksalaries = pd.read_json(players)
            minutes = dksalaries[['Name','TeamAbbrev']]
            minutes.to_csv('mincheck.csv')
            
            teams_response = requests.get("https://api.pbpstats.com/get-teams/wnba")
            teams = teams_response.json()
            all_teams = teams['teams']
            all_teams[0]['id']
            team_list = []
            for i in range(len(all_teams)):
                    team_id = all_teams[i]['id']
                    team_list.append(team_id)
                
            
            wowy_url = "https://api.pbpstats.com/get-wowy-stats/wnba"
            lineups =[]
            for i in range(len(team_list)):
                    teamid = team_list[i]   
                    wowy_params = {
                        "TeamId": f"{teamid}", # Seattle
                        "Season": "2023",
                        "SeasonType": "Regular Season",
                        "Type": "Player" # Team stats
                    }
                    wowy_response = requests.get(wowy_url, params=wowy_params)
                    wowy = wowy_response.json()
                    team_stats = wowy["single_row_table_data"]
                    lineup_stats = wowy["multi_row_table_data"]
                    lineups.append(lineup_stats)
            
            frame = []
            for i in range(len(lineups)):
                    player1 = pd.json_normalize(lineups[i])
                    frame.append(player1)
                
            baseline= pd.concat(frame).fillna(0)
            
            baseline.to_csv('players.csv')
            
            baseline_rates = baseline[['TeamId','Name','TeamAbbreviation','Minutes','Usage','Assists','Rebounds']]
            team_totals = baseline_rates.groupby(['TeamAbbreviation']).sum(['Assists','Rebounds']).reset_index()
            baseline_rates = pd.merge(baseline_rates,team_totals[['TeamAbbreviation','Assists','Rebounds']],on='TeamAbbreviation',how='left')
            baseline_rates['Ast%'] = baseline_rates['Assists_x'] / baseline_rates['Assists_y']
            baseline_rates['Reb%'] = baseline_rates['Rebounds_x'] / baseline_rates['Rebounds_y']
            baseline_rates['APM'] = baseline_rates['Assists_x'] / baseline_rates['Minutes']
            baseline_rates['RPM'] = baseline_rates['Rebounds_x'] / baseline_rates['Minutes']
            
            
            baseline_rates = baseline_rates[['TeamId','Name','TeamAbbreviation','Minutes','Usage','Ast%','Reb%','APM','RPM']]
            
            
            wowy_url = "https://api.pbpstats.com/get-wowy-stats/wnba"
            
            injuries = list(rotoinj.player.unique())
            
        
            check = injuries[0]
        
            teamdicts = []
            for team in injuredteams:
                    players = rotoinj[rotoinj['team'] == team]
                    key = team
                    value = list(players['player'].unique())
                    playerinj =[]
                    for i in range(len(value)):
                        try:
                            player = value[i]
                            player_id = baseline.loc[baseline['Name'] == player , 'Name'].values[0]
                            playerinj.append(player_id)
                        except:
                            pass
                    injdict = {key:playerinj}
                    teamdicts.append(injdict)
            

            teamsinju = list(dksalaries.TeamAbbrev.unique())
            
            
            def wowyadjustment(team, injuredplayers):
                    edits = []
                    currentteam = team
                    print(currentteam)
                    total = len(injuredplayers)
                    teamcheck = baseline[baseline['TeamId'] == currentteam]
                    
                    teamplayers = teamcheck['Name'].tolist()
                    for i in range(len(injuredplayers)):
                        pl = injuredplayers[i]
                        pl_id = baseline.loc[baseline['Name'] == pl , 'EntityId'].values[0]
                        edits.append(pl_id)
                    inj = ",".join(edits)
                    wowy_url2 =f"https://api.pbpstats.com/get-wowy-stats/wnba?0Exactly{total}OffFloor={inj}&Season=2023&SeasonType=Regular%20Season&TeamId={currentteam}&Type=Player"    
                    wowy_response = requests.get(wowy_url2)
                    wowy = wowy_response.json()
                    team_stats = wowy["single_row_table_data"]
                    lineup_stats = wowy["multi_row_table_data"]
                    lineups.append(lineup_stats)
            
            lineups = []
            
            teamiddict = []
            for i in range(len(all_teams)):
                    teamn = all_teams[i]
                    teamiddict.append(teamn)
            
            for team in teamsinju:
                    for element in teamiddict:
                        # print(element)
                        if element['text'] == team:
                            teamid = element['id']
                            print(teamid)
                    try:
                        injuredplayers = [ele for ele in teamdicts if team in ele][0]
                        print(injuredplayers[team])
                        wowyadjustment(teamid, injuredplayers[team])
                    except:
                        pass
            
            frame = []
            for i in range(len(lineups)):
                    player = pd.json_normalize(lineups[i])
                    frame.append(player)
                
            
            
            try:
                    adjusted= pd.concat(frame).fillna(0)
                    adjusted.to_csv('adj players.csv')
                    adjusted_rates = adjusted[['TeamId','Name','TeamAbbreviation','Minutes','Usage','Assists','Rebounds','Points']]
                    team_totals = adjusted_rates.groupby(['TeamAbbreviation']).sum(['Assists','Rebounds','Points']).reset_index()
                    adjusted_rates = pd.merge(adjusted_rates,team_totals[['TeamAbbreviation','Assists','Rebounds','Points']],on='TeamAbbreviation',how='left')
                    adjusted_rates['APM'] = adjusted_rates['Assists_x'] / adjusted_rates['Minutes']
                    adjusted_rates['RPM'] = adjusted_rates['Rebounds_x'] / adjusted_rates['Minutes']
                    adjusted_rates['PPM'] = adjusted_rates['Points_x'] / adjusted_rates['Minutes']
                
                
                
                    adjusted_rates['Ast%'] = adjusted_rates['Assists_x'] / adjusted_rates['Assists_y']
                    adjusted_rates['Reb%'] = adjusted_rates['Rebounds_x'] / adjusted_rates['Rebounds_y']
                
                    adjusted_rates = adjusted_rates[['TeamId','Name','TeamAbbreviation','Minutes','Usage','APM','RPM','PPM']]
                    baseline_rates = baseline_rates.sort_values(by ='Minutes',ascending=False)
                    baseline_rates = baseline_rates.drop_duplicates(subset ='Name')
                    adjusted_rates = adjusted_rates.sort_values(by ='Minutes',ascending=False)
                    adjusted_rates = adjusted_rates.drop_duplicates(subset ='Name')
                
                    new_rates = pd.merge(adjusted_rates,baseline_rates[['Name','Usage','APM','RPM']],how='left',on='Name')
                
                    new_rates['usage_boost'] = new_rates['Usage_x'] / new_rates['Usage_y']
                    new_rates['ast_boost'] = new_rates['APM_x'] / new_rates['APM_y']
                    new_rates['reb_boost'] = new_rates['RPM_x'] / new_rates['RPM_y']
                    new_rates = new_rates.fillna(1)
                
                    rate_boosts = new_rates[['Name','usage_boost','ast_boost','reb_boost']]
                    rate_boosts['usage_boost'] = np.where(rate_boosts['usage_boost'] > 1.5, 1.5, rate_boosts['usage_boost'] )
                    rate_boosts['usage_boost'] = np.where(rate_boosts['usage_boost'] < .5, .5, rate_boosts['usage_boost'] )
                    rate_boosts['ast_boost'] = np.where(rate_boosts['ast_boost'] > 1.5, 1.5, rate_boosts['ast_boost'] )
                    rate_boosts['ast_boost'] = np.where(rate_boosts['ast_boost'] < .5, .5, rate_boosts['ast_boost'] )
                    rate_boosts['reb_boost'] = np.where(rate_boosts['reb_boost'] > 1.5, 1.5, rate_boosts['reb_boost'] )
                    rate_boosts['reb_boost'] = np.where(rate_boosts['reb_boost'] < .5, .5, rate_boosts['reb_boost'] )
                    rate_boosts.to_csv('boosts.csv')
            except :
                    adjusted_rates = baseline_rates
                    rates = adjusted_rates[['Name']]
                    rates['usage_boost'],rates['ast_boost'],rates['reb_boost'] = [1,1,1]
                    rate_boosts = rates.copy()
                    rate_boosts.to_csv('boosts.csv')
        
            
            abb = {'Seattle Storm':'SEA','Minnesota Lynx':'MIN','Chicago Sky':'CHI','Atlanta Dream':'ATL','Las Vegas Aces':'LVA','Connecticut Sun':'CON','Los Angeles Sparks':'LAS','Washington Mystics':'WAS'}
            
            
            
            
            def call_endpoint(url, max_level=3, include_new_player_attributes=False):
                    '''
                    takes: 
                        - url (str): the API endpoint to call
                        - max_level (int): level of json normalizing to apply
                        - include_player_attributes (bool): whether to include player object attributes in the returned dataframe
                    returns:
                        - df (pd.DataFrame): a dataframe of the call response content
                    '''
                    resp = requests.get(url).json()
                    data = pd.json_normalize(resp['data'], max_level=max_level)
                    included = pd.json_normalize(resp['included'], max_level=max_level)
                    if include_new_player_attributes:
                        inc_cop = included[included['type'] == 'new_player'].copy().dropna(axis=1)
                        data = pd.merge(data, inc_cop, how='left', left_on=['relationships.new_player.data.id','relationships.new_player.data.type'], right_on=['id','type'], suffixes=('', '_new_player'))
                    return data
            try:
                    proj_prize  = 'https://partner-api.prizepicks.com/projections?league_id=3&per_page=1000&single_stat=true'
                    fb_projection = call_endpoint(proj_prize, include_new_player_attributes=True)
                    fb_projection = fb_projection[['attributes.name','attributes.team','attributes.position','attributes.stat_type','attributes.line_score']]
                    fb_projection['attributes.line_score'] = fb_projection['attributes.line_score'].astype(float)   
                    cf_pivot= fb_projection.pivot_table(values = 'attributes.line_score',index=['attributes.name','attributes.team'],columns='attributes.stat_type').reset_index()
                    cf_pivot['3-PT Made'] = cf_pivot.get('3-PT Made', float('NaN')) 
            except: 
                    pass
            

            players = 'https://sheetdb.io/api/v1/21hb3awidwbyi'
            dksalaries = pd.read_json(players)
            
            
            
            
            
            slate = [today['v.ta'] , today['h.ta']]
            
            main = []
            for i in range(len(today['v.ta'])):
                    home = slate[0][i]
                    away = slate[1][i]
                    gameid = [home,away]
                    main.append(gameid)
        
            teams =pd.DataFrame(main)
            
            teams.columns = ['Home_abb','Away_abb']
            
            team_list = data_pull('wnba_team list.csv')
            
            
            res = requests.get(url = team_stats_url, headers=headers).json()
            
            historical_logs = []
            # for year in season_id:    
            #         print(year)
            #         game_log_url ='https://stats.wnba.com/stats/playergamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season='  + str(year) +'&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision='                    
            #         response = requests.get(url = game_log_url, headers=headers).json()
            #         columns_list = response['resultSets'][0]['headers']
            #         player_info = response['resultSets'][0]['rowSet']    
            #         wnba_game_log = pd.DataFrame(player_info,columns = columns_list)    
            #         wnba_game_log.columns = wnba_game_log.columns.str.lower()
            #         historical_logs.append(wnba_game_log)
                    
            for year in season_id:   
                    game_log_url ='https://stats.wnba.com/stats/playergamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season='  + str(year) +'&SeasonSegment=&SeasonType=Pre+Season&ShotClockRange=&VsConference=&VsDivision='    
                    response = requests.get(url = game_log_url, headers=headers).json()
                    columns_list = response['resultSets'][0]['headers']
                    player_info = response['resultSets'][0]['rowSet']    
                    wnba_game_log = pd.DataFrame(player_info,columns = columns_list)    
                    wnba_game_log.columns = wnba_game_log.columns.str.lower()
                    historical_logs.append(wnba_game_log) 
                
            wnba_game_log = pd.concat(historical_logs)
            
            wnba_game_log['GameKey'] = wnba_game_log.game_date.astype(str) + '-' + wnba_game_log.team_abbreviation.astype(str)
            gamekey = wnba_game_log[['GameKey','game_date','team_abbreviation']]
            gamekey = gamekey.drop_duplicates(subset=['GameKey'])
            gamekey = gamekey.sort_values(by=['team_abbreviation','game_date'], ascending = (True,False))
            gamekey['gamenumber'] = gamekey.groupby('team_abbreviation').cumcount() +1
            
            wnba_mins = pd.merge(wnba_game_log,gamekey[['GameKey','gamenumber']],how='left',on='GameKey')
            
            last_3 = wnba_mins[wnba_mins['gamenumber'] <=3].fillna(0)
            last_3.to_csv('last3.csv')
            min_avg = last_3[['player_name','min']].groupby('player_name').mean('min').reset_index()
            
            min_start = pd.merge(dksalaries,min_avg, how='left',left_on='Name', right_on ='player_name').fillna(0)
            min_start.to_csv('wnbaminstart.csv',index=False)
            
            if minutesprojected == "Y":
                    minutes = data_pull('mincheckproj.csv')
            else:
                    maxmin = last_3[['player_name','team_abbreviation','min']].groupby(['player_name','team_abbreviation']).max('min')
                    avgmin = last_3[['player_name','team_abbreviation','min']].groupby(['player_name','team_abbreviation']).mean('min')
                    minp = pd.merge(maxmin,avgmin,how='left',left_index=True,right_index =True).reset_index()
                    minp['minproj'] = (minp['min_x'] + minp['min_y']) /2
                    minutes = minp[['player_name','team_abbreviation','minproj']]
                    minutes.columns = ['Name','TeamAbbrev','min']
                    minutes = minutes.replace(roto_replace)
                
            min_start = pd.merge(min_start,minutes, how='left', on='Name')
            
            min_start = min_start[['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info','TeamAbbrev_x', 'AvgPointsPerGame',  'min_x', 'min_y']]
            
            min_start.columns = ['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info','TeamAbbrev', 'AvgPointsPerGame',  'min_x', 'min_y']
            
            
            adv = 'https://stats.wnba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2023&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='
            
            reso = requests.get(url = adv, headers=headers).json()
            check = reso['resultSets'][0]['headers']
            advdata = reso['resultSets'][0]['rowSet']
            advdef = pd.DataFrame(advdata,columns = check)
            advdef = advdef[['TEAM_ID','TEAM_NAME','PACE']]
            
            playerids = wnba_game_log['player_id'].unique().tolist()
            
            
            
            player_position_table = data_pull('playerposition.csv')
            
            
            pgconn = psycopg2.connect(host='postgres-cbb.c4h8ukw5kz5z.us-east-1.rds.amazonaws.com',database = 'postgres',user='postgres',password='Tw236565!!')
            
            pgconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            pgcursor = pgconn.cursor()
            
            pgcursor.execute("SET search_path TO wnba;")
            
            engine = create_engine('postgresql+psycopg2://postgres:Tw236565!!@postgres-cbb.c4h8ukw5kz5z.us-east-1.rds.amazonaws.com/postgres')

            
            # pgconn = psycopg2.connect(host='localhost',database = 'WNBA',user='postgres',password='Tw236565!!')
            
            # pgconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            # pgcursor = pgconn.cursor()
            
            # engine = create_engine('postgresql+psycopg2://postgres:Tw236565!!@localhost/WNBA')
            
            pgcursor.execute( """ DROP TABLE totalgamelogs ;DROP TABLE "PLAYER_GAME_LOGS" ; DROP TABLE "Games";DROP TABLE draftkings
                            ;DROP TABLE playerlast_10; DROP TABLE playerpermin; DROP TABLE playerstdpermin;
                            DROP TABLE teamstats; DROP TABLE statsallowedpermin; DROP TABLE defpermin
                            ; DROP TABLE wowy """)
            # DROP TABLE prizepicks;DROP TABLE "TEAMS";
            
            wnba_game_log.to_sql(name ='PLAYER_GAME_LOGS', schema = 'wnba',con = engine, if_exists='replace',index = False)
            # wnba_game_log.to_sql(name ='HISTORICAL_PLAYER_GAME_LOGS', schema = 'wnba',con = engine, if_exists='replace',index = False)
            team_list.to_sql(name ='TEAMS', schema = 'wnba', con =engine, if_exists='replace',index = False)
            player_position_table.to_sql(name ='PLAYERS', schema = 'wnba',con =engine, if_exists='replace',index = False)
            teams.to_sql(name ='Games',schema = 'wnba',con = engine, if_exists='replace',index = False)
            rate_boosts.to_sql(name ='wowy', schema = 'wnba',con =engine, if_exists = 'replace',index=False)
            advdef.to_sql(name ='pace', schema = 'wnba',con =engine, if_exists = 'replace',index=False)
            try:
                    wnbaund.to_sql(name ='underdog',schema = 'wnba', con = engine,if_exists = 'replace', index=False)
            except:
                    pass
            # min_avg.to_sql('3gameminavg', engine, if_exists='replace',index = False)
            
            min_start.to_sql(name ='draftkings',schema = 'wnba',con =   engine, if_exists='replace',index = False)
            try:
                    cf_pivot.to_sql(name ='prizepicks',schema = 'wnba', con = engine, if_exists = 'replace' , index =False)
            except:
                    pass
            minutes.to_sql(name ='projmins' ,schema = 'wnba', con = engine, if_exists = 'replace' , index=False)
            
            
            pgcursor.execute("""
            --update
            update wnba.draftkings set "TeamAbbrev" = replace( "TeamAbbrev", 'LAV','LVA')  ;               
                            
            --combine gamelogs	
            create table wnba.totalgamelogs as
            select * from wnba."PLAYER_GAME_LOGS" pgl 
            union
            select * from wnba."HISTORICAL_PLAYER_GAME_LOGS" hpgl  ;
            
            
            update wnba.totalgamelogs set player_name = replace(player_name, 'Amanda Zahui B', 'Amanda Zahui B.');
            update wnba.totalgamelogs set player_name = replace(player_name, 'Ivana Dojkić', 'Ivana Dojkic');
            update wnba.totalgamelogs set player_name = replace(player_name, 'Dorka Juhász', 'Dorka Juhasz');
            update wnba.totalgamelogs set player_name = replace(player_name, 'Marine Johannès', 'Marine Johannes');
            update wnba.totalgamelogs set player_name = replace(player_name, 'Asia (AD) Durr', 'Asia Durr');
            update wnba.totalgamelogs set player_name = replace(player_name, 'Sika Koné', 'Sika Kone');
            update wnba.totalgamelogs set player_name = replace(player_name, 'Li Meng', 'Meng Li');
            update wnba."PLAYERS" set "Player" = replace("Player", 'Dorka Juhász', 'Dorka Juhasz');
            update wnba."PLAYERS" set "Player" = replace("Player", 'Marine Johannès', 'Marine Johannes');
            update wnba."PLAYERS" set "Player" = replace("Player", 'Asia (AD) Durr', 'Asia Durr');
            update wnba."PLAYERS" set "Player"= replace("Player", 'Sika Koné', 'Sika Kone');
            update wnba."PLAYERS" set "Player"= replace("Player", 'Li Meng', 'Meng Li');
            update wnba."PLAYERS" set "Player" = replace("Player", 'Ivana Dojkić', 'Ivana Dojkic');
            update wnba."PLAYERS" set "Player" = replace("Player", 'Amanda Zahui B', 'Amanda Zahui B.');
            
                            
            --player last 10
            create table  wnba.playerlast_10 as
            select gl.player_name, playerposition , team_abbreviation, game_date, sum(fga) as totalfga, sum(fg3m) as totalfg3m , sum(ftm) as totalftm , sum(oreb) as totaloreb , sum(dreb) as totaldreb, sum(ast) as totalast , sum(stl) as totalstl , sum(blk) as totalblk , sum(pfd) as totalpfd, sum(pts) as totalpts , sum("min") as  totalmins
            ,(sum(pts) + (1.5* sum(ast)) + (1.2 * (sum(oreb) + sum(dreb))) +(2 * (sum(stl) + sum(blk))) + (.5*sum(fg3m))) as dkfant
            ,row_number() over (partition by gl.player_name , playerposition order by game_date desc )  game_number
            from wnba.totalgamelogs gl
            left join (select pl."Player",case when split_part(pl."Position",'-',1) = '' then pl."Position" 
            when split_part(pl."Position",'-',1) = 'C2K' then 'Center' else split_part(pl."Position",'-',1) end as playerposition
            from wnba."PLAYERS" pl) play on gl.player_name = play."Player"
            where gl.game_date > '2023-01-01'
            group by gl.player_name,playerposition, team_abbreviation ,gl.game_date;
            
            
            
            --player per min
            create table wnba.playerpermin as 
            select pla.player_name
            , pla.playerposition
            ,sum(totalpts) / sum(totalmins) as ppm 
            , sum(totalfga) / sum(totalmins) as fgapm 
            , sum(totalfg3m) /sum(totalmins)  as threespm
            , sum(totalftm) /sum(totalmins)  as ftpm
            , (sum(totaloreb) + sum(totaldreb)) / sum(totalmins) as rpm
            , sum(totalast)/ sum(totalmins)  as apm
            , sum(totalstl)/sum(totalmins)  as spm
            ,sum(totalblk) /sum(totalmins)  as bpm
            , sum(totalpfd) /sum(totalmins)  as pfdpm
            , avg(totalmins) as minutes
            from wnba.playerlast_10 pla
            where pla.game_number <=10
            group by pla.player_name, pla.playerposition
            order by pla.player_name;
            
            --player stddev
            create table wnba.playerstdpermin as
            select pla.player_name
            , pla.playerposition
            , coalesce(stddev_pop(totalpts/nullif(totalmins,0)),0)  as ppmstd
            , coalesce(stddev_pop(totalfga/nullif(totalmins,0)),0)  as fgapmstd
            , coalesce(stddev_pop(totalfg3m/nullif(totalmins,0)),0) as threespmstd
            , coalesce(stddev_pop(totalftm/nullif(totalmins,0)),0) as ftpmstd
            , coalesce(stddev_pop((totaloreb+totaldreb)/nullif(totalmins,0)),0) as rpmstd
            , coalesce(stddev_pop(totalast/nullif(totalmins,0)),0)as apmstd
            , coalesce(stddev_pop(totalstl/nullif(totalmins,0)),0)  as spmstd
            ,coalesce(stddev_pop(totalblk/nullif(totalmins,0)),0) as bpmstd
            , coalesce(stddev_pop(totalpfd/nullif(totalmins,0)),0)   as pfdpmstd
            , coalesce(stddev_pop(nullif(totalmins,0)),0) as minutesstd
            , coalesce(stddev_pop(dkfant),0) as dkstd 
            from wnba.playerlast_10 pla
            group by pla.player_name, pla.playerposition;
            
            -- create team sttats
            create  table wnba.teamstats as
            select case when split_part(gl.matchup,'@',2) = '' then split_part(gl.matchup,'vs.',2) else  split_part(gl.matchup,'@',2) end as opponent ,playerposition , game_date, sum(fga) as totalfga, sum(fg3a) as totalfg3a , sum(fta) as totalfta , sum(oreb) as totaloreb , sum(dreb) as totaldreb, sum(ast) as totalast , sum(stl) as totalstl , sum(blk) as totalblk , sum(pfd) as totalpfd, sum(pts) as totalpts , sum("min") as  totalmins
            , row_number() over (partition by case when split_part(gl.matchup,'@',2) = '' then split_part(gl.matchup,'vs.',2) else  split_part(gl.matchup,'@',2) end , playerposition order by game_date desc )  game_number
            from wnba."PLAYER_GAME_LOGS" gl
            left join (select pl."Player",case when split_part(pl."Position",'-',1) = '' then pl."Position" 
            when split_part(pl."Position",'-',1) = 'C2K' then 'Center' else split_part(pl."Position",'-',1) end as playerposition
            from wnba."PLAYERS" pl) play on gl.player_name = play."Player"
            group by opponent, playerposition ,game_date
            having playerposition <> '' ;
            
            
            
            create  table wnba.statsallowedpermin as
            select ts.opponent
            , ts.playerposition as positionone
            , sum(totalfga) / sum(totalmins) as fgapm 
            , sum(totalpts) / sum(totalmins) as ppm 
            , sum(totalfg3a) /sum(totalmins)  as threespm
            , sum(totalfta) /sum(totalmins)  as ftpm
            , (sum(totaloreb) + sum(totaldreb)) / sum(totalmins) as rpm
            , sum(totalast)/ sum(totalmins)  as apm
            , sum(totalstl)/sum(totalmins)  as spm
            ,sum(totalblk) /sum(totalmins)  as bpm
            , sum(totalpfd) /sum(totalmins)  as pfdpm
            from wnba.teamstats ts
            where ts.game_number <=10
            group by ts.opponent, ts.playerposition
            order by ts.opponent;
            
            
            create table wnba.defpermin as 
            select ts.opponent
            , lea.positionone
            , (sum(totalfga) / sum(totalmins)) /avg(lea.league_fga) as fgapmdef 
            , sum(totalfg3a) /sum(totalmins) /avg(lea.league_three) as threespmdef
            , sum(totalfta) /sum(totalmins) / avg(lea.leaguefree) as ftpmdef
            , (sum(totaloreb) + sum(totaldreb)) / sum(totalmins) / avg(lea.leaguereb) as rpmdef
            , sum(totalast)/ sum(totalmins) / avg(lea.leagueast) as apmdef
            , sum(totalstl)/sum(totalmins) / avg(lea.leaguestl) as spmdef
            ,sum(totalblk) /sum(totalmins)/ avg(lea.leagueblk)  as bpmdef
            , sum(totalpfd) /sum(totalmins) / avg(lea.leaguepfd) as pfdpmdef
            , sum(totalpts) /sum(totalmins) / avg(lea.leagueppm) as ppmdef
            from wnba.teamstats ts
            left join(select sa.positionone,avg(ppm) as leagueppm, avg(fgapm) as league_fga , avg(threespm) as league_three ,avg(ftpm) as leaguefree , avg(rpm) as leaguereb , avg(apm) as leagueast , avg(spm) as leaguestl , avg(bpm) as leagueblk, avg(pfdpm)  as leaguepfd
            from wnba.statsallowedpermin  sa
            group by sa.positionone) lea on ts.playerposition = lea.positionone
            where ts.game_number <=10
            group by ts.opponent, lea.positionone
            order by ts.opponent;""")
            
            
            
            pgcursor.execute(""" 
            
            select player_name,"TeamAbbrev",percentile_cont(0.5) within group (order by min_proj)  as minutes_projection
            , percentile_cont(0.5) within group (order by ppmsim)  as medianppm
            , percentile_cont(0.5) within group (order by rebsim)  as medianreb
            , percentile_cont(0.5) within group (order by apmsim)  as medianast
            , percentile_cont(0.5) within group (order by bpmsim)  as medianblk
            , percentile_cont(0.5) within group (order by spmsim)  as medianstl
            , percentile_cont(0.5) within group (order by threessim)  as medianthrees
            , percentile_cont(0.5) within group (order by ftsim)  as medianft
            , (avg(ppmsim) + (.5*avg(threessim) ) + (1.25 * avg(rebsim) ) + (1.5*avg(apmsim) ) + (2 * (avg(bpmsim) + avg(spmsim) ))) as dkpts
            from (select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * fgapmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as fgasim
            , ((threespm * threespmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * threespmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as threessim
            , ((ftpm * ftpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ftpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ftsim
            , ((bpm * bpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * bpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as bpmsim
            , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * rpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as rebsim
            , ((apm * apmdef * homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * apmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as apmsim
            , ((spm * spmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * spmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as spmsim
            , ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ppmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ppmsim
            from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
            , case when dk."TeamAbbrev"= gm."Home_abb" then gm."Away_abb" 
                when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                end as opponent
                ,usage_boost, reb_boost,ast_boost
            from playerpermin pl
            left join wowy w on pl.player_name = w."Name"
            left join (select * from playerstdpermin) std on pl.player_name = std.player_name
            left join draftkings dk on pl.player_name = dk."Name"
            left join (select * from "Games") gm on dk."TeamAbbrev" = gm."Home_abb"
            left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
            where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                end is not null
            ) mai
            left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
            left join (select g."Home_abb"
            ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
            from "Games" g 
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pac on pac.team_abbreviation = g."Home_abb"
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pace on pace.team_abbreviation = g."Away_abb"
            union all
            select g."Away_abb"
            , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
            from "Games" g 
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pac on pac.team_abbreviation = g."Home_abb"
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
            cross join (select i 
            from generate_series(1,(6800)) i) seq
            union all 
            select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * fgapmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as fgasim
            , ((threespm * threespmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * threespmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as threessim
            , ((ftpm * ftpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ftpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ftsim
            , ((bpm * bpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * bpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as bpmsim
            , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * rpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as rebsim
            , ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * apmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as apmsim
            , ((spm * spmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * spmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as spmsim
            , ((mai.ppm * ppmdef* homepacefactor *coalesce(usage_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ppmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ppmsim
            from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
            , case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                end as opponent
                ,usage_boost, reb_boost,ast_boost
            from playerpermin pl
            left join wowy w on pl.player_name = w."Name"
            left join (select * from playerstdpermin) std on pl.player_name = std.player_name
            left join draftkings dk on pl.player_name = dk."Name"
            left join (select * from "Games") gm on dk."TeamAbbrev"= gm."Home_abb"
            left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
            where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                end is not null) mai
            left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
            left join (select g."Home_abb"
            ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
            from "Games" g 
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pac on pac.team_abbreviation = g."Home_abb"
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pace on pace.team_abbreviation = g."Away_abb"
            union all
            select g."Away_abb"
            , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
            from "Games" g 
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pac on pac.team_abbreviation = g."Home_abb"
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
            cross join (select i 
            from generate_series(1,(2700)) i) seq
            union all
            select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * fgapmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as fgasim
            , ((threespm * threespmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * threespmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as threessim
            , ((ftpm * ftpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ftpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ftsim
            , ((bpm * bpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * bpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as bpmsim
            , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1) ) + ((floor(random() * (300-(-300)+1) -300 ))/100 * rpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as rebsim
            , ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * apmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as apmsim
            , ((spm * spmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * spmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as spmsim
            , ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ppmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ppmsim
            from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
            , case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                end as opponent
                ,usage_boost, reb_boost,ast_boost
            from playerpermin pl
            left join wowy w on pl.player_name = w."Name"
            left join (select * from playerstdpermin) std on pl.player_name = std.player_name
            left join draftkings dk on pl.player_name = dk."Name"
            left join (select * from "Games") gm on dk."TeamAbbrev" = gm."Home_abb"
            left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
            where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                end is not null) mai
            left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
            left join (select g."Home_abb"
            ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
            from "Games" g 
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pac on pac.team_abbreviation = g."Home_abb"
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pace on pace.team_abbreviation = g."Away_abb"
            union all
            select g."Away_abb"
            , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
            from "Games" g 
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pac on pac.team_abbreviation = g."Home_abb"
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
            cross join (select i 
            from generate_series(1,(500	)) i) seq
            order by player_name) tot
            group by player_name,"TeamAbbrev";
            """)
            
            medians = pgcursor.fetchall()
            cols = []
            for thing in pgcursor.description:
                    cols.append(thing[0])
            
            
            try:
                    pgcursor.execute("""select player_name
                        ,"Points"
                        , sum(case 
                            when "Points" is null then null
                            when ppmsim  > "Points" and "Points" is not null then 1 
                            else 0
                        end)/10000 ::float as pointsover
                        ,"Pts+Rebs"
                        , sum(case 
                            when "Pts+Rebs" is null then null
                            when ppmsim + rebsim > "Pts+Rebs" and "Pts+Rebs" is not null then 1 
                            else 0
                        end ) /10000 ::float as prover
                        ,"Pts+Asts"
                        ,sum(case
                            when "Pts+Asts" is null then null
                            when ppmsim +apmsim > "Pts+Asts" and "Pts+Asts" is not null then 1 
                            else 0
                        end)/10000 ::float  as paover
                        ,"Pts+Rebs+Asts"
                        ,sum(case
                            when "Pts+Rebs+Asts" is null then null
                            when ppmsim +apmsim +rebsim > "Pts+Rebs+Asts" and "Pts+Rebs+Asts" is not null then 1 
                            else 0
                        end)/10000 ::float as parover
                        ,"Rebounds"
                        ,sum(case
                            when "Rebounds" is null then null
                            when rebsim > "Rebounds" and "Rebounds" is not null then 1 
                            else 0
                        end)/10000 ::float as rebover
                        ,"Assists"
                        ,sum(case
                            when "Assists" is null then null
                            when apmsim > "Assists" and "Assists" is not null then 1 
                            else 0
                        end)/10000 ::float as astover
                        ,"Rebs+Asts"
                        ,sum(case
                            when "Rebs+Asts" is null then null
                            when rebsim +apmsim > "Rebs+Asts" and "Rebs+Asts" is not null then 1 
                            else 0
                        end)/10000 ::float as raover
                        ,"Fantasy Score"
                        ,sum(case
                            when "Fantasy Score" is null then null
                            when ppmsim  + (1.25 * rebsim ) + (1.5*apmsim ) + (3 * (bpmsim +  spmsim) ) > "Fantasy Score" and "Fantasy Score" is not null then 1 
                            else 0
                        end)/10000 ::float as fantover
                        ,"3-PT Made"
                        ,sum(case
                            when "3-PT Made" is null then null
                            when threessim > "3-PT Made" and "3-PT Made" is not null then 1 
                            else 0
                        end)/10000 ::float as threesover
                        from (select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * fgapmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as fgasim
                        , ((threespm * threespmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * threespmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as threessim
                        , ((ftpm * ftpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ftpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ftsim
                        , ((bpm * bpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * bpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as bpmsim
                        , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * rpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as rebsim
                        , ((apm * apmdef * homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * apmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as apmsim
                        , ((spm * spmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * spmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as spmsim
                        , ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ppmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ppmsim
                        from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
                        , case when dk."TeamAbbrev"= gm."Home_abb" then gm."Away_abb" 
                            when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                            end as opponent
                            ,usage_boost, reb_boost,ast_boost
                        from playerpermin pl
                        left join wowy w on pl.player_name = w."Name"
                        left join (select * from playerstdpermin) std on pl.player_name = std.player_name
                        left join draftkings dk on pl.player_name = dk."Name"
                        left join (select * from "Games") gm on dk."TeamAbbrev" = gm."Home_abb"
                        left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
                        where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                            when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                            end is not null
                        ) mai
                        left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
                        left join (select g."Home_abb"
                        ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
                        from "Games" g 
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pac on pac.team_abbreviation = g."Home_abb"
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pace on pace.team_abbreviation = g."Away_abb"
                        union all
                        select g."Away_abb"
                        , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
                        from "Games" g 
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pac on pac.team_abbreviation = g."Home_abb"
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
                        cross join (select i 
                        from generate_series(1,(6800)) i) seq
                        union all 
                        select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * fgapmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as fgasim
                        , ((threespm * threespmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * threespmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as threessim
                        , ((ftpm * ftpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ftpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ftsim
                        , ((bpm * bpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * bpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as bpmsim
                        , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * rpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as rebsim
                        , ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * apmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as apmsim
                        , ((spm * spmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * spmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as spmsim
                        , ((mai.ppm * ppmdef* homepacefactor *coalesce(usage_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ppmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ppmsim
                        from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
                        , case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                            when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                            end as opponent
                            ,usage_boost, reb_boost,ast_boost
                        from playerpermin pl
                        left join wowy w on pl.player_name = w."Name"
                        left join (select * from playerstdpermin) std on pl.player_name = std.player_name
                        left join draftkings dk on pl.player_name = dk."Name"
                        left join (select * from "Games") gm on dk."TeamAbbrev"= gm."Home_abb"
                        left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
                        where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                            when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                            end is not null) mai
                        left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
                        left join (select g."Home_abb"
                        ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
                        from "Games" g 
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pac on pac.team_abbreviation = g."Home_abb"
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pace on pace.team_abbreviation = g."Away_abb"
                        union all
                        select g."Away_abb"
                        , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
                        from "Games" g 
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pac on pac.team_abbreviation = g."Home_abb"
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
                        cross join (select i 
                        from generate_series(1,(2700)) i) seq
                        union all
                        select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * fgapmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as fgasim
                        , ((threespm * threespmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * threespmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as threessim
                        , ((ftpm * ftpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ftpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ftsim
                        , ((bpm * bpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * bpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as bpmsim
                        , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1) ) + ((floor(random() * (300-(-300)+1) -300 ))/100 * rpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as rebsim
                        , ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * apmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as apmsim
                        , ((spm * spmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * spmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as spmsim
                        , ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ppmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ppmsim
                        from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
                        , case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                            when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                            end as opponent
                            ,usage_boost, reb_boost,ast_boost
                        from playerpermin pl
                        left join wowy w on pl.player_name = w."Name"
                        left join (select * from playerstdpermin) std on pl.player_name = std.player_name
                        left join draftkings dk on pl.player_name = dk."Name"
                        left join (select * from "Games") gm on dk."TeamAbbrev" = gm."Home_abb"
                        left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
                        where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                            when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                            end is not null) mai
                        left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
                        left join (select g."Home_abb"
                        ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
                        from "Games" g 
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pac on pac.team_abbreviation = g."Home_abb"
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pace on pace.team_abbreviation = g."Away_abb"
                        union all
                        select g."Away_abb"
                        , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
                        from "Games" g 
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pac on pac.team_abbreviation = g."Home_abb"
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
                        cross join (select i 
                        from generate_series(1,(500	)) i) seq
                        order by player_name)sims
                        left join prizepicks pp on pp."attributes.name" = sims.player_name
                        where pp."attributes.name" is not null 
                        group by player_name,"Points","Pts+Rebs", "Pts+Asts", "Pts+Rebs+Asts","Rebs+Asts","Rebounds","Assists","Fantasy Score"
                        ,"3-PT Made"
                        order by player_name;""")
                
                
                    
                    overs = pgcursor.fetchall()
                    column = []
                    for thing in pgcursor.description:
                            column.append(thing[0])   
            
            except:
                    pass
            
            try:
                    pgcursor.execute("""select player_name
                    ,"points"
                    , sum(case 
                        when "points"::float is null then null
                        when ppmsim  > "points"::float  and "points" is not null then 1 
                        else 0
                    end)/10000 ::float as pointsover
                    ,"pts_rebs"
                    , sum(case 
                        when "pts_rebs" is null then null
                        when ppmsim + rebsim > "pts_rebs"::float and "pts_rebs" is not null then 1 
                        else 0
                    end ) /10000 ::float as prover
                    ,"pts_asts"
                    ,sum(case
                        when "pts_asts" is null then null
                        when ppmsim +apmsim > "pts_asts"::float and "pts_asts" is not null then 1 
                        else 0
                    end) /10000 ::float as paover
                    ,"pts_rebs_asts"
                    ,sum(case
                        when "pts_rebs_asts" is null then null
                        when ppmsim +apmsim +rebsim > "pts_rebs_asts"::float and "pts_rebs_asts" is not null then 1 
                        else 0
                    end)/10000 ::float as parover
                    ,"rebounds"
                    ,sum(case
                        when "rebounds" is null then null
                        when rebsim > "rebounds"::float and "rebounds" is not null then 1 
                        else 0
                    end) /10000 ::float as rebover
                    ,"assists"
                    ,sum(case
                        when "assists" is null then null
                        when apmsim > "assists"::float and "assists" is not null then 1 
                        else 0
                    end)/10000 ::float as astover
                    ,"rebs_asts"
                    ,sum(case
                        when "rebs_asts" is null then null
                        when rebsim +apmsim > "rebs_asts"::float and "rebs_asts" is not null then 1 
                        else 0
                    end)/10000 ::float as raover
                    ,"fantasy_points"
                    ,sum(case
                        when "fantasy_points" is null then null
                        when ppmsim  + (1.25 * rebsim ) + (1.5*apmsim ) + (2 * (bpmsim +  spmsim) ) > "fantasy_points"::float and "fantasy_points" is not null then 1 
                        else 0
                    end)/10000 ::float as fantover
                    ,"three_points_made"
                    ,sum(case
                        when "three_points_made" is null then null
                        when threessim > "three_points_made"::float and "three_points_made" is not null then 1 
                        else 0
                    end)/10000 ::float as threesover
                    from (select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * fgapmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as fgasim
                    , ((threespm * threespmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * threespmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as threessim
                    , ((ftpm * ftpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ftpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ftsim
                    , ((bpm * bpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * bpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as bpmsim
                    , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * rpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as rebsim
                    , ((apm * apmdef * homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * apmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as apmsim
                    , ((spm * spmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * spmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as spmsim
                    , ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ppmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ppmsim
                    from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
                    , case when dk."TeamAbbrev"= gm."Home_abb" then gm."Away_abb" 
                        when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                        end as opponent
                        ,usage_boost, reb_boost,ast_boost
                    from playerpermin pl
                    left join wowy w on pl.player_name = w."Name"
                    left join (select * from playerstdpermin) std on pl.player_name = std.player_name
                    left join draftkings dk on pl.player_name = dk."Name"
                    left join (select * from "Games") gm on dk."TeamAbbrev" = gm."Home_abb"
                    left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
                    where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                        when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                        end is not null
                    ) mai
                    left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
                    left join (select g."Home_abb"
                    ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
                    from "Games" g 
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pac on pac.team_abbreviation = g."Home_abb"
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pace on pace.team_abbreviation = g."Away_abb"
                    union all
                    select g."Away_abb"
                    , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
                    from "Games" g 
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pac on pac.team_abbreviation = g."Home_abb"
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
                    cross join (select i 
                    from generate_series(1,(6800)) i) seq
                    union all 
                    select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * fgapmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as fgasim
                    , ((threespm * threespmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * threespmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as threessim
                    , ((ftpm * ftpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ftpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ftsim
                    , ((bpm * bpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * bpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as bpmsim
                    , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * rpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as rebsim
                    , ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * apmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as apmsim
                    , ((spm * spmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * spmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as spmsim
                    , ((mai.ppm * ppmdef* homepacefactor *coalesce(usage_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ppmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ppmsim
                    from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
                    , case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                        when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                        end as opponent
                        ,usage_boost, reb_boost,ast_boost
                    from playerpermin pl
                    left join wowy w on pl.player_name = w."Name"
                    left join (select * from playerstdpermin) std on pl.player_name = std.player_name
                    left join draftkings dk on pl.player_name = dk."Name"
                    left join (select * from "Games") gm on dk."TeamAbbrev"= gm."Home_abb"
                    left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
                    where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                        when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                        end is not null) mai
                    left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
                    left join (select g."Home_abb"
                    ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
                    from "Games" g 
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pac on pac.team_abbreviation = g."Home_abb"
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pace on pace.team_abbreviation = g."Away_abb"
                    union all
                    select g."Away_abb"
                    , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
                    from "Games" g 
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pac on pac.team_abbreviation = g."Home_abb"
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
                    cross join (select i 
                    from generate_series(1,(2700)) i) seq
                    union all
                    select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * fgapmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as fgasim
                    , ((threespm * threespmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * threespmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as threessim
                    , ((ftpm * ftpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ftpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ftsim
                    , ((bpm * bpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * bpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as bpmsim
                    , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1) ) + ((floor(random() * (300-(-300)+1) -300 ))/100 * rpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as rebsim
                    , ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * apmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as apmsim
                    , ((spm * spmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * spmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as spmsim
                    , ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ppmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ppmsim
                    from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
                    , case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                        when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                        end as opponent
                        ,usage_boost, reb_boost,ast_boost
                    from playerpermin pl
                    left join wowy w on pl.player_name = w."Name"
                    left join (select * from playerstdpermin) std on pl.player_name = std.player_name
                    left join draftkings dk on pl.player_name = dk."Name"
                    left join (select * from "Games") gm on dk."TeamAbbrev" = gm."Home_abb"
                    left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
                    where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                        when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                        end is not null) mai
                    left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
                    left join (select g."Home_abb"
                    ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
                    from "Games" g 
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pac on pac.team_abbreviation = g."Home_abb"
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pace on pace.team_abbreviation = g."Away_abb"
                    union all
                    select g."Away_abb"
                    , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
                    from "Games" g 
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pac on pac.team_abbreviation = g."Home_abb"
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
                    cross join (select i 
                    from generate_series(1,(500	)) i) seq
                    order by player_name)sims
                    left join underdog ud on ud."name" = sims.player_name
                    where ud."name" is not null 
                    group by player_name,"points"
                    ,"pts_rebs","pts_asts"
                    ,"pts_rebs_asts"
                    ,"rebounds","assists"
                    ,"fantasy_points"
                    ,"rebs_asts"
                    ,"three_points_made"
                    order by player_name;""")
                
                
                    
                    udovers = pgcursor.fetchall()
                    udcolumn = []
                    for thing in pgcursor.description:
                            udcolumn.append(thing[0])   
            
            except:
                    pass
                
            try:
                    pgcursor.execute("""with overs as (select *
                    ,(pts + (1.2*reb)+(1.5*ast)+ (3*(stl+blk))- tov) as prizepicksfant
                    ,row_number() over (partition by t.player_name order by game_date desc )  game_number 
                    ,case
                        when p."3-PT Made" is null then 0
                        when p."3-PT Made" > fg3m then 0
                        when p."3-PT Made" < fg3m then 1
                    end as "3-PT Over"
                    ,case
                        when p."Assists" is null then 0
                        when p."Assists" > ast then 0
                        when p."Assists" < ast then 1
                    end as "Assists Over"
                    ,case
                        when p."Blks+Stls" is null then 0
                        when p."Blks+Stls" > (blk + stl) then 0
                        when p."Blks+Stls" < (blk + stl) then 1
                    end as "Blks+Stls Over"
                    ,case
                        when p."Fantasy Score" is null then 0
                        when p."Fantasy Score" > (pts + (1.2*reb)+(1.5*ast)+ (3*(stl+blk))- tov) then 0
                        when p."Fantasy Score" < (pts + (1.2*reb)+(1.5*ast)+ (3*(stl+blk))- tov) then 1
                    end as "Fantasy Score Over"
                    ,case
                        when p."Free Throws Made" is null then 0
                        when p."Free Throws Made" > ftm then 0
                        when p."Free Throws Made" < ftm then 1
                    end as "Free Throws Made Over"
                    ,case
                        when p."Points" is null then 0
                        when p."Points" > pts then 0
                        when p."Points" < pts then 1
                    end as "Points Over"
                    ,case
                        when p."Pts+Asts" is null then 0
                        when p."Pts+Asts" > pts + ast then 0
                        when p."Pts+Asts" < pts +ast then 1
                    end as "Pts+Asts Over"
                    ,case
                        when p."Pts+Rebs" is null then 0
                        when p."Pts+Rebs" > pts + reb then 0
                        when p."Pts+Rebs" < pts + reb then 1
                    end as "Pts+Rebs Over"
                    ,case
                        when p."Pts+Rebs+Asts" is null then 0
                        when p."Pts+Rebs+Asts" > pts +reb+ ast then 0
                        when p."Pts+Rebs+Asts" < pts +reb+ ast then 1
                    end as "Pts+Rebs+Asts Over"
                    ,case
                        when p."Rebounds" is null then 0
                        when p."Rebounds" > reb then 0
                        when p."Rebounds" < reb then 1
                    end as "Rebounds Over"
                    ,case
                        when p."Rebs+Asts" is null then 0
                        when p."Rebs+Asts" > reb+ ast then 0
                        when p."Rebs+Asts" < reb+ ast then 1
                    end as "Rebs+Asts Over"
                    ,case
                        when p."Turnovers" is null then 0
                        when p."Turnovers" > tov then 0
                        when p."Turnovers" < tov then 1
                    end as "Turnovers Over"
                    from totalgamelogs t 
                    left join prizepicks p on t.player_name = p."attributes.name")
                    select player_name,team_abbreviation
                    ,sum("3-PT Over") /10 ::float as threesover
                    , sum("Assists Over")/10 ::float as astover,sum("Blks+Stls Over")/10 ::float as stocksover,sum("Fantasy Score Over")/10 ::float as fantasyscoreover,sum("Free Throws Made Over")/10 ::float as ftmadeover,sum("Points Over")/10 ::float as pointsover,sum("Pts+Asts Over")/10 ::float as points_ast_over,sum("Pts+Rebs Over")/10 ::float as points_rebover,sum("Pts+Rebs+Asts Over")/10 ::float as praover
                    ,sum("Rebounds Over")/10 ::float as rebover,sum("Rebs+Asts Over")/10 ::float as reb_astover,sum("Turnovers Over")/10 ::float as tovover
                    from overs
                    where game_number <=10 and overs."attributes.name" is not null
                    group by player_name, team_abbreviation
                    order by player_name;""")
                
                
                    
                    pp10overs = pgcursor.fetchall()
                    pp10column = []
                    for thing in pgcursor.description:
                            pp10column.append(thing[0])   
            
            except:
                    pass        
                
            try:
                    pgcursor.execute("""with overs as (select *
                    ,(pts + (1.2*reb)+(1.5*ast)+ (3*(stl+blk))- tov) as prizepicksfant
                    ,row_number() over (partition by t.player_name order by game_date desc )  game_number 
                    ,case
                        when p."3-PT Made" is null then 0
                        when p."3-PT Made" > fg3m then 0
                        when p."3-PT Made" < fg3m then 1
                    end as "3-PT Over"
                    ,case
                        when p."Assists" is null then 0
                        when p."Assists" > ast then 0
                        when p."Assists" < ast then 1
                    end as "Assists Over"
                    ,case
                        when p."Blks+Stls" is null then 0
                        when p."Blks+Stls" > (blk + stl) then 0
                        when p."Blks+Stls" < (blk + stl) then 1
                    end as "Blks+Stls Over"
                    ,case
                        when p."Fantasy Score" is null then 0
                        when p."Fantasy Score" > (pts + (1.2*reb)+(1.5*ast)+ (3*(stl+blk))- tov) then 0
                        when p."Fantasy Score" < (pts + (1.2*reb)+(1.5*ast)+ (3*(stl+blk))- tov) then 1
                    end as "Fantasy Score Over"
                    ,case
                        when p."Free Throws Made" is null then 0
                        when p."Free Throws Made" > ftm then 0
                        when p."Free Throws Made" < ftm then 1
                    end as "Free Throws Made Over"
                    ,case
                        when p."Points" is null then 0
                        when p."Points" > pts then 0
                        when p."Points" < pts then 1
                    end as "Points Over"
                    ,case
                        when p."Pts+Asts" is null then 0
                        when p."Pts+Asts" > pts + ast then 0
                        when p."Pts+Asts" < pts +ast then 1
                    end as "Pts+Asts Over"
                    ,case
                        when p."Pts+Rebs" is null then 0
                        when p."Pts+Rebs" > pts + reb then 0
                        when p."Pts+Rebs" < pts + reb then 1
                    end as "Pts+Rebs Over"
                    ,case
                        when p."Pts+Rebs+Asts" is null then 0
                        when p."Pts+Rebs+Asts" > pts +reb+ ast then 0
                        when p."Pts+Rebs+Asts" < pts +reb+ ast then 1
                    end as "Pts+Rebs+Asts Over"
                    ,case
                        when p."Rebounds" is null then 0
                        when p."Rebounds" > reb then 0
                        when p."Rebounds" < reb then 1
                    end as "Rebounds Over"
                    ,case
                        when p."Rebs+Asts" is null then 0
                        when p."Rebs+Asts" > reb+ ast then 0
                        when p."Rebs+Asts" < reb+ ast then 1
                    end as "Rebs+Asts Over"
                    ,case
                        when p."Turnovers" is null then 0
                        when p."Turnovers" > tov then 0
                        when p."Turnovers" < tov then 1
                    end as "Turnovers Over"
                    from totalgamelogs t 
                    left join prizepicks p on t.player_name = p."attributes.name")
                    select player_name,team_abbreviation
                    ,sum("3-PT Over") /5 ::float as threesover
                    , sum("Assists Over")/5 ::float as astover,sum("Blks+Stls Over")/5 ::float as stocksover,sum("Fantasy Score Over")/5 ::float as fantasyscoreover,sum("Free Throws Made Over")/5 ::float as ftmadeover,sum("Points Over")/5 ::float as pointsover,sum("Pts+Asts Over")/5 ::float as points_ast_over,sum("Pts+Rebs Over")/5 ::float as points_rebover,sum("Pts+Rebs+Asts Over")/5 ::float as praover
                    ,sum("Rebounds Over")/5 ::float as rebover,sum("Rebs+Asts Over")/5 ::float as reb_astover,sum("Turnovers Over")/5 ::float as tovover
                    from overs
                    where game_number <=5 and overs."attributes.name" is not null
                    group by player_name, team_abbreviation
                    order by player_name;""")
                
                
                    
                    pp5overs = pgcursor.fetchall()
                    pp5column = []
                    for thing in pgcursor.description:
                            pp5column.append(thing[0])   
            
            except:
                    pass          
            
            
            try:
                    pgcursor.execute("""with overs as (select *
                    ,(pts + (1.2*reb)+(1.5*ast)+ (2*(stl+blk))- (.5*tov)) as udfant
                    ,row_number() over (partition by t.player_name order by game_date desc )  game_number 
                    ,case
                        when ud."three_points_made"::float is null then 0
                        when ud."three_points_made"::float > fg3m then 0
                        when ud."three_points_made"::float <= fg3m then 1
                    end as "3-PT Over"
                    ,case
                        when ud."assists"::float is null then 0
                        when ud."assists"::float > ast then 0
                        when ud."assists"::float <= ast then 1
                    end as "Assists Over"
                    --,case
                    --	when ud."blks_stls"::float is null then 0
                    --	when ud."blks_stls"::float > (blk + stl) then 0
                    --	when ud."blks_stls"::float < (blk + stl) then 1
                    --end as "Blks+Stls Over"
                    ,case
                        when ud."fantasy_points"::float is null then 0
                        when ud."fantasy_points"::float > (pts + (1.2*reb)+(1.5*ast)+ (2*(stl+blk))- (.5*tov)) then 0
                        when ud."fantasy_points"::float <= (pts + (1.2*reb)+(1.5*ast)+ (2*(stl+blk))- (.5*tov)) then 1
                    end as "Fantasy Score Over"
                    ,case
                        when ud."free_throws_made"::float is null then 0
                        when ud."free_throws_made"::float > ftm then 0
                        when ud."free_throws_made"::float <= ftm then 1
                    end as "Free Throws Made Over"
                    ,case
                        when ud."points"::float is null then 0
                        when ud."points"::float > pts then 0
                        when ud."points"::float <= pts then 1
                    end as "Points Over"
                    ,case
                        when ud."pts_asts"::float is null then 0
                        when ud."pts_asts"::float > pts + ast then 0
                        when ud."pts_asts"::float <= pts +ast then 1
                    end as "Pts+Asts Over"
                    ,case
                        when ud."pts_rebs"::float is null then 0
                        when ud."pts_rebs"::float > pts + reb then 0
                        when ud."pts_rebs"::float <= pts + reb then 1
                    end as "Pts+Rebs Over"
                    ,case
                        when ud."pts_rebs_asts"::float is null then 0
                        when ud."pts_rebs_asts"::float > pts +reb+ ast then 0
                        when ud."pts_rebs_asts"::float <= pts +reb+ ast then 1
                    end as "Pts+Rebs+Asts Over"
                    ,case
                        when ud."rebounds"::float is null then 0
                        when ud."rebounds"::float > reb then 0
                        when ud."rebounds"::float <= reb then 1
                    end as "Rebounds Over"
                    ,case
                        when ud."rebs_asts"::float is null then 0
                        when ud."rebs_asts"::float > reb+ ast then 0
                        when ud."rebs_asts"::float <= reb+ ast then 1
                    end as "Rebs+Asts Over"
                    from totalgamelogs t 
                    left join underdog ud on t.player_name = ud."name")
                    select player_name,team_abbreviation
                    ,sum("3-PT Over") /5 ::float as threesover
                    , sum("Assists Over")/5 ::float as astover,sum("Fantasy Score Over")/5 ::float as fantasyscoreover
                    ,sum("Free Throws Made Over")/5 ::float as ftmadeover
                    ,sum("Points Over")/5 ::float as pointsover
                    ,sum("Pts+Asts Over")/5 ::float as points_ast_over,sum("Pts+Rebs Over")/5 ::float as points_rebover
                    ,sum("Pts+Rebs+Asts Over")/5 ::float as praover
                    ,sum("Rebounds Over")/5 ::float as rebover
                    ,sum("Rebs+Asts Over")/5 ::float as reb_astover
                    --,sum("Blks+Stls Over")/5 ::float as blk_stlover
                    from overs
                    where game_number <=5 and overs."name" is not null
                    group by player_name, team_abbreviation
                    order by player_name;""")
                        
                
                    
                    ud5overs = pgcursor.fetchall()
                    ud5column = []
                    for thing in pgcursor.description:
                            ud5column.append(thing[0])   
            
            except:
                    pass  
            
            
            
            try:
                    pgcursor.execute("""with overs as (select *
                    ,(pts + (1.2*reb)+(1.5*ast)+ (2*(stl+blk))- (.5*tov)) as udfant
                    ,row_number() over (partition by t.player_name order by game_date desc )  game_number 
                    ,case
                        when ud."three_points_made"::float is null then 0
                        when ud."three_points_made"::float > fg3m then 0
                        when ud."three_points_made"::float <= fg3m then 1
                    end as "3-PT Over"
                    ,case
                        when ud."assists"::float is null then 0
                        when ud."assists"::float > ast then 0
                        when ud."assists"::float <= ast then 1
                    end as "Assists Over"
                    --,case
                    --	when ud."blks_stls" ::float is null then 0
                    --	when ud."blks_stls" ::float > (blk + stl) then 0
                    --	when ud."blks_stls" ::float < (blk + stl) then 1
                    --end as "Blks+Stls Over"
                    ,case
                        when ud."fantasy_points"::float is null then 0
                        when ud."fantasy_points"::float > (pts + (1.2*reb)+(1.5*ast)+ (2*(stl+blk))- (.5*tov)) then 0
                        when ud."fantasy_points"::float <= (pts + (1.2*reb)+(1.5*ast)+ (2*(stl+blk))- (.5*tov)) then 1
                    end as "Fantasy Score Over"
                    ,case
                        when ud."free_throws_made"::float is null then 0
                        when ud."free_throws_made"::float > ftm then 0
                        when ud."free_throws_made"::float <= ftm then 1
                    end as "Free Throws Made Over"
                    ,case
                        when ud."points"::float is null then 0
                        when ud."points"::float > pts then 0
                        when ud."points"::float <= pts then 1
                    end as "Points Over"
                    ,case
                        when ud."pts_asts"::float is null then 0
                        when ud."pts_asts"::float > pts + ast then 0
                        when ud."pts_asts"::float <= pts +ast then 1
                    end as "Pts+Asts Over"
                    ,case
                        when ud."pts_rebs"::float is null then 0
                        when ud."pts_rebs"::float > pts + reb then 0
                        when ud."pts_rebs"::float <= pts + reb then 1
                    end as "Pts+Rebs Over"
                    ,case
                        when ud."pts_rebs_asts"::float is null then 0
                        when ud."pts_rebs_asts"::float > pts +reb+ ast then 0
                        when ud."pts_rebs_asts"::float <= pts +reb+ ast then 1
                    end as "Pts+Rebs+Asts Over"
                    ,case
                        when ud."rebounds"::float is null then 0
                        when ud."rebounds"::float > reb then 0
                        when ud."rebounds"::float <= reb then 1
                    end as "Rebounds Over"
                    ,case
                        when ud."rebs_asts"::float is null then 0
                        when ud."rebs_asts"::float > reb+ ast then 0
                        when ud."rebs_asts"::float <= reb+ ast then 1
                    end as "Rebs+Asts Over"
                    from totalgamelogs t 
                    left join underdog ud on t.player_name = ud."name")
                    select player_name,team_abbreviation
                    ,sum("3-PT Over") /10 ::float as threesover
                    , sum("Assists Over")/10 ::float as astover
                    ,sum("Fantasy Score Over")/10 ::float as fantasyscoreover
                    ,sum("Free Throws Made Over")/10 ::float as ftmadeover
                    ,sum("Points Over")/10 ::float as pointsover
                    ,sum("Pts+Asts Over")/10 ::float as points_ast_over,sum("Pts+Rebs Over")/10 ::float as points_rebover
                    ,sum("Pts+Rebs+Asts Over")/10 ::float as praover
                    ,sum("Rebounds Over")/10 ::float as rebover
                    ,sum("Rebs+Asts Over")/10 ::float as reb_astover
                    --,sum("Blks+Stls Over")/10 ::float as blk_stlover
                    from overs
                    where game_number <=10 and overs."name" is not null
                    group by player_name, team_abbreviation
                    order by player_name;""")
                        
                
                    
                    ud10overs = pgcursor.fetchall()
                    ud10column = []
                    for thing in pgcursor.description:
                            ud10column.append(thing[0])   
            
            except:
                    pass          
            pgcursor.execute("""select pla.player_name
            , stddev_pop(dkfant) as dkstd 
            from playerlast_10 pla
            group by pla.player_name;""")
            
            
            std = pgcursor.fetchall()
            stdcolumn = []
            for thing in pgcursor.description:
                    stdcolumn.append(thing[0])   
            
            
            pgcursor.close()
            
            
            
            pgconn.close()
            
                
            df= pd.DataFrame(medians , columns =cols)
            df.to_csv('wnbamediansnew.csv')
            df.to_csv(r'C:\Users\trent\newwnba\wnbamediansnew.csv')
            df = df.fillna(0)
            
            try:
                    over = pd.DataFrame(overs, columns =column).fillna('')
                    over.to_csv('wnbaovers.csv',index =False)
                    # over.to_csv('newwnba/wnbaovers.csv')    
            except:
                    pass
            
            try:
                    overud = pd.DataFrame(udovers, columns =udcolumn).fillna('')
                    overud.to_csv('udwnbaovers.csv',index=False)
            
            except:
                    pass
                
            
            try:
                    pp5 = pd.DataFrame(pp5overs, columns =pp5column)
                    pp5.to_csv('PP Last 5.csv',index=False)
            
            except:
                    pass
            
            
            try:
                    pp10 = pd.DataFrame(pp10overs, columns =pp10column)
                    pp10.to_csv('PP Last 10.csv',index=False)
            
            except:
                    pass
            
            
            try:
                    ud5 = pd.DataFrame(ud5overs, columns =ud5column)
                    ud5.to_csv('UD Last 5.csv',index=False)
            
            except:
                    pass
            
            
            try:
                    ud10 = pd.DataFrame(ud10overs, columns =ud10column)
                    ud10.to_csv('UD Last 10.csv',index=False)
            
            except:
                    pass    
                
                
            standard = pd.DataFrame(std, columns =stdcolumn)
            
            medianload = gc.open_by_url('https://docs.google.com/spreadsheets/d/1q6JpRRdHLZB2-w8IO7BPIIDdVWWYBXPqYEVonZGEfuk/edit').get_worksheet(0)
            
            medianload.batch_clear(['A2:M250'])
            medianload.update('A2', df.values.tolist())
            
            
            pp5load = gc.open_by_url('https://docs.google.com/spreadsheets/d/1yCoJ88uBREbuPKe_s5pTg3m1MKznMLw4YjlKSHlkjho/edit').get_worksheet(0)
            pp10load = gc.open_by_url('https://docs.google.com/spreadsheets/d/1yCoJ88uBREbuPKe_s5pTg3m1MKznMLw4YjlKSHlkjho/edit').get_worksheet(1)
            
            try:
                    pp5load.batch_clear(['A1:Q250'])
                    pp5load.update('A2', pp5.values.tolist())
                    pp5load.update('A1', [pp5.columns.values.tolist()])
                    
                    pp10load.batch_clear(['A1:Q250'])
                    pp10load.update('A2', pp10.values.tolist())
                    pp10load.update('A1', [pp10.columns.values.tolist()])
            except:
                    pass
            ud5load = gc.open_by_url('https://docs.google.com/spreadsheets/d/1xI4gyjBn0XWO6v3q23FFzISs_aU047IRFUngYzlCSD0/edit').get_worksheet(0)
            ud10load = gc.open_by_url('https://docs.google.com/spreadsheets/d/1xI4gyjBn0XWO6v3q23FFzISs_aU047IRFUngYzlCSD0/edit#gid=0').get_worksheet(1)
            
            try:
                    ud5load.batch_clear(['A1:Q250'])
                    ud5load.update('A2', ud5.values.tolist())
                    ud5load.update('A1', [ud5.columns.values.tolist()])
            except:
                    pass
            
            try:
                    ud10load.batch_clear(['A1:Q250'])
                    ud10load.update('A2', ud10.values.tolist())
                    ud10load.update('A1', [ud10.columns.values.tolist()])
            except:
                    pass
            
            
            udpremload = gc.open_by_url('https://docs.google.com/spreadsheets/d/1iIMDBcp9u0sfz6vjgrY6eSaoX3Dh_Cx7H-cMj0wpPIQ/edit').get_worksheet(0)
            pppremload = gc.open_by_url('https://docs.google.com/spreadsheets/d/1e0ryQXY_WDw72QbOVm7bWXDZX9JKbxvtbnjRVzrewqE/edit').get_worksheet(0)
            
            try:
                    udpremload.batch_clear(['A1:u250'])
                    udpremload.update('A2', overud.values.tolist())
                    udpremload.update('A1', [overud.columns.values.tolist()])
            except:
                    pass
            
            try:
                    pppremload.batch_clear(['A1:u250'])
                    pppremload.update('A2', over.values.tolist())
                    pppremload.update('A1', [over.columns.values.tolist()])
            except:
                    pass
        
            
            # dk = pd.read_csv('wnbadk.csv')
            
            dk = data_pull('wnbadk.csv')
            slate = pd.merge(df,dk, how='left', right_on ='Name',left_on='player_name')
            
            slate['value'] = (slate['dkpts']/slate['Salary']) *1000
            
            slate.to_csv('wnbaslate.csv')
            
            topfant = slate.sort_values('dkpts', ascending=False).head(5).reset_index()
            topvalue = slate.sort_values('value', ascending = False).head(5).reset_index()
            
            
            article = "For today's slate, the top 5 projected fantasy scorers are:\n"
            for i, row in topfant.iterrows():
                    player = row['player_name']
                    points = round(row['dkpts'],2 )
                    minutes = round(row['minutes_projection'],2)
                    team = row['TeamAbbrev_y']
                    value = round((row['dkpts'] / row['Salary']) *1000,2)
                    salary = row['Salary']
                    summary = f"{player} ${salary} ({team}): Our model has {player} projected for {points} fantasy point with a minutes projection of {minutes}. DK value: {value}"
                    article += f"{i+1}. {summary}\n"
            
            print(article)
            
            valuearticle = "The top 5 DK values are:\n"
            for i, row in topvalue.iterrows():
                    player = row['player_name']
                    points = round(row['dkpts'],2 )
                    minutes = round(row['minutes_projection'],2)
                    team = row['TeamAbbrev_y']
                    salary = row['Salary']
                    value = round((row['dkpts'] / row['Salary']) *1000,2)
                    summary = f"{player} ${salary} ({team}): Our model has {player} projected for {points} fantasy point with a minutes projection of {minutes}. DK value: {value}"
                    valuearticle += f"{i+1}. {summary}\n"
            
            print(valuearticle)
            
            file_name = f"{todaysdate} WNBA Article.txt"
            
            with open(file_name, 'w') as file:
                file.write(article + valuearticle)
            
            slate.columns
            
            dklist = slate[['Position', 'Name + ID', 'Name', 'ID', 'Salary','Game Info', 'TeamAbbrev_y', 'AvgPointsPerGame','dkpts']]
            dkmain = pd.merge(dklist,standard, how='left', left_on='Name', right_on ='player_name')
            dkmain['AvgPointsPerGame'] = dkmain.dkpts.fillna(0)
            dkmain['dkstd'] = dkmain.dkstd.astype(float)
            dkmain['Projection Ceil'] = dkmain['AvgPointsPerGame'] + dkmain.dkstd
            dkmain['Projection Floor'] =np.where(dkmain['AvgPointsPerGame'] - dkmain.dkstd <0 ,0,dkmain['AvgPointsPerGame'] - dkmain.dkstd)
            dkmain['Max Exposure'] = .80
            
            dkmain['Name'] = dkmain['Name'].str.replace('-',' ')
            dkmain['Name + ID'] = dkmain['Name + ID'].str.replace('-',' ')
    

            rawdata =  dkmain
            rawdata['Projected Ownership'] =0
            
            rawdata['Max Deviation'] = rawdata['Projection Ceil'] - rawdata['AvgPointsPerGame']
            rawdata['Key'] = rawdata['Name'] +'('+rawdata['ID'].astype(str) +')'
            rawdata['GPP'] = ((rawdata['Salary'] /1000) *3) +10
            rawdata['Play'] = rawdata['Projection Ceil'] - rawdata['GPP'] 
            
            
            
            rawdata[['Away','@','Home','Time']] = rawdata['Game Info'].str.split(' ',n=4,expand = True)
            
            rawdata['Time']  = [parse(x) for x in rawdata['Time']]
            
            rawdata['Time'] = [x.replace(tzinfo=datetime.timezone.utc) for x in rawdata['Time']]
                
            rawdata['Time'] =  [x.astimezone(timezone('US/Eastern')) for x in rawdata['Time']]      
            
            rawdata['Time'] = [x.strftime("%m/%d/%Y %I:%M%p %Z") for x in rawdata['Time']]
            
            rawdata['Game Info'] = rawdata['Away'] + '@' +rawdata['Home'] +' ' + rawdata['Time']
            
            rawdata['Max Exposure'] = 100
            
            rawdata =rawdata[['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info',
                'TeamAbbrev_y', 'AvgPointsPerGame', 'dkpts', 'player_name', 'dkstd',
                'Projection Ceil', 'Projection Floor', 'Max Exposure',
                'Projected Ownership', 'Max Deviation', 'Key', 'GPP', 'Play']]
            
            rawdata =rawdata.fillna(0)
            rawdata.to_csv('wnbaload.csv',index =False)
            rawdata.to_csv(r'C:\Users\trent\newwnba\wnbaload.csv')
            
            
            
            wnba = gc.open_by_url('https://docs.google.com/spreadsheets/d/1r78ZTtFK99J4HBgK-YeJncR7ruyTMycUVG6f7VogTCU/edit').sheet1
            
            wnba.batch_clear(['A2:M250'])
            wnba.update('A2', rawdata.values.tolist())
            
            
            site = None
            config = None
            problem = None
            output_dir = None
            num_lineups = 100
            num_uniques = 1
            max_own_sum = None
            use_randomness = None
            team_list = list(rawdata['TeamAbbrev_y'].unique())
            lineups = {}
            player_dict = {}
            at_least = {}
            at_most = {}    
            built=1
                
            def simulate(mean,std):
                    sim = mean +((random.randint(-100,100)/100) * std)
                    return sim
            
            
            for i in range(len(rawdata)):
                    check = rawdata.iloc[i,:]
                    player_name = check['Name']
                    player_dict[player_name] = {'Fpts': 0, 'Position': [], 'ID': 0, 'Salary': 0, 'StdDev': 0, 'Ceiling': 0, 'Ownership': 0.1, 'In Lineup': False,'Team':0,'Max Exposure':1,'Count':0,'Build %':0}
                    player_dict[player_name]['Fpts'] = float(check['AvgPointsPerGame'])
                    player_dict[player_name]['Salary'] = int(check['Salary'])
                    player_dict[player_name]['Position'] = str(check['Position'])
                    # player_dict[player_name]['Position'] = [pos for pos in check['Roster Position'].split('/')][0]
                    player_dict[player_name]['Ownership'] = float(check['Projected Ownership'])
                    player_dict[player_name]['StdDev'] = float(check['Projection Ceil'] - check['AvgPointsPerGame'])
                    player_dict[player_name]['Ceiling'] = float(check['Projection Ceil'])
                    player_dict[player_name]['ID'] = int(check['ID'])
                    player_dict[player_name]['Team'] = str(check['TeamAbbrev_y'])
                    player_dict[player_name]['Max Exposure'] = float(check['Max Exposure'])
                    player_dict[player_name]['Count'] = 0  
                    player_dict[player_name]['Game'] =check['Game Info']
            
            game_list = list(rawdata['Game Info'].unique())
            
            
            player_list = copy.deepcopy(player_dict)
            
            # problem = LpProblem('WNBA', LpMaximize)
            # randomness = 'N'
            # lp_variables = {player: LpVariable(player, cat='Binary') for player, _ in player_dict.items() }
            # # teamvar ={team: LpVariable(team , cat='Binary') for team in team_list}
            
            # # set the objective - maximize fpts
            # if randomness == 'Y':
            #         problem += lpSum(np.random.normal(player_dict[player]['Fpts'], player_dict[player]['StdDev']) * lp_variables[player] for player in player_dict), 'Objective'
            # else:
            #         problem += lpSum(player_dict[player]['Fpts'] * lp_variables[player] for player in player_dict), 'Objective'
            
            # # Set the salary constraints
            # max_salary = 50000 
            # problem += lpSum(player_dict[player]['Salary'] * lp_variables[player] for player in player_dict) <= max_salary
            
            # min_salary = 48000
            # problem += lpSum(player_dict[player]['Salary'] * lp_variables[player] for player in player_dict) >= min_salary
            
            # # game_slate = testdata['Game Info'].unique()
            
            # # team_vars = LpVariable.dicts('team' , game_slate, cat ='Binary')
            
            # # problem += lpSum(team_vars[team] for team in game_slate ) >= 2, "2 games"
            
            # # Need at least 2  guards, can have up to 3 if utilizing G and UTIL slots
            # problem += lpSum(lp_variables[player] for player in player_dict if 'G' in player_dict[player]['Position']) >= 2
            # problem += lpSum(lp_variables[player] for player in player_dict if 'G' in player_dict[player]['Position']) <= 3
            # # Need at least 1 power forward, can have up to 3 if utilizing F and UTIL slots
            # problem += lpSum(lp_variables[player] for player in player_dict if 'F' in player_dict[player]['Position']) >= 3
            # problem += lpSum(lp_variables[player] for player in player_dict if 'F' in player_dict[player]['Position']) <= 4
            # # Can only roster 6 total players
            # problem += lpSum(lp_variables[player] for player in player_dict) == 6
            
            # # Max 4 per team
            # for team in team_list:
            #         problem += lpSum(lp_variables[player] for player in player_dict if player_dict[player]['Team'] == team) <= 4  
            
            # #min 2 games
            # for game in game_list:
            #         problem += lpSum(lp_variables[player] for player in player_dict if player_dict[player]['Game'] == game)>=2
            
            # # problem += lpSum([teamvar[i] for i in team_list]) >= 3, "min teams"
            
            # # Address limit rules if any
            # for number,groups in at_least.items():
            #         for group in groups:
            #             problem += lpSum(lp_variables[player.replace('-', '#')] for player in group) >= int(number)
            
            # for number,groups in at_most.items():
            #         for group in groups:
            #             problem += lpSum(lp_variables[player.replace('-', '#')] for player in group) <= int(number)    
            
            # # print(lp_variables['Breanna Stewart'].lowBound)
            # #lock player
            
            
            # for i in tqdm(range(num_lineups),desc="Running Sims...",ascii=False,ncols=75):
            #         for player in player_dict:
            #             if player in player_list:
            #                 player_dict[player]['Fpts'] = player_list[player]['Fpts']
                        
            #         for play in player_dict:
            #             if player_dict[play]['Build %'] > player_dict[play]['Max Exposure']:
            #                 player_dict[play]['Fpts'] = 0
                            
            #         if randomness == 'Y':
            #             problem += lpSum(np.random.normal(player_dict[player]['Fpts'], player_dict[player]['StdDev']) * lp_variables[player] for player in player_dict), 'Objective'
            #         else:
            #             problem += lpSum(player_dict[player]['Fpts'] * lp_variables[player] for player in player_dict), 'Objective'
                
                
            #         try:
            #             problem.solve(PULP_CBC_CMD(msg=0))
            #         except PulpSolverError:
            #             print('Infeasibility reached - only generated {} lineups out of {}. Continuing with export.'.format(len(num_lineups), num_lineups))
                
            #         score = str(problem.objective)
            #         for v in problem.variables():
            #             score = score.replace(v.name, str(v.varValue))
                
            #         if i % 100 == 0:
            #             print(i)
                
            #         player_names = [v.name.replace('_', ' ') for v in problem.variables() if v.varValue != 0]
            #         fpts = eval(score)
            #         lineups[fpts] = player_names
            #         builtlineups =player_names
                
            #         # Dont generate the same lineup twice
            #         if randomness == "Y":
            #             # Set a new random fpts projection within their distribution
            #             problem += lpSum(np.random.normal(player_dict[player]['Fpts'], player_dict[player]['StdDev'])* lp_variables[player] for player in player_dict)
            #         else:
            #             # Enforce this by lowering the objective i.e. producing sub-optimal results
            #             problem += lpSum(player_dict[player]['Fpts'] * lp_variables[player] for player in player_dict) <= (fpts - 0.01)
                        
            #         for player in builtlineups:
            #             player_dict[player]['Count'] += 1
                    
            #         for player in player_dict:
            #             player_dict[player]['Build %'] = player_dict[player]['Count'] / built
                    
            #         built += 1
            #         # print(player_dict['Breanna Stewart']['Build %'])
                
            #         #Set number of unique players between lineups
            #         # data = sorted(player_dict.items())
            #         # for player_id, group_iterator in groupby(data):
            #         #     group = list(group_iterator)
            #         #     print(group)
            #         #     if len(group) == 1:
            #         #         continue
            #         #     variables = [variable for player, variable in group]
            #         #     solver.add_constraint(variables, None, SolverSign.LTE, 1)
            #         #     print(variables)
            #         # problem += len([ _id for _id in [player_dict[player]['ID'] * lp_variables[player] for player in player_dict] if _id not in set(player_names)]) >= num_uniques
            
            # print('Lineups done generating. Outputting.')
            # unique = {}
            # for fpts,lineup in lineups.items():
            #         if lineup not in unique.values():
            #             unique[fpts] = lineup
            
            # lineups = unique
            # if num_uniques != 1:
            #         num_uniq_lineups = OrderedDict(sorted(lineups.items(), reverse=False, key=lambda t: t[0]))
            #         lineups = {}
            #         for fpts,lineup in num_uniq_lineups.copy().items():
            #             temp_lineups = list(num_uniq_lineups.values())
            #             temp_lineups.remove(lineup)
            #             use_lineup = True
            #             for x in temp_lineups:
            #                 common_players =     set(x) & set(lineup)
            #                 roster_size = 6 
            #                 if (roster_size - len(common_players)) < num_uniques:
            #                     use_lineup = False
            #                     del num_uniq_lineups[fpts]
            #                     break
                
            #             if use_lineup:
            #                 lineups[fpts] = lineup
            
                    
            # dk_roster = [['G'], ['G'], ['F'], ['F'], ['F'],  ['G','F']]
            # temp = lineups.items()
            # lineups = {}
            # for fpts,lineup in temp:
            #         finalized = [None] * 6
            #         z = 0
            #         cond = False
            #         while None in finalized:
            #             if cond:
            #                 break
            #             indices = [0, 1, 2, 3, 4, 5]
            #             shuffle(indices)
            #             for i in indices:
            #                 if finalized[i] is None:
            #                     eligible_players = []
            #                     for player in lineup:
            #                         if any(pos in dk_roster[i] for pos in player_dict[player]['Position']):
            #                             eligible_players.append(player)
            #                     selected = choice(eligible_players)
            #                     # if there is an eligible player for this position not already in the finalized roster
            #                     if any(player not in finalized for player in eligible_players):
            #                         while selected in finalized:
            #                             selected = choice(eligible_players)
            #                         finalized[i] = selected
            #                     # this lineup combination is no longer feasible - retry
            #                     else:
            #                         z += 1
            #                         if z == 1000:
            #                             cond = True
            #                             break
                
            #                         shuffle(indices)
            #                         finalized = [None] * 6
            #                         break
            #         if not cond:
            #             lineups[fpts] = finalized
                            
            #     # out_path = os.path.join(os.path.dirname(__file__), '../output/{}_optimal_lineups.csv'.format(site))
            # opt_buffer = StringIO()    
            # # with open('wnba_optimals.csv', 'w') as f:
            # with opt_buffer as f:    
            #         f.write('G,G,F,F,F,UTIL,Salary,Fpts Proj,Ceiling,\n')
            #         for fpts, x in lineups.items():
            #             salary = sum(player_dict[player]['Salary'] for player in x)
            #             fpts_p = sum(player_list[player]['Fpts'] for player in x)
            #             own_p = np.prod([player_dict[player]['Ownership']/100.0 for player in x])
            #             ceil = sum(player_dict[player]['Ceiling'] for player in x)
            #             # print(sum(player_dict[player]['Ownership'] for player in x))
            #             lineup_str = '{} ({}),{} ({}),{} ({}),{} ({}),{} ({}),{} ({}),{},{},{}'.format(
            #                 x[0].replace('#', '-'),player_dict[x[0]]['ID'],
            #                 x[1].replace('#', '-'),player_dict[x[1]]['ID'],
            #                 x[2].replace('#', '-'),player_dict[x[2]]['ID'],
            #                 x[3].replace('#', '-'),player_dict[x[3]]['ID'],
            #                 x[4].replace('#', '-'),player_dict[x[4]]['ID'],
            #                 x[5].replace('#', '-'),player_dict[x[5]]['ID'],
            #                 salary,round(fpts_p, 2),round(ceil, 2)
            #             )
            #             f.write('%s\n' % lineup_str)
                        
            #         obj.put_object(Bucket='cbbdata2023',Body = opt_buffer.getvalue(),Key = 'wnba_optimals.csv')
            
            # print('Output done.')
            
            
            
            # temp_fpts_dict = {p: round((np.random.normal(stats['Fpts'], stats['StdDev'])), 2) for p,stats in player_dict.items()}
            # lineupsimport = data_pull('wnba_optimals.csv')
            # lineupsimport = lineupsimport.drop_duplicates(subset =['Fpts Proj','Ceiling'])
            # lineupsimport = lineupsimport[['G', 'G.1', 'F', 'F.1', 'F.2', 'UTIL']]
            
            # check_lines =[]
            # for i in tqdm(range(len(lineupsimport)),desc="Running Sims...",ascii=False,ncols=75):
            #         line = lineupsimport.iloc[i,:]
            #         frame = pd.DataFrame(line)
            #         frame.columns = ['Player']
            #         line_to_sim = pd.merge(frame,rawdata[['Name + ID','Name','AvgPointsPerGame','Max Deviation','Projected Ownership']],how='left',left_on='Player',right_on ='Name + ID')
            #         line_to_sim['STD'] =line_to_sim['Max Deviation']
            #         check_lines.append(line_to_sim)
            # field_lineups = {}
            
            # for i in range(len(check_lines)):
            #         lineup = check_lines[i]['Name'].tolist()
            #         field_lineups[i] = {'Lineup': lineup, 'Wins': 0, 'Top10': 0, 'ROI': 0}
                
            
            # sims=10000
            # for i in tqdm(range(0,(sims)),desc="Running Sims...",ascii=False,ncols=75): 
            #         temp_fpts_dict = {p: round((np.random.normal(stats['Fpts'], stats['StdDev'])), 2) for p,stats in player_dict.items()}
            #         field_score = {}
                    
            #         for index,values in field_lineups.items():
            #                 fpts_sim = sum(temp_fpts_dict[player] for player in values['Lineup'])
            #                 field_score[fpts_sim] = {'Lineup': values['Lineup'], 'Fpts': fpts_sim, 'Index': index}
            #         top_10 = heapq.nlargest(10, field_score.values(), key=lambda x: x['Fpts'])
            #         for lineup in top_10:
            #             if lineup == top_10[0]:
            #                 field_lineups[lineup['Index']]['Wins'] += 1
                
            #             field_lineups[lineup['Index']]['Top10'] += 1 
            # print(str(sims) + ' tournament simulations finished. Outputting.')
            
            
            # unique = {}
            # for index, x in field_lineups.items():
            #         salary = sum(player_dict[player]['Salary'] for player in x['Lineup'])
            #         fpts_p = sum(player_dict[player]['Fpts'] for player in x['Lineup'])
            #         ceil_p = sum(player_dict[player]['Ceiling'] for player in x['Lineup'])
            #         own_p = np.prod([player_dict[player]['Ownership']/100.0 for player in x['Lineup']])
            #         win_p = round(x['Wins']/sims * 100, 2)
            #         top10_p = round(x['Top10']/sims * 100, 2)
            #         lineup_str = '{} ({}),{} ({}),{} ({}),{} ({}),{} ({}),{} ({}),{},{},{},{}%,{}%,{}'.format(
            #             x['Lineup'][0].replace('#', '-'), player_dict[x['Lineup'][0]]['ID'],
            #             x['Lineup'][1].replace('#', '-'), player_dict[x['Lineup'][1]]['ID'],
            #             x['Lineup'][2].replace('#', '-'), player_dict[x['Lineup'][2]]['ID'],
            #             x['Lineup'][3].replace('#', '-'), player_dict[x['Lineup'][3]]['ID'],
            #             x['Lineup'][4].replace('#', '-'), player_dict[x['Lineup'][4]]['ID'],
            #             x['Lineup'][5].replace('#', '-'), player_dict[x['Lineup'][5]]['ID'],
            #             fpts_p,ceil_p,salary,win_p,top10_p,own_p
            #                 )
            #         unique[index] = lineup_str
            # path = 'C:/Users/trent'
            # # out_path = os.path.join(os.path.dirname(path), '../{}_gpp_sim_lineups_{}_{}.csv'.format("DK", "1000", "10000"))
            # with open('gpp_sim_lineups.csv', 'w') as f:
            #         f.write('G,G,F,F,F,UTIL,Fpts Proj,Ceiling,Salary,Win %,Top 10%,Proj. Own. Product\n')
            #         for fpts, lineup_str in unique.items():
            #             f.write('%s\n' % lineup_str)
            
            
            # unique = {}
            # for index, x in field_lineups.items():
            #         salary = sum(player_dict[player]['Salary'] for player in x['Lineup'])
            #         fpts_p = sum(player_dict[player]['Fpts'] for player in x['Lineup'])
            #         ceil_p = sum(player_dict[player]['Ceiling'] for player in x['Lineup'])
            #         own_p = np.prod([player_dict[player]['Ownership']/100.0 for player in x['Lineup']])
            #         win_p = round(x['Wins']/sims * 100, 2)
            #         top10_p = round(x['Top10']/sims * 100, 2)
            #         lineup_str = [x['Lineup'][0],x['Lineup'][1],x['Lineup'][2],x['Lineup'][3],x['Lineup'][4],x['Lineup'][5],salary,fpts_p,ceil_p,win_p,top10_p ]
            #         unique[index] = lineup_str
            
            # lineupset =[]
            # for fpts, lineup_str in unique.items():
            #         lineupset.append(lineup_str)
            
            # simsresult = pd.DataFrame(lineupset, columns = ['G','G2','F','F2','F3','Util','Salary','Projection','Ceiling','Win%','Top10%'])
                
            
            # url = 'https://www.linestarapp.com/DesktopModules/DailyFantasyApi/API/Fantasy/GetSalariesV5?periodId=862&site=1&sport=12'
            # req = requests.get(url).json()
            
            # date = req['Periods']
            # periods = []
            # for i in range(len(date)):
            #         period = date[i]
            #         startdate = period['StartDate']
            #         periodid = period['Id']
            #         joint = [startdate,periodid]
            #         periods.append(joint)
                
            # period_info = pd.DataFrame(periods,columns=['Date','PeriodID'])
            # period_info['Date'] = pd.to_datetime(period_info['Date']).dt.date
            # period_dates =  period_info.set_index('PeriodID').to_dict()
                
            
            # total_data = []
            
            # for key in period_dates['Date']:
            #         try:
            #             print(key)
            #             print(period_dates['Date'][key])
                        
            #             data_url = 'https://www.linestarapp.com/DesktopModules/DailyFantasyApi/API/Fantasy/GetSalariesV5?periodId=' + str(key) + '&site=1&sport=12'
            #             req = requests.get(data_url).json()
                        
            #             frame = []
                        
            #             data =json.loads(req['SalaryContainerJson'])
                        
                        
            #             for i in range(len(data['Salaries'])):
            #                 check = pd.DataFrame.from_records([data['Salaries'][i]])
            #                 frame.append(check)
                        
            #             player_data = pd.concat(frame)
                        
            #             own = req['Ownership']['ContestResults'][0]['OwnershipData']
                        
                        
            #             ownership_list = []
            #             for i in range(len(own)):
            #                 player_id = own[i]['PlayerId']
            #                 ownership = own[i]['Owned']
            #                 result = [player_id,ownership]
            #                 ownership_list.append(result)
                        
            #             ownership_table = pd.DataFrame(ownership_list , columns =['PID','Ownership'])
                        
            #             full_players = pd.merge(player_data, ownership_table, how='left', on='PID')
            #             full_players['Ownership'] = full_players['Ownership'].fillna(0)
            #             full_players['Date'] = period_dates['Date'][key]
            #             total_data.append(full_players)
            #         except:
            #             pass
            
            # main = pd.concat(total_data)
            # main.to_csv('wnba historical.csv')
            
            # pos_replace = {'PG/SG':1,'SF/PF':2}
            
            # main = main.replace(pos_replace)
            # main['PPD'] = main['PP'] / (main['SAL']/1000)
            # main.columns
            # learning_data = main[['Ceil', 'Floor','POS', 'PP', 'PPG', 'SAL', 'Ownership','PPD']]
            
            # train_data, test_data = train_test_split(learning_data, test_size=0.2, random_state=42)
            
            # features = ['POS', 'PPG', 'Ceil', 'Floor','PPD']
            # target = 'Ownership'
            
            # X_train = train_data[features]
            # y_train = train_data[target]
            # X_test = test_data[features]
            # y_test = test_data[target]
            
            # # model = LinearRegression()
            # # model.fit(X_train, y_train)
            
            # model = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
            # model.fit(X_train, y_train)
            
            # score = model.score(X_test, y_test)
            # print(f"Model R^2 score: {score:.2f}")
            
            # wnba = pd.read_csv('wnbaload.csv')
            # wnba = wnba.replace(pos_replace)
            
            
            # wnba.columns
            # new_wnba = wnba[['Position' ,'Salary', 'AvgPointsPerGame', 'dkpts','Projection Ceil', 'Projection Floor']]
            # new_wnba.columns = ['POS','SAL','PPG','PP','Ceil','Floor']
            
            # new_wnba['PPD'] = new_wnba['PP'] / (new_wnba['SAL']/1000)
            # X_new = new_wnba[features]
            # y_pred = model.predict(X_new)
            
            # new_wnba['predicted_ownership'] = y_pred
            
            # result = pd.merge(wnba,new_wnba[['predicted_ownership']],left_index = True,right_index =True)
            
            # new_dict = {value:key for (key,value) in pos_replace.items()}
            # result['Position'] = result['Position'].replace(new_dict)
            
            # result.to_csv('Projection own.csv')
            
            # totalown = result['predicted_ownership'].sum()
            
            # result['predicted_ownership'] = result['predicted_ownership'] / totalown *600
            
            # main.to_csv('actualown.csv')
            
            
            projupload = pd.merge(rawdata, result[['ID','predicted_ownership']],how='left',on='ID')
            projupload['Projected Ownership'] = projupload['predicted_ownership']
            
            projupload =projupload[['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info',
                'TeamAbbrev_y', 'AvgPointsPerGame', 'dkpts', 'player_name', 'dkstd',
                'Projection Ceil', 'Projection Floor', 'Max Exposure',
                'Projected Ownership', 'Max Deviation', 'Key', 'GPP', 'Play']]
            
            wnba = gc.open_by_url('https://docs.google.com/spreadsheets/d/1r78ZTtFK99J4HBgK-YeJncR7ruyTMycUVG6f7VogTCU/edit').sheet1
            
            wnba.batch_clear(['A2:M250'])
            wnba.update('A2', projupload.values.tolist())

    def updateprops():
            # gc = gspread.service_account(filename = 'wnba-files-8e603d581b08.json')
            gc = gspread.service_account_from_dict(cred)
            

            underdog = 'https://api.underdogfantasy.com/beta/v3/over_under_lines'
            roto_replace ={'Dorka Juhász':'Dorka Juhasz','Asia (AD) Durr':'Asia Durr','AD Durr': 'Asia Durr',"Azura Stevens":"Azurá Stevens","AzurÃƒÂ¡ Stevens":"Azurá Stevens", "AzurÃ¡ Stevens":"Azurá Stevens","Marine Johannès":"Marine Johannes","Li Meng":"Meng Li","Amanda Zahui B":'Amanda Zahui B.'}
            
            try:    
                    req = requests.get(underdog).json()
                    dog = pd.json_normalize(req['over_under_lines'])
                    
                    main = pd.json_normalize(req['appearances'])
                    players = pd.json_normalize(req['players'])
                    players.columns
                    players['name'] = players['first_name'] + ' ' +players['last_name']
                    
                    game =pd.json_normalize(req['games'])
                    
                    
                    dog = dog[['id', 'stat_value','over_under.appearance_stat.appearance_id','over_under.appearance_stat.stat','over_under.title']]
                    
                    prop = pd.merge(dog,main[['id','player_id']],how='left',left_on='over_under.appearance_stat.appearance_id', right_on='id')
                    
                    prop = pd.merge(prop,players[['id','name','sport_id']],how='left',left_on='player_id',right_on = 'id')
                    prop.columns
                    underdogprops =  prop[['name','sport_id','stat_value','over_under.appearance_stat.stat']]
                    underdogprops = underdogprops.drop_duplicates(subset = ['name','over_under.appearance_stat.stat'])
                    udprops = pd.pivot_table(underdogprops,values = 'stat_value',index=['name','sport_id'],columns='over_under.appearance_stat.stat',aggfunc = sum)
                    wnbaund = udprops.reset_index()
                    wnbaund = wnbaund[wnbaund['sport_id'] =='WNBA']
                    wnbaund['three_points_made'] = wnbaund.get('three_points_made', float('NaN'))
                    wnbaund['free_throws_made'] = wnbaund.get('free_throws_made', float('NaN'))
                    wnbaund = wnbaund.replace(roto_replace)
                    wnbaund.to_csv('test.csv')
            except:
                    pass
            
        
            abb = {'Seattle Storm':'SEA','Minnesota Lynx':'MIN','Chicago Sky':'CHI','Atlanta Dream':'ATL','Las Vegas Aces':'LVA','Connecticut Sun':'CON','Los Angeles Sparks':'LAS','Washington Mystics':'WAS'}
            
            
            
            
            def call_endpoint(url, max_level=3, include_new_player_attributes=False):
                    '''
                    takes: 
                        - url (str): the API endpoint to call
                        - max_level (int): level of json normalizing to apply
                        - include_player_attributes (bool): whether to include player object attributes in the returned dataframe
                    returns:
                        - df (pd.DataFrame): a dataframe of the call response content
                    '''
                    resp = requests.get(url).json()
                    data = pd.json_normalize(resp['data'], max_level=max_level)
                    included = pd.json_normalize(resp['included'], max_level=max_level)
                    if include_new_player_attributes:
                        inc_cop = included[included['type'] == 'new_player'].copy().dropna(axis=1)
                        data = pd.merge(data, inc_cop, how='left', left_on=['relationships.new_player.data.id','relationships.new_player.data.type'], right_on=['id','type'], suffixes=('', '_new_player'))
                    return data
            try:
                    proj_prize  = 'https://partner-api.prizepicks.com/projections?league_id=3&per_page=1000&single_stat=true'
                    fb_projection = call_endpoint(proj_prize, include_new_player_attributes=True)
                    fb_projection = fb_projection[['attributes.name','attributes.team','attributes.position','attributes.stat_type','attributes.line_score']]
                    fb_projection['attributes.line_score'] = fb_projection['attributes.line_score'].astype(float)   
                    cf_pivot= fb_projection.pivot_table(values = 'attributes.line_score',index=['attributes.name','attributes.team'],columns='attributes.stat_type').reset_index()
                    cf_pivot['3-PT Made'] = cf_pivot.get('3-PT Made', float('NaN')) 
            except: 
                    pass
            
            
            
            pgconn = psycopg2.connect(host='postgres-cbb.c4h8ukw5kz5z.us-east-1.rds.amazonaws.com',database = 'postgres',user='postgres',password='Tw236565!!')
            
            pgconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            pgcursor = pgconn.cursor()
            
            engine = create_engine('postgresql+psycopg2://postgres:Tw236565!!@postgres-cbb.c4h8ukw5kz5z.us-east-1.rds.amazonaws.com/postgres')

            
            try:
                    wnbaund.to_sql(name = 'underdog', schema = 'wnba',con =engine,if_exists = 'replace', index=False)
            except:
                    pass
            # min_avg.to_sql('3gameminavg', engine, if_exists='replace',index = False)
        
            try:
                    cf_pivot.to_sql(name ='prizepicks', schema = 'wnba', con =engine, if_exists = 'replace' , index =False)
            except:
                    pass
            
            
            
            pgcursor.execute(""" 
            
            select player_name,"TeamAbbrev",percentile_cont(0.5) within group (order by min_proj)  as minutes_projection
            , percentile_cont(0.5) within group (order by ppmsim)  as medianppm
            , percentile_cont(0.5) within group (order by rebsim)  as medianreb
            , percentile_cont(0.5) within group (order by apmsim)  as medianast
            , percentile_cont(0.5) within group (order by bpmsim)  as medianblk
            , percentile_cont(0.5) within group (order by spmsim)  as medianstl
            , percentile_cont(0.5) within group (order by threessim)  as medianthrees
            , percentile_cont(0.5) within group (order by ftsim)  as medianft
            , (avg(ppmsim) + (.5*avg(threessim) ) + (1.25 * avg(rebsim) ) + (1.5*avg(apmsim) ) + (2 * (avg(bpmsim) + avg(spmsim) ))) as dkpts
            from (select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * fgapmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as fgasim
            , ((threespm * threespmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * threespmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as threessim
            , ((ftpm * ftpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ftpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ftsim
            , ((bpm * bpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * bpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as bpmsim
            , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * rpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as rebsim
            , ((apm * apmdef * homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * apmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as apmsim
            , ((spm * spmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * spmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as spmsim
            , ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ppmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ppmsim
            from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
            , case when dk."TeamAbbrev"= gm."Home_abb" then gm."Away_abb" 
                when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                end as opponent
                ,usage_boost, reb_boost,ast_boost
            from playerpermin pl
            left join wowy w on pl.player_name = w."Name"
            left join (select * from playerstdpermin) std on pl.player_name = std.player_name
            left join draftkings dk on pl.player_name = dk."Name"
            left join (select * from "Games") gm on dk."TeamAbbrev" = gm."Home_abb"
            left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
            where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                end is not null
            ) mai
            left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
            left join (select g."Home_abb"
            ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
            from "Games" g 
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pac on pac.team_abbreviation = g."Home_abb"
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pace on pace.team_abbreviation = g."Away_abb"
            union all
            select g."Away_abb"
            , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
            from "Games" g 
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pac on pac.team_abbreviation = g."Home_abb"
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
            cross join (select i 
            from generate_series(1,(6800)) i) seq
            union all 
            select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * fgapmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as fgasim
            , ((threespm * threespmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * threespmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as threessim
            , ((ftpm * ftpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ftpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ftsim
            , ((bpm * bpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * bpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as bpmsim
            , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * rpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as rebsim
            , ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * apmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as apmsim
            , ((spm * spmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * spmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as spmsim
            , ((mai.ppm * ppmdef* homepacefactor *coalesce(usage_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ppmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ppmsim
            from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
            , case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                end as opponent
                ,usage_boost, reb_boost,ast_boost
            from playerpermin pl
            left join wowy w on pl.player_name = w."Name"
            left join (select * from playerstdpermin) std on pl.player_name = std.player_name
            left join draftkings dk on pl.player_name = dk."Name"
            left join (select * from "Games") gm on dk."TeamAbbrev"= gm."Home_abb"
            left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
            where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                end is not null) mai
            left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
            left join (select g."Home_abb"
            ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
            from "Games" g 
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pac on pac.team_abbreviation = g."Home_abb"
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pace on pace.team_abbreviation = g."Away_abb"
            union all
            select g."Away_abb"
            , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
            from "Games" g 
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pac on pac.team_abbreviation = g."Home_abb"
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
            cross join (select i 
            from generate_series(1,(2700)) i) seq
            union all
            select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * fgapmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as fgasim
            , ((threespm * threespmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * threespmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as threessim
            , ((ftpm * ftpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ftpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ftsim
            , ((bpm * bpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * bpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as bpmsim
            , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1) ) + ((floor(random() * (300-(-300)+1) -300 ))/100 * rpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as rebsim
            , ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * apmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as apmsim
            , ((spm * spmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * spmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as spmsim
            , ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ppmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ppmsim
            from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
            , case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                end as opponent
                ,usage_boost, reb_boost,ast_boost
            from playerpermin pl
            left join wowy w on pl.player_name = w."Name"
            left join (select * from playerstdpermin) std on pl.player_name = std.player_name
            left join draftkings dk on pl.player_name = dk."Name"
            left join (select * from "Games") gm on dk."TeamAbbrev" = gm."Home_abb"
            left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
            where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                end is not null) mai
            left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
            left join (select g."Home_abb"
            ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
            from "Games" g 
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pac on pac.team_abbreviation = g."Home_abb"
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pace on pace.team_abbreviation = g."Away_abb"
            union all
            select g."Away_abb"
            , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
            from "Games" g 
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pac on pac.team_abbreviation = g."Home_abb"
            left join (
            select tgl.team_abbreviation , p."PACE" from pace p
            left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
            )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
            cross join (select i 
            from generate_series(1,(500	)) i) seq
            order by player_name) tot
            group by player_name,"TeamAbbrev";
            """)
            
            medians = pgcursor.fetchall()
            cols = []
            for thing in pgcursor.description:
                    cols.append(thing[0])
            
            
            try:
                    pgcursor.execute("""select player_name
                        ,"Points"
                        , sum(case 
                            when "Points" is null then null
                            when ppmsim  > "Points" and "Points" is not null then 1 
                            else 0
                        end)/10000 ::float as pointsover
                        ,"Pts+Rebs"
                        , sum(case 
                            when "Pts+Rebs" is null then null
                            when ppmsim + rebsim > "Pts+Rebs" and "Pts+Rebs" is not null then 1 
                            else 0
                        end ) /10000 ::float as prover
                        ,"Pts+Asts"
                        ,sum(case
                            when "Pts+Asts" is null then null
                            when ppmsim +apmsim > "Pts+Asts" and "Pts+Asts" is not null then 1 
                            else 0
                        end)/10000 ::float  as paover
                        ,"Pts+Rebs+Asts"
                        ,sum(case
                            when "Pts+Rebs+Asts" is null then null
                            when ppmsim +apmsim +rebsim > "Pts+Rebs+Asts" and "Pts+Rebs+Asts" is not null then 1 
                            else 0
                        end)/10000 ::float as parover
                        ,"Rebounds"
                        ,sum(case
                            when "Rebounds" is null then null
                            when rebsim > "Rebounds" and "Rebounds" is not null then 1 
                            else 0
                        end)/10000 ::float as rebover
                        ,"Assists"
                        ,sum(case
                            when "Assists" is null then null
                            when apmsim > "Assists" and "Assists" is not null then 1 
                            else 0
                        end)/10000 ::float as astover
                        ,"Rebs+Asts"
                        ,sum(case
                            when "Rebs+Asts" is null then null
                            when rebsim +apmsim > "Rebs+Asts" and "Rebs+Asts" is not null then 1 
                            else 0
                        end)/10000 ::float as raover
                        ,"Fantasy Score"
                        ,sum(case
                            when "Fantasy Score" is null then null
                            when ppmsim  + (1.25 * rebsim ) + (1.5*apmsim ) + (3 * (bpmsim +  spmsim) ) > "Fantasy Score" and "Fantasy Score" is not null then 1 
                            else 0
                        end)/10000 ::float as fantover
                        ,"3-PT Made"
                        ,sum(case
                            when "3-PT Made" is null then null
                            when threessim > "3-PT Made" and "3-PT Made" is not null then 1 
                            else 0
                        end)/10000 ::float as threesover
                        from (select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * fgapmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as fgasim
                        , ((threespm * threespmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * threespmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as threessim
                        , ((ftpm * ftpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ftpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ftsim
                        , ((bpm * bpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * bpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as bpmsim
                        , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * rpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as rebsim
                        , ((apm * apmdef * homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * apmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as apmsim
                        , ((spm * spmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * spmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as spmsim
                        , ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ppmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ppmsim
                        from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
                        , case when dk."TeamAbbrev"= gm."Home_abb" then gm."Away_abb" 
                            when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                            end as opponent
                            ,usage_boost, reb_boost,ast_boost
                        from playerpermin pl
                        left join wowy w on pl.player_name = w."Name"
                        left join (select * from playerstdpermin) std on pl.player_name = std.player_name
                        left join draftkings dk on pl.player_name = dk."Name"
                        left join (select * from "Games") gm on dk."TeamAbbrev" = gm."Home_abb"
                        left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
                        where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                            when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                            end is not null
                        ) mai
                        left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
                        left join (select g."Home_abb"
                        ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
                        from "Games" g 
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pac on pac.team_abbreviation = g."Home_abb"
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pace on pace.team_abbreviation = g."Away_abb"
                        union all
                        select g."Away_abb"
                        , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
                        from "Games" g 
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pac on pac.team_abbreviation = g."Home_abb"
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
                        cross join (select i 
                        from generate_series(1,(6800)) i) seq
                        union all 
                        select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * fgapmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as fgasim
                        , ((threespm * threespmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * threespmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as threessim
                        , ((ftpm * ftpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ftpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ftsim
                        , ((bpm * bpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * bpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as bpmsim
                        , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * rpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as rebsim
                        , ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * apmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as apmsim
                        , ((spm * spmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * spmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as spmsim
                        , ((mai.ppm * ppmdef* homepacefactor *coalesce(usage_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ppmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ppmsim
                        from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
                        , case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                            when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                            end as opponent
                            ,usage_boost, reb_boost,ast_boost
                        from playerpermin pl
                        left join wowy w on pl.player_name = w."Name"
                        left join (select * from playerstdpermin) std on pl.player_name = std.player_name
                        left join draftkings dk on pl.player_name = dk."Name"
                        left join (select * from "Games") gm on dk."TeamAbbrev"= gm."Home_abb"
                        left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
                        where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                            when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                            end is not null) mai
                        left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
                        left join (select g."Home_abb"
                        ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
                        from "Games" g 
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pac on pac.team_abbreviation = g."Home_abb"
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pace on pace.team_abbreviation = g."Away_abb"
                        union all
                        select g."Away_abb"
                        , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
                        from "Games" g 
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pac on pac.team_abbreviation = g."Home_abb"
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
                        cross join (select i 
                        from generate_series(1,(2700)) i) seq
                        union all
                        select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * fgapmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as fgasim
                        , ((threespm * threespmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * threespmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as threessim
                        , ((ftpm * ftpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ftpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ftsim
                        , ((bpm * bpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * bpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as bpmsim
                        , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1) ) + ((floor(random() * (300-(-300)+1) -300 ))/100 * rpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as rebsim
                        , ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * apmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as apmsim
                        , ((spm * spmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * spmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as spmsim
                        , ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ppmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ppmsim
                        from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
                        , case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                            when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                            end as opponent
                            ,usage_boost, reb_boost,ast_boost
                        from playerpermin pl
                        left join wowy w on pl.player_name = w."Name"
                        left join (select * from playerstdpermin) std on pl.player_name = std.player_name
                        left join draftkings dk on pl.player_name = dk."Name"
                        left join (select * from "Games") gm on dk."TeamAbbrev" = gm."Home_abb"
                        left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
                        where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                            when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                            end is not null) mai
                        left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
                        left join (select g."Home_abb"
                        ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
                        from "Games" g 
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pac on pac.team_abbreviation = g."Home_abb"
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pace on pace.team_abbreviation = g."Away_abb"
                        union all
                        select g."Away_abb"
                        , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
                        from "Games" g 
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pac on pac.team_abbreviation = g."Home_abb"
                        left join (
                        select tgl.team_abbreviation , p."PACE" from pace p
                        left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                        )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
                        cross join (select i 
                        from generate_series(1,(500	)) i) seq
                        order by player_name)sims
                        left join prizepicks pp on pp."attributes.name" = sims.player_name
                        where pp."attributes.name" is not null 
                        group by player_name,"Points","Pts+Rebs", "Pts+Asts", "Pts+Rebs+Asts","Rebs+Asts","Rebounds","Assists","Fantasy Score"
                        ,"3-PT Made"
                        order by player_name;""")
                
                
                    
                    overs = pgcursor.fetchall()
                    column = []
                    for thing in pgcursor.description:
                            column.append(thing[0])   
            
            except:
                    pass
            
            try:
                    pgcursor.execute("""select player_name
                    ,"points"
                    , sum(case 
                        when "points"::float is null then null
                        when ppmsim  > "points"::float  and "points" is not null then 1 
                        else 0
                    end)/10000 ::float as pointsover
                    ,"pts_rebs"
                    , sum(case 
                        when "pts_rebs" is null then null
                        when ppmsim + rebsim > "pts_rebs"::float and "pts_rebs" is not null then 1 
                        else 0
                    end ) /10000 ::float as prover
                    ,"pts_asts"
                    ,sum(case
                        when "pts_asts" is null then null
                        when ppmsim +apmsim > "pts_asts"::float and "pts_asts" is not null then 1 
                        else 0
                    end) /10000 ::float as paover
                    ,"pts_rebs_asts"
                    ,sum(case
                        when "pts_rebs_asts" is null then null
                        when ppmsim +apmsim +rebsim > "pts_rebs_asts"::float and "pts_rebs_asts" is not null then 1 
                        else 0
                    end)/10000 ::float as parover
                    ,"rebounds"
                    ,sum(case
                        when "rebounds" is null then null
                        when rebsim > "rebounds"::float and "rebounds" is not null then 1 
                        else 0
                    end) /10000 ::float as rebover
                    ,"assists"
                    ,sum(case
                        when "assists" is null then null
                        when apmsim > "assists"::float and "assists" is not null then 1 
                        else 0
                    end)/10000 ::float as astover
                    ,"rebs_asts"
                    ,sum(case
                        when "rebs_asts" is null then null
                        when rebsim +apmsim > "rebs_asts"::float and "rebs_asts" is not null then 1 
                        else 0
                    end)/10000 ::float as raover
                    ,"fantasy_points"
                    ,sum(case
                        when "fantasy_points" is null then null
                        when ppmsim  + (1.25 * rebsim ) + (1.5*apmsim ) + (2 * (bpmsim +  spmsim) ) > "fantasy_points"::float and "fantasy_points" is not null then 1 
                        else 0
                    end)/10000 ::float as fantover
                    ,"three_points_made"
                    ,sum(case
                        when "three_points_made" is null then null
                        when threessim > "three_points_made"::float and "three_points_made" is not null then 1 
                        else 0
                    end)/10000 ::float as threesover
                    from (select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * fgapmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as fgasim
                    , ((threespm * threespmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * threespmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as threessim
                    , ((ftpm * ftpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ftpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ftsim
                    , ((bpm * bpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * bpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as bpmsim
                    , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * rpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as rebsim
                    , ((apm * apmdef * homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * apmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as apmsim
                    , ((spm * spmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * spmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as spmsim
                    , ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ppmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ppmsim
                    from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
                    , case when dk."TeamAbbrev"= gm."Home_abb" then gm."Away_abb" 
                        when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                        end as opponent
                        ,usage_boost, reb_boost,ast_boost
                    from playerpermin pl
                    left join wowy w on pl.player_name = w."Name"
                    left join (select * from playerstdpermin) std on pl.player_name = std.player_name
                    left join draftkings dk on pl.player_name = dk."Name"
                    left join (select * from "Games") gm on dk."TeamAbbrev" = gm."Home_abb"
                    left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
                    where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                        when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                        end is not null
                    ) mai
                    left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
                    left join (select g."Home_abb"
                    ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
                    from "Games" g 
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pac on pac.team_abbreviation = g."Home_abb"
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pace on pace.team_abbreviation = g."Away_abb"
                    union all
                    select g."Away_abb"
                    , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
                    from "Games" g 
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pac on pac.team_abbreviation = g."Home_abb"
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
                    cross join (select i 
                    from generate_series(1,(6800)) i) seq
                    union all 
                    select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * fgapmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as fgasim
                    , ((threespm * threespmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * threespmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as threessim
                    , ((ftpm * ftpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ftpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ftsim
                    , ((bpm * bpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * bpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as bpmsim
                    , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * rpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as rebsim
                    , ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * apmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as apmsim
                    , ((spm * spmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * spmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as spmsim
                    , ((mai.ppm * ppmdef* homepacefactor *coalesce(usage_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ppmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ppmsim
                    from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
                    , case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                        when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                        end as opponent
                        ,usage_boost, reb_boost,ast_boost
                    from playerpermin pl
                    left join wowy w on pl.player_name = w."Name"
                    left join (select * from playerstdpermin) std on pl.player_name = std.player_name
                    left join draftkings dk on pl.player_name = dk."Name"
                    left join (select * from "Games") gm on dk."TeamAbbrev"= gm."Home_abb"
                    left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
                    where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                        when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                        end is not null) mai
                    left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
                    left join (select g."Home_abb"
                    ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
                    from "Games" g 
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pac on pac.team_abbreviation = g."Home_abb"
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pace on pace.team_abbreviation = g."Away_abb"
                    union all
                    select g."Away_abb"
                    , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
                    from "Games" g 
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pac on pac.team_abbreviation = g."Home_abb"
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
                    cross join (select i 
                    from generate_series(1,(2700)) i) seq
                    union all
                    select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * fgapmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as fgasim
                    , ((threespm * threespmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * threespmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as threessim
                    , ((ftpm * ftpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ftpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ftsim
                    , ((bpm * bpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * bpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as bpmsim
                    , ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1) ) + ((floor(random() * (300-(-300)+1) -300 ))/100 * rpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as rebsim
                    , ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * apmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as apmsim
                    , ((spm * spmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * spmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as spmsim
                    , ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ppmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ppmsim
                    from (select dk."min_y" as min_proj,pl.player_name,pl.playerposition,dk."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,pfdpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,pfdpmstd,minutesstd,ppmstd
                    , case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                        when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                        end as opponent
                        ,usage_boost, reb_boost,ast_boost
                    from playerpermin pl
                    left join wowy w on pl.player_name = w."Name"
                    left join (select * from playerstdpermin) std on pl.player_name = std.player_name
                    left join draftkings dk on pl.player_name = dk."Name"
                    left join (select * from "Games") gm on dk."TeamAbbrev" = gm."Home_abb"
                    left join (select * from "Games") gmt on dk."TeamAbbrev" = gmt."Away_abb"
                    where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                        when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
                        end is not null) mai
                    left join(select * from defpermin ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
                    left join (select g."Home_abb"
                    ,((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p )) /pac."PACE"  as homepacefactor
                    from "Games" g 
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pac on pac.team_abbreviation = g."Home_abb"
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pace on pace.team_abbreviation = g."Away_abb"
                    union all
                    select g."Away_abb"
                    , ((pac."PACE" - (select avg(p."PACE") as league_pace from pace p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from pace p )) + (select avg(p."PACE") as league_pace from pace p ))/ pace."PACE" as awaypacefactor
                    from "Games" g 
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pac on pac.team_abbreviation = g."Home_abb"
                    left join (
                    select tgl.team_abbreviation , p."PACE" from pace p
                    left outer join(select distinct t.team_name ,t.team_abbreviation  from totalgamelogs t)tgl  on p."TEAM_NAME" = tgl.team_name 
                    )pace on pace.team_abbreviation = g."Away_abb")pa on pa."Home_abb" = mai."TeamAbbrev"
                    cross join (select i 
                    from generate_series(1,(500	)) i) seq
                    order by player_name)sims
                    left join underdog ud on ud."name" = sims.player_name
                    where ud."name" is not null 
                    group by player_name,"points"
                    ,"pts_rebs","pts_asts"
                    ,"pts_rebs_asts"
                    ,"rebounds","assists"
                    ,"fantasy_points"
                    ,"rebs_asts"
                    ,"three_points_made"
                    order by player_name;""")
                
                
                    
                    udovers = pgcursor.fetchall()
                    udcolumn = []
                    for thing in pgcursor.description:
                            udcolumn.append(thing[0])   
            
            except:
                    pass
                
            try:
                    pgcursor.execute("""with overs as (select *
                    ,(pts + (1.2*reb)+(1.5*ast)+ (3*(stl+blk))- tov) as prizepicksfant
                    ,row_number() over (partition by t.player_name order by game_date desc )  game_number 
                    ,case
                        when p."3-PT Made" is null then 0
                        when p."3-PT Made" > fg3m then 0
                        when p."3-PT Made" < fg3m then 1
                    end as "3-PT Over"
                    ,case
                        when p."Assists" is null then 0
                        when p."Assists" > ast then 0
                        when p."Assists" < ast then 1
                    end as "Assists Over"
                    ,case
                        when p."Blks+Stls" is null then 0
                        when p."Blks+Stls" > (blk + stl) then 0
                        when p."Blks+Stls" < (blk + stl) then 1
                    end as "Blks+Stls Over"
                    ,case
                        when p."Fantasy Score" is null then 0
                        when p."Fantasy Score" > (pts + (1.2*reb)+(1.5*ast)+ (3*(stl+blk))- tov) then 0
                        when p."Fantasy Score" < (pts + (1.2*reb)+(1.5*ast)+ (3*(stl+blk))- tov) then 1
                    end as "Fantasy Score Over"
                    ,case
                        when p."Free Throws Made" is null then 0
                        when p."Free Throws Made" > ftm then 0
                        when p."Free Throws Made" < ftm then 1
                    end as "Free Throws Made Over"
                    ,case
                        when p."Points" is null then 0
                        when p."Points" > pts then 0
                        when p."Points" < pts then 1
                    end as "Points Over"
                    ,case
                        when p."Pts+Asts" is null then 0
                        when p."Pts+Asts" > pts + ast then 0
                        when p."Pts+Asts" < pts +ast then 1
                    end as "Pts+Asts Over"
                    ,case
                        when p."Pts+Rebs" is null then 0
                        when p."Pts+Rebs" > pts + reb then 0
                        when p."Pts+Rebs" < pts + reb then 1
                    end as "Pts+Rebs Over"
                    ,case
                        when p."Pts+Rebs+Asts" is null then 0
                        when p."Pts+Rebs+Asts" > pts +reb+ ast then 0
                        when p."Pts+Rebs+Asts" < pts +reb+ ast then 1
                    end as "Pts+Rebs+Asts Over"
                    ,case
                        when p."Rebounds" is null then 0
                        when p."Rebounds" > reb then 0
                        when p."Rebounds" < reb then 1
                    end as "Rebounds Over"
                    ,case
                        when p."Rebs+Asts" is null then 0
                        when p."Rebs+Asts" > reb+ ast then 0
                        when p."Rebs+Asts" < reb+ ast then 1
                    end as "Rebs+Asts Over"
                    ,case
                        when p."Turnovers" is null then 0
                        when p."Turnovers" > tov then 0
                        when p."Turnovers" < tov then 1
                    end as "Turnovers Over"
                    from totalgamelogs t 
                    left join prizepicks p on t.player_name = p."attributes.name")
                    select player_name,team_abbreviation
                    ,sum("3-PT Over") /10 ::float as threesover
                    , sum("Assists Over")/10 ::float as astover,sum("Blks+Stls Over")/10 ::float as stocksover,sum("Fantasy Score Over")/10 ::float as fantasyscoreover,sum("Free Throws Made Over")/10 ::float as ftmadeover,sum("Points Over")/10 ::float as pointsover,sum("Pts+Asts Over")/10 ::float as points_ast_over,sum("Pts+Rebs Over")/10 ::float as points_rebover,sum("Pts+Rebs+Asts Over")/10 ::float as praover
                    ,sum("Rebounds Over")/10 ::float as rebover,sum("Rebs+Asts Over")/10 ::float as reb_astover,sum("Turnovers Over")/10 ::float as tovover
                    from overs
                    where game_number <=10 and overs."attributes.name" is not null
                    group by player_name, team_abbreviation
                    order by player_name;""")
                
                
                    
                    pp10overs = pgcursor.fetchall()
                    pp10column = []
                    for thing in pgcursor.description:
                            pp10column.append(thing[0])   
            
            except:
                    pass        
                
            try:
                    pgcursor.execute("""with overs as (select *
                    ,(pts + (1.2*reb)+(1.5*ast)+ (3*(stl+blk))- tov) as prizepicksfant
                    ,row_number() over (partition by t.player_name order by game_date desc )  game_number 
                    ,case
                        when p."3-PT Made" is null then 0
                        when p."3-PT Made" > fg3m then 0
                        when p."3-PT Made" < fg3m then 1
                    end as "3-PT Over"
                    ,case
                        when p."Assists" is null then 0
                        when p."Assists" > ast then 0
                        when p."Assists" < ast then 1
                    end as "Assists Over"
                    ,case
                        when p."Blks+Stls" is null then 0
                        when p."Blks+Stls" > (blk + stl) then 0
                        when p."Blks+Stls" < (blk + stl) then 1
                    end as "Blks+Stls Over"
                    ,case
                        when p."Fantasy Score" is null then 0
                        when p."Fantasy Score" > (pts + (1.2*reb)+(1.5*ast)+ (3*(stl+blk))- tov) then 0
                        when p."Fantasy Score" < (pts + (1.2*reb)+(1.5*ast)+ (3*(stl+blk))- tov) then 1
                    end as "Fantasy Score Over"
                    ,case
                        when p."Free Throws Made" is null then 0
                        when p."Free Throws Made" > ftm then 0
                        when p."Free Throws Made" < ftm then 1
                    end as "Free Throws Made Over"
                    ,case
                        when p."Points" is null then 0
                        when p."Points" > pts then 0
                        when p."Points" < pts then 1
                    end as "Points Over"
                    ,case
                        when p."Pts+Asts" is null then 0
                        when p."Pts+Asts" > pts + ast then 0
                        when p."Pts+Asts" < pts +ast then 1
                    end as "Pts+Asts Over"
                    ,case
                        when p."Pts+Rebs" is null then 0
                        when p."Pts+Rebs" > pts + reb then 0
                        when p."Pts+Rebs" < pts + reb then 1
                    end as "Pts+Rebs Over"
                    ,case
                        when p."Pts+Rebs+Asts" is null then 0
                        when p."Pts+Rebs+Asts" > pts +reb+ ast then 0
                        when p."Pts+Rebs+Asts" < pts +reb+ ast then 1
                    end as "Pts+Rebs+Asts Over"
                    ,case
                        when p."Rebounds" is null then 0
                        when p."Rebounds" > reb then 0
                        when p."Rebounds" < reb then 1
                    end as "Rebounds Over"
                    ,case
                        when p."Rebs+Asts" is null then 0
                        when p."Rebs+Asts" > reb+ ast then 0
                        when p."Rebs+Asts" < reb+ ast then 1
                    end as "Rebs+Asts Over"
                    ,case
                        when p."Turnovers" is null then 0
                        when p."Turnovers" > tov then 0
                        when p."Turnovers" < tov then 1
                    end as "Turnovers Over"
                    from totalgamelogs t 
                    left join prizepicks p on t.player_name = p."attributes.name")
                    select player_name,team_abbreviation
                    ,sum("3-PT Over") /5 ::float as threesover
                    , sum("Assists Over")/5 ::float as astover,sum("Blks+Stls Over")/5 ::float as stocksover,sum("Fantasy Score Over")/5 ::float as fantasyscoreover,sum("Free Throws Made Over")/5 ::float as ftmadeover,sum("Points Over")/5 ::float as pointsover,sum("Pts+Asts Over")/5 ::float as points_ast_over,sum("Pts+Rebs Over")/5 ::float as points_rebover,sum("Pts+Rebs+Asts Over")/5 ::float as praover
                    ,sum("Rebounds Over")/5 ::float as rebover,sum("Rebs+Asts Over")/5 ::float as reb_astover,sum("Turnovers Over")/5 ::float as tovover
                    from overs
                    where game_number <=5 and overs."attributes.name" is not null
                    group by player_name, team_abbreviation
                    order by player_name;""")
                
                
                    
                    pp5overs = pgcursor.fetchall()
                    pp5column = []
                    for thing in pgcursor.description:
                            pp5column.append(thing[0])   
            
            except:
                    pass          
            
            
            try:
                    pgcursor.execute("""with overs as (select *
                    ,(pts + (1.2*reb)+(1.5*ast)+ (2*(stl+blk))- (.5*tov)) as udfant
                    ,row_number() over (partition by t.player_name order by game_date desc )  game_number 
                    ,case
                        when ud."three_points_made"::float is null then 0
                        when ud."three_points_made"::float > fg3m then 0
                        when ud."three_points_made"::float <= fg3m then 1
                    end as "3-PT Over"
                    ,case
                        when ud."assists"::float is null then 0
                        when ud."assists"::float > ast then 0
                        when ud."assists"::float <= ast then 1
                    end as "Assists Over"
                    --,case
                    --	when ud."blks_stls"::float is null then 0
                    --	when ud."blks_stls"::float > (blk + stl) then 0
                    --	when ud."blks_stls"::float < (blk + stl) then 1
                    --end as "Blks+Stls Over"
                    ,case
                        when ud."fantasy_points"::float is null then 0
                        when ud."fantasy_points"::float > (pts + (1.2*reb)+(1.5*ast)+ (2*(stl+blk))- (.5*tov)) then 0
                        when ud."fantasy_points"::float <= (pts + (1.2*reb)+(1.5*ast)+ (2*(stl+blk))- (.5*tov)) then 1
                    end as "Fantasy Score Over"
                    ,case
                        when ud."free_throws_made"::float is null then 0
                        when ud."free_throws_made"::float > ftm then 0
                        when ud."free_throws_made"::float <= ftm then 1
                    end as "Free Throws Made Over"
                    ,case
                        when ud."points"::float is null then 0
                        when ud."points"::float > pts then 0
                        when ud."points"::float <= pts then 1
                    end as "Points Over"
                    ,case
                        when ud."pts_asts"::float is null then 0
                        when ud."pts_asts"::float > pts + ast then 0
                        when ud."pts_asts"::float <= pts +ast then 1
                    end as "Pts+Asts Over"
                    ,case
                        when ud."pts_rebs"::float is null then 0
                        when ud."pts_rebs"::float > pts + reb then 0
                        when ud."pts_rebs"::float <= pts + reb then 1
                    end as "Pts+Rebs Over"
                    ,case
                        when ud."pts_rebs_asts"::float is null then 0
                        when ud."pts_rebs_asts"::float > pts +reb+ ast then 0
                        when ud."pts_rebs_asts"::float <= pts +reb+ ast then 1
                    end as "Pts+Rebs+Asts Over"
                    ,case
                        when ud."rebounds"::float is null then 0
                        when ud."rebounds"::float > reb then 0
                        when ud."rebounds"::float <= reb then 1
                    end as "Rebounds Over"
                    ,case
                        when ud."rebs_asts"::float is null then 0
                        when ud."rebs_asts"::float > reb+ ast then 0
                        when ud."rebs_asts"::float <= reb+ ast then 1
                    end as "Rebs+Asts Over"
                    from totalgamelogs t 
                    left join underdog ud on t.player_name = ud."name")
                    select player_name,team_abbreviation
                    ,sum("3-PT Over") /5 ::float as threesover
                    , sum("Assists Over")/5 ::float as astover,sum("Fantasy Score Over")/5 ::float as fantasyscoreover
                    ,sum("Free Throws Made Over")/5 ::float as ftmadeover
                    ,sum("Points Over")/5 ::float as pointsover
                    ,sum("Pts+Asts Over")/5 ::float as points_ast_over,sum("Pts+Rebs Over")/5 ::float as points_rebover
                    ,sum("Pts+Rebs+Asts Over")/5 ::float as praover
                    ,sum("Rebounds Over")/5 ::float as rebover
                    ,sum("Rebs+Asts Over")/5 ::float as reb_astover
                    --,sum("Blks+Stls Over")/5 ::float as blk_stlover
                    from overs
                    where game_number <=5 and overs."name" is not null
                    group by player_name, team_abbreviation
                    order by player_name;""")
                        
                
                    
                    ud5overs = pgcursor.fetchall()
                    ud5column = []
                    for thing in pgcursor.description:
                            ud5column.append(thing[0])   
            
            except:
                    pass  
            
            
            
            try:
                    pgcursor.execute("""with overs as (select *
                    ,(pts + (1.2*reb)+(1.5*ast)+ (2*(stl+blk))- (.5*tov)) as udfant
                    ,row_number() over (partition by t.player_name order by game_date desc )  game_number 
                    ,case
                        when ud."three_points_made"::float is null then 0
                        when ud."three_points_made"::float > fg3m then 0
                        when ud."three_points_made"::float <= fg3m then 1
                    end as "3-PT Over"
                    ,case
                        when ud."assists"::float is null then 0
                        when ud."assists"::float > ast then 0
                        when ud."assists"::float <= ast then 1
                    end as "Assists Over"
                    --,case
                    --	when ud."blks_stls" ::float is null then 0
                    --	when ud."blks_stls" ::float > (blk + stl) then 0
                    --	when ud."blks_stls" ::float < (blk + stl) then 1
                    --end as "Blks+Stls Over"
                    ,case
                        when ud."fantasy_points"::float is null then 0
                        when ud."fantasy_points"::float > (pts + (1.2*reb)+(1.5*ast)+ (2*(stl+blk))- (.5*tov)) then 0
                        when ud."fantasy_points"::float <= (pts + (1.2*reb)+(1.5*ast)+ (2*(stl+blk))- (.5*tov)) then 1
                    end as "Fantasy Score Over"
                    ,case
                        when ud."free_throws_made"::float is null then 0
                        when ud."free_throws_made"::float > ftm then 0
                        when ud."free_throws_made"::float <= ftm then 1
                    end as "Free Throws Made Over"
                    ,case
                        when ud."points"::float is null then 0
                        when ud."points"::float > pts then 0
                        when ud."points"::float <= pts then 1
                    end as "Points Over"
                    ,case
                        when ud."pts_asts"::float is null then 0
                        when ud."pts_asts"::float > pts + ast then 0
                        when ud."pts_asts"::float <= pts +ast then 1
                    end as "Pts+Asts Over"
                    ,case
                        when ud."pts_rebs"::float is null then 0
                        when ud."pts_rebs"::float > pts + reb then 0
                        when ud."pts_rebs"::float <= pts + reb then 1
                    end as "Pts+Rebs Over"
                    ,case
                        when ud."pts_rebs_asts"::float is null then 0
                        when ud."pts_rebs_asts"::float > pts +reb+ ast then 0
                        when ud."pts_rebs_asts"::float <= pts +reb+ ast then 1
                    end as "Pts+Rebs+Asts Over"
                    ,case
                        when ud."rebounds"::float is null then 0
                        when ud."rebounds"::float > reb then 0
                        when ud."rebounds"::float <= reb then 1
                    end as "Rebounds Over"
                    ,case
                        when ud."rebs_asts"::float is null then 0
                        when ud."rebs_asts"::float > reb+ ast then 0
                        when ud."rebs_asts"::float <= reb+ ast then 1
                    end as "Rebs+Asts Over"
                    from totalgamelogs t 
                    left join underdog ud on t.player_name = ud."name")
                    select player_name,team_abbreviation
                    ,sum("3-PT Over") /10 ::float as threesover
                    , sum("Assists Over")/10 ::float as astover
                    ,sum("Fantasy Score Over")/10 ::float as fantasyscoreover
                    ,sum("Free Throws Made Over")/10 ::float as ftmadeover
                    ,sum("Points Over")/10 ::float as pointsover
                    ,sum("Pts+Asts Over")/10 ::float as points_ast_over,sum("Pts+Rebs Over")/10 ::float as points_rebover
                    ,sum("Pts+Rebs+Asts Over")/10 ::float as praover
                    ,sum("Rebounds Over")/10 ::float as rebover
                    ,sum("Rebs+Asts Over")/10 ::float as reb_astover
                    --,sum("Blks+Stls Over")/10 ::float as blk_stlover
                    from overs
                    where game_number <=10 and overs."name" is not null
                    group by player_name, team_abbreviation
                    order by player_name;""")
                        
                
                    
                    ud10overs = pgcursor.fetchall()
                    ud10column = []
                    for thing in pgcursor.description:
                            ud10column.append(thing[0])   
            
            except:
                    pass          
            pgcursor.execute("""select pla.player_name
            , stddev_pop(dkfant) as dkstd 
            from playerlast_10 pla
            group by pla.player_name;""")
            
            
            std = pgcursor.fetchall()
            stdcolumn = []
            for thing in pgcursor.description:
                    stdcolumn.append(thing[0])   
            
            
            pgcursor.close()
            
            
            
            pgconn.close()
            
                
            df= pd.DataFrame(medians , columns =cols)
            df.to_csv('wnbamediansnew.csv')
            df.to_csv(r'C:\Users\trent\newwnba\wnbamediansnew.csv')
            df = df.fillna(0)
            
            try:
                    over = pd.DataFrame(overs, columns =column).fillna('')
                    over.to_csv('wnbaovers.csv',index =False)
                    # over.to_csv('newwnba/wnbaovers.csv')    
            except:
                    pass
            
            try:
                    overud = pd.DataFrame(udovers, columns =udcolumn).fillna('')
                    overud.to_csv('udwnbaovers.csv',index=False)
            
            except:
                    pass
                
            
            try:
                    pp5 = pd.DataFrame(pp5overs, columns =pp5column)
                    pp5.to_csv('PP Last 5.csv',index=False)
            
            except:
                    pass
            
            
            try:
                    pp10 = pd.DataFrame(pp10overs, columns =pp10column)
                    pp10.to_csv('PP Last 10.csv',index=False)
            
            except:
                    pass
            
            
            try:
                    ud5 = pd.DataFrame(ud5overs, columns =ud5column)
                    ud5.to_csv('UD Last 5.csv',index=False)
            
            except:
                    pass
            
            
            try:
                    ud10 = pd.DataFrame(ud10overs, columns =ud10column)
                    ud10.to_csv('UD Last 10.csv',index=False)
            
            except:
                    pass    
                
                
            standard = pd.DataFrame(std, columns =stdcolumn)
            
            medianload = gc.open_by_url('https://docs.google.com/spreadsheets/d/1q6JpRRdHLZB2-w8IO7BPIIDdVWWYBXPqYEVonZGEfuk/edit').get_worksheet(0)
            
            medianload.batch_clear(['A2:M250'])
            medianload.update('A2', df.values.tolist())
            
            
            pp5load = gc.open_by_url('https://docs.google.com/spreadsheets/d/1yCoJ88uBREbuPKe_s5pTg3m1MKznMLw4YjlKSHlkjho/edit').get_worksheet(0)
            pp10load = gc.open_by_url('https://docs.google.com/spreadsheets/d/1yCoJ88uBREbuPKe_s5pTg3m1MKznMLw4YjlKSHlkjho/edit').get_worksheet(1)
            
            try:
                    pp5load.batch_clear(['A1:Q250'])
                    pp5load.update('A2', pp5.values.tolist())
                    pp5load.update('A1', [pp5.columns.values.tolist()])
                    
                    pp10load.batch_clear(['A1:Q250'])
                    pp10load.update('A2', pp10.values.tolist())
                    pp10load.update('A1', [pp10.columns.values.tolist()])
            except:
                    pass
            ud5load = gc.open_by_url('https://docs.google.com/spreadsheets/d/1xI4gyjBn0XWO6v3q23FFzISs_aU047IRFUngYzlCSD0/edit').get_worksheet(0)
            ud10load = gc.open_by_url('https://docs.google.com/spreadsheets/d/1xI4gyjBn0XWO6v3q23FFzISs_aU047IRFUngYzlCSD0/edit#gid=0').get_worksheet(1)
            
            try:
                    ud5load.batch_clear(['A1:Q250'])
                    ud5load.update('A2', ud5.values.tolist())
                    ud5load.update('A1', [ud5.columns.values.tolist()])
            except:
                    pass
            
            try:
                    ud10load.batch_clear(['A1:Q250'])
                    ud10load.update('A2', ud10.values.tolist())
                    ud10load.update('A1', [ud10.columns.values.tolist()])
            except:
                    pass
            
            
            udpremload = gc.open_by_url('https://docs.google.com/spreadsheets/d/1iIMDBcp9u0sfz6vjgrY6eSaoX3Dh_Cx7H-cMj0wpPIQ/edit').get_worksheet(0)
            pppremload = gc.open_by_url('https://docs.google.com/spreadsheets/d/1e0ryQXY_WDw72QbOVm7bWXDZX9JKbxvtbnjRVzrewqE/edit').get_worksheet(0)
            
            try:
                    udpremload.batch_clear(['A1:u250'])
                    udpremload.update('A2', overud.values.tolist())
                    udpremload.update('A1', [overud.columns.values.tolist()])
            except:
                    pass
            
            try:
                    pppremload.batch_clear(['A1:u250'])
                    pppremload.update('A2', over.values.tolist())
                    pppremload.update('A1', [over.columns.values.tolist()])
            except:
                    pass
        



