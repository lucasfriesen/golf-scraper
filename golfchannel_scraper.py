# -*- coding: utf-8 -*-
"""
Created on Wed Sep 12 19:35:47 2018

@author: lucas
"""

import os
import pandas as pd
from bs4 import BeautifulSoup
import requests
import time

def results_collect(years, path, tours):
    """
    Runs through tours, collect schedules and results for defined years, saves to output, creates master tour file for tours defined.
    Then combines all tours, cleans, saves and transposes data to round-specific observations.
    
    Params:
        years = list of years as integers (i.e. [2018, 2019]) - scraper will work for years 2003 to present
        path = directory to save within (i.e. C://.../Golf Data)
        TOUR = a list of tours to collect data for [PGA TOUR, Web.com Tour, LPGA Tour, European Tour] - as strings in a list
    
    Returns:
        pandas Dataframe of full, clean results dataset for all tours - for all events that have been collected in that directory
    """
    
    check_dirs(path)
    
    for TOUR in tours:
        
        ds = schedule_collect(years, path, TOUR)
    
        dr = tourn_collect(path, TOUR, ds)
    
        clean_results(path, TOUR, dr)

    dall = combine_tours(path)
    
    return dall


def schedule_collect(years, path, TOUR):
    """
    Collects schedule for defined tour/years, saves to path
    
    Params:
        years = list of years as integers
        path = directory to save within
        TOUR = tour to collect data for (PGA TOUR, Web.com Tour, LPGA Tour, European Tour) - as a string
    
    Returns:
        pandas Dataframe of tournament schedule and links, with year number
    """
    
    start_time = time.time()
    
    ds1 = pd.DataFrame(columns=['tourn', 'tourn_link', 'year_num'])
    
    link_extend = 'https://www.golfchannel.com'
    
    for year in years:
        
        tournament = []
        tourn_link = []
        year_num= []
    
        if TOUR == 'PGA TOUR':
            url = 'https://www.golfchannel.com/tours/pga-tour/?t=schedule&year=' + str(year)
        elif TOUR == 'Web.com Tour':
            url = 'https://www.golfchannel.com/tours/web-com-tour/?t=schedule&year=' + str(year)
        elif TOUR == 'LPGA Tour':
            url = 'https://www.golfchannel.com/tours/lpga/?t=schedule&year=' + str(year)
        elif TOUR == 'European Tour':
            url = 'https://www.golfchannel.com/tours/european-tour/?t=schedule&year=' + str(year)
        
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'lxml')
        table = soup.find('table', {'id': 'tourSchedule'})
        
        trs = table.find_all('tr')
        
        for tr in trs[1:]:
            
            year_num.append(str(year))
            
            try:
                a2 = tr.find('a')
                tourn_name = a2.text
                tournament.append(tourn_name)
            except:
                tournament.append('')
                
            try:
                a2 = tr.find('a')
                tourn_link.append(link_extend + a2['href'])
            except:
                tourn_link.append('')
            
            #print(tourn_name + ' ' + str(year) + ' COLLECTED')
            
        dict_sch = {'tourn': tournament, 'tourn_link': tourn_link, 'year_num': year_num}
        
        ds = pd.DataFrame(dict_sch)  
        
        ds.to_csv(path + 'schedules/' + TOUR + '/' + TOUR + ' Schedule_' + str(year) + '.csv', sep=',', index=False)

        ds1 = ds1.append(ds)
        
        del(ds)
        
        print(TOUR + ' ' + str(year) + ' COLLECTED')
    
    df_list = []
    for file in os.listdir(path + 'schedules/' + TOUR +'/'):
        if file.endswith('.csv'):
            df = pd.read_csv(path + 'schedules/' + TOUR + '/' + file, engine='python')
            df_list.append(df)
        
    ds_all = pd.concat(df_list)
        
    ds_all.to_csv(path + 'schedules/' + TOUR + ' Schedule_Full.csv', sep=',', index=False)

    del(ds_all)
    del(df_list)
    
    end_time = time.time()
    duration = float("{0:.2f}".format((end_time-start_time)))
    
    print(TOUR + ' schedule collected in {} seconds.'.format(duration))
    
    return ds1

    

def tourn_collect(path, TOUR, ds):
    """
    Collects tournament data as available from the schedule on golfchannel.com. 
    Specific the years, tour, path, and schedule dataset collecte from schedule_collect
    
    Params:

        path = directory to save within
        TOUR = tour to collect data for (PGA TOUR, Web.com Tour, LPGA Tour, European Tour) - as a string
        ds: input dataframe, collected from schedule_collect
    
    Returns:
        pandas Dataframe of tournament results for the years and tour selected
    """
    
    start_time_tour = time.time()
    
    _event = ds['tourn'].tolist()
    _link = ds['tourn_link'].tolist()
    _year = ds['year_num'].tolist()
    
    bad_event = []
    
    br_event = []
    br_year = []
    br_row_num = []
    br_first = []
    br_second = []
    br_len = []
    
    for EVENT, LINK, YEAR in zip(_event, _link, _year):
        
        start_time = time.time()
        
        event = []
        year_num = []
        url = []
        details = []
        finish = []
        player = []
        to_par = []
        round_one = []
        round_two = []
        round_three = []
        round_four = []
        round_five = []
        round_six = []
        total = []
    
        try:
            
            r = requests.get(LINK)
            soup = BeautifulSoup(r.text, 'lxml')
            
            try:
                d = soup.find('div', {'id':'tourteaser'})
            except:
                d = 'N/A'
                
            table = soup.find('table', {'id': 'fullLeaderboard'})
            
            body = table.find('tbody')
            trs = body.find_all('tr')
            
            num = 0
            
            for tr in trs:
                
                tds = tr.find_all('td')
                
                if len(tds) == 11:
                    event.append(EVENT)
                    url.append(LINK)
                    year_num.append(str(YEAR))
                    details.append(d.text)
                    
                    finish.append(tds[1].text)
                    player.append(tds[3].text.strip())
                    to_par.append(tds[4].text)
                    round_one.append(tds[7].text)
                    round_two.append(tds[8].text)
                    round_three.append(tds[9].text)
                    total.append(tds[10].text)
                    round_four.append('')
                    round_five.append('')
                    round_six.append('')
                    
                elif len(tds) == 12:
                    event.append(EVENT)
                    url.append(LINK)
                    year_num.append(str(YEAR))
                    details.append(d.text)
                    
                    finish.append(tds[1].text)
                    player.append(tds[3].text.strip())
                    to_par.append(tds[4].text)
                    round_one.append(tds[7].text)
                    round_two.append(tds[8].text)
                    round_three.append(tds[9].text)
                    round_four.append(tds[10].text)
                    total.append(tds[11].text)
                    round_five.append('')
                    round_six.append('')
                   
                elif len(tds) == 13:
                    event.append(EVENT)
                    url.append(LINK)
                    year_num.append(str(YEAR))
                    details.append(d.text)
                    
                    finish.append(tds[1].text)
                    player.append(tds[3].text.strip())
                    to_par.append(tds[4].text)
                    round_one.append(tds[7].text)
                    round_two.append(tds[8].text)
                    round_three.append(tds[9].text)
                    round_four.append(tds[10].text)
                    round_five.append(tds[11].text)
                    round_six.append('')
                    total.append(tds[12].text)
                    
                elif len(tds) == 14:
                    event.append(EVENT)
                    url.append(LINK)
                    year_num.append(str(YEAR))
                    details.append(d.text)
                    
                    finish.append(tds[1].text)
                    player.append(tds[3].text.strip())
                    to_par.append(tds[4].text)
                    round_one.append(tds[7].text)
                    round_two.append(tds[8].text)
                    round_three.append(tds[9].text)
                    round_four.append(tds[10].text)
                    round_five.append(tds[11].text)
                    round_six.append(tds[12].text)
                    total.append(tds[13].text)
                
                elif 12 > len(tds) >= 8:
                    event.append(EVENT)
                    url.append(LINK)
                    year_num.append(str(YEAR))
                    details.append(d.text)
                    
                    finish.append(tds[1].text)
                    player.append(tds[3].text.strip())
                    to_par.append(tds[4].text)
                    round_one.append('')
                    round_two.append('')
                    round_three.append('')
                    round_four.append('')
                    round_five.append('')
                    round_six.append('')
                    total.append('')
                    
                elif 0 <= len(tds) <= 1:
                    pass
    
                else:
                    br_event.append(EVENT)
                    br_year.append(str(YEAR))
                    br_row_num.append(str(num))
                    br_len.append(len(tds))
                    try:
                        br_first.append(tds[0])
                    except:
                        br_first.append('')
                    try:
                        br_second.append(tds[1])
                    except:
                        br_second.append('')
            
            dict_results = {'event': event,
                        'url': url,
                        'year_num': year_num,
                        'details': details,
                        'finish': finish,
                        'player': player,
                        'to_par': to_par,
                        'round_one': round_one,
                        'round_two': round_two,
                        'round_three': round_three,
                        'round_four': round_four,
                        'round_five': round_five,
                        'round_six': round_six,
                        'total': total}        
                
            dr = pd.DataFrame(dict_results)
            dr['tour'] = TOUR
            dr = dr[['tour', 'event', 'url', 'year_num', 'details', 'finish', 'player', 'to_par', 'round_one', 'round_two', 'round_three', 'round_four', 'round_five', 'round_six', 'total']]
            
            if len(dr)>0:
                dr.to_csv(path + 'results/' + TOUR + '/' + str(YEAR) + '_' + EVENT + '.csv', sep=',', index=False)

                end_time = time.time()
                duration = float("{0:.2f}".format((end_time-start_time)))
                print(EVENT + ' - ' + str(YEAR) + ' - collected in {} seconds.'.format(duration))
            else:
                end_time = time.time()
                duration = float("{0:.2f}".format((end_time-start_time)))
                print(EVENT + ' - ' + str(YEAR) + ' - RESULTS NOT AVAILABLE - {} seconds.'.format(duration))
        
        except:
            print(EVENT + ' - ' + str(YEAR) + ' - FAILED')
            bad_event.append(EVENT + ' - ' + LINK + ' - ' + str(YEAR))
    
    df_list = []
    for file in os.listdir(path + 'results/' + TOUR +'/'):
        if file.endswith('.csv'):
            df = pd.read_csv(path + 'results/' + TOUR + '/' + file, engine='python')
            df_list.append(df)
    
    dall = pd.concat(df_list)
    dall.to_csv(path + 'results/' + TOUR + ' Results_Raw.csv', index=False)
    
    end_time_tour = time.time()
    
    duration_tour = float("{0:.2f}".format((end_time_tour-start_time_tour)/60))
    
    print(TOUR + ' Results collected in {} minutes.'.format(duration_tour))
    
    return dall


def clean_results(path, TOUR, dm):
    """
    Cleans data from specific tour - input tour dataset with name of tour and path, cleans data and saves to master file.
    
    Params:
        path = directory to save within
        TOUR = tour to collect data for (PGA TOUR, Web.com Tour, LPGA Tour, European Tour) - as a string
        dm: input dataframe, collected from tourn_collect
    
    Returns:
        pandas Dataframe, cleaned with appropriate columns ready for usage
    """
    
    print('Cleaning {} full dataset'.format(TOUR))
    
    dm['details'] = dm['details'].str.strip()

    dm['to_par'] = dm['to_par'].replace(to_replace='E', value=0)
    
    dm.loc[dm['finish'] == "CUT", "missed_cut"] = "YES"
    dm.loc[dm['finish'] != "CUT", "missed_cut"] = "NO"
    
    dm['rank'] = dm['finish']
    dm['rank'] = dm['rank'].str.replace('T', '', regex=False)
    
    dm['last_name'], dm['first_name'] = dm['player'].str.split(',', 1).str
    dm['last_name'] = dm['last_name'].str.strip()
    
    dm['first_name'] = dm['first_name'].str.replace('*','', regex=False)
    dm['first_name'] = dm['first_name'].str.replace('(a)','', regex=False)
    dm['first_name'] = dm['first_name'].str.strip()
    
    dm['full_name']= dm['first_name'] + ' ' + dm['last_name']
    
    dm['course_par'] = dm['details'].str.split('Par:').str[1]
    dm['course_par'] = dm['course_par'].str.split('|').str[0]
    dm['course_par'] = dm['course_par'].str.strip()
    
    dm['defending_champ'] = dm['details'].str.split('Defending Champion:').str[1]
    dm['defending_champ'] = dm['defending_champ'].str.strip()
    
    dm['course_yardage'] = dm['details'].str.split('Yardage:').str[1]
    dm['course_yardage'] = dm['course_yardage'].str.split('Purse:').str[0]
    dm['course_yardage'] = dm['course_yardage'].str.strip()
    
    dm['purse'] = dm['details'].str.split('Purse:').str[1]
    dm['purse'] = dm['purse'].str.split('|').str[0]
    dm['purse'] = dm['purse'].str.strip()
    
    dm['date_course'] = dm['details'].str.split('|').str[0]
    dm['date'], dm['course'] = dm['date_course'].str.split('\n', 1).str
    dm['date'] = dm['date'].str.strip()
    dm['course'] = dm['course'].str.strip()
    
    cols = ['year_num', 'round_one', 'round_two', 'round_three', 'round_four', 'round_five', 'round_six', 'total', 'to_par', 'course_par', 'course_yardage', 'rank']
    dm[cols] = dm[cols].apply(pd.to_numeric, errors='coerce', axis=0)
    
    dm = dm[['tour', 'year_num', 'event', 'date', 'course','course_yardage', 'defending_champ', 'purse', 'finish', 'rank', 'full_name', 'first_name', 'last_name', 'to_par', 'total', 'course_par', 'round_one', 'round_two', 'round_three', 'round_four', 'round_five', 'round_six', 'missed_cut']]
    
    dm = dm.sort_values(['tour', 'year_num', 'event', 'rank', 'finish'], ascending=[1, 1, 1, 1, 1])
    
    dm = dm.reset_index()
    dm = dm[['tour', 'year_num', 'event', 'date', 'course','course_yardage', 'defending_champ', 'purse', 'finish', 'rank', 'full_name', 'first_name', 'last_name', 'to_par', 'total', 'course_par', 'round_one', 'round_two', 'round_three', 'round_four', 'round_five', 'round_six', 'missed_cut']]
    
    dm.to_csv(path + 'results/' + TOUR + ' Results.csv', index=False)
    
    print(TOUR + ' results cleaned and saved to file')


def combine_tours(path):
    """
    Combines the full datasets for each of the tours collected, saves it, cleans the data (round-specific) and saves that
    
    Params:
        path = directory to save within
        tours = list of tours to combine
    Returns:
        pandas Dataframe, cleaned with appropriate columns ready for usage
    """
    
    print('Combining all tour data and cleaning...')
    
    dm = pd.DataFrame(columns=['tour', 'year_num', 'event', 'date', 'course','course_yardage', 'defending_champ', 'purse', 'finish', 'rank', 'full_name', 'first_name', 'last_name', 'to_par', 'total', 'course_par', 'round_one', 'round_two', 'round_three', 'round_four', 'round_five', 'round_six', 'missed_cut'])
    
    tours = ['PGA TOUR', 'LPGA Tour', 'Web.com Tour', 'European Tour']
    for tour in tours:
        try:
            dtemp =  pd.read_csv(path + 'results/' + tour + ' Results.csv', engine='python')
            dm = dm.append(dtemp)
        except:
            pass
        
    dm = dm[['tour', 'year_num', 'event', 'date', 'course','course_yardage', 'defending_champ', 'purse', 'finish', 'rank', 'full_name', 'first_name', 'last_name', 'to_par', 'total', 'course_par', 'round_one', 'round_two', 'round_three', 'round_four', 'round_five', 'round_six', 'missed_cut']]
    
    dm.to_csv(path + '/results/TourResults_Full.csv', index=False)
    
    print('Full multi-tour dataset saved. \nClean and transpose to round-specific observations...')
    
    rounds = ['round_one', 'round_two', 'round_three', 'round_four', 'round_five', 'round_six']
    df = pd.DataFrame(columns=['tour', 'year_num', 'event', 'date', 'course', 'course_yardage', 'defending_champ', 'purse', 'rank', 'finish', 'full_name', 'first_name', 'last_name', 'to_par', 'total', 'course_par'])
    
    num = 0
    for x in rounds:
        temp_list = ['tour', 'year_num', 'event', 'date', 'course', 'course_yardage', 'defending_champ', 'purse', 'rank', 'finish', 'full_name', 'first_name', 'last_name', 'to_par', 'total', 'course_par', x]
        d_temp = dm[temp_list]
        num = num + 1
        d_temp = d_temp.rename(columns={x : 'round_score'})
        d_temp['round_num'] = num
        df = df.append(d_temp, sort=False)
    
    df = df[['tour', 'year_num', 'event', 'date', 'course', 'course_yardage',  'course_par', 'defending_champ', 'purse', 'rank', 'finish', 'full_name', 'first_name', 'last_name', 'round_num', 'round_score', 'to_par', 'total']]
    
    def fix_round(row):
        if row['round_score'] < 58:
            return None
        elif row['round_score'] > 105:
            return None
        else:
            return row['round_score']
        
    df['round_score'] = df.apply(fix_round, axis=1)
    df.dropna(subset=['round_score'], inplace=True)
    
    dg = pd.DataFrame(df.groupby(['tour', 'year_num', 'event', 'round_num'], axis=0, as_index=False)['round_score'].mean())
    dg = dg.rename(columns={'round_score': 'avg_round_score'})
    
    cols = ['year_num', 'to_par']
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce', axis=0)
    
    dr = pd.merge(df, dg, left_on=['tour', 'year_num', 'event', 'round_num'], right_on=['tour', 'year_num', 'event', 'round_num'], how='outer')
    
    dr['round_to_par'] = dr['round_score'] - dr['course_par']
    dr['strokes_gained_avg'] = dr['round_score'] - dr['avg_round_score']
    
    dr = dr.sort_values(['tour', 'year_num', 'event', 'rank', 'full_name', 'round_num', 'round_score'], ascending=[1, 1, 1, 1, 1, 1, 1]) 
    dr = dr.reset_index()
    
    dr = dr[['tour', 'year_num', 'event', 'date', 'course','course_yardage', 'course_par', 'defending_champ', 'purse', 'rank','finish', 'full_name', 'first_name', 'last_name', 'round_num',
             'round_score', 'to_par', 'total', 'avg_round_score', 'round_to_par','strokes_gained_avg']]
    
    dr.to_csv(path + 'results/TourResults_ByRound.csv', index=False)
    
    print('Round specific dataset created, sorted, and saved to file. Strokes gained added to dataset')
    
    return dr
        
def check_dirs(path):
    """
    Creates directories required for the full collection process.
    
    Params:
        path = main directory
    Returns:
        None
    """
    
    print('Checking for directories within path - creating folders if not existing...')
    folders = ['results', 'schedules', 'results/PGA TOUR', 'results/LPGA Tour', 'results/Web.com Tour', 'results/European Tour',  'schedules/PGA TOUR', 'schedules/LPGA Tour', 'schedules/Web.com Tour', 'schedules/European Tour']
    
    for folder in folders:
        directory = path + folder
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(directory + ' - CREATED')
        else:
            print(directory + ' - EXISTS')
    
    print('Directories created/ready...')

    