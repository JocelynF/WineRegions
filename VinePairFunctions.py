import os,sys,re
import numpy as np
import pandas as pd
import datetime




def convert_csv_input(df, column):
    #convert inputs from strings or floats to lists of ints
    df[column]  = df[column].to_dict()
    for key, val in df[column].items():
        if isinstance(val, str):
            if val.isspace():
                new_val = []
            else:
                new_val = [int(i) for i in df[column][key].split(';')]
        elif np.isnan(val):
            new_val = []
        elif isinstance(val, float):
            new_val = [int(val)]
        elif isinstance(val, int):
            new_val = [val]
        else:
            #new_val = []
            print("ERROR", key, val)
        df[column][key]=new_val
    return df

def invert_dict(d): 
    inverse = dict() 
    for key in d: 
        # Go through the list that is saved in the dict:
        for item in d[key]:
            # Check if in the inverted dict the key exists
            if item not in inverse: 
                # If not create a new list
                inverse[item] = [key] 
            else: 
                invers[item].append(key)
    return inverse

def get_pageviews(connection, pindex_list, type_of_pageviews = "pageviews",subtract_scroll=True):
    """
    type of pageviews: pageviews or unique_pageviews
    pindex_list: list of pages
    subtract_scroll: false if you want raw pageviews, true if you want to subtract scroll events from pageviews
    """
    sql_pindex_tup = str(tuple(pindex_list))#",".join(list(str(pindex) for pindex in pindex_list))
    #query for all the pageviews
    sql_sub1 = f"(SELECT MAX(id) as mxid,date,pindex FROM pagedata WHERE `key` = '{type_of_pageviews}' AND pindex in {sql_pindex_tup} GROUP BY date,pindex)"
    sql_in1 = f"WITH getmax AS {sql_sub1} SELECT pagedata.date,pagedata.pindex,pagedata.count, pagedata.`key` FROM pagedata JOIN getmax ON pagedata.id=getmax.mxid;" 
    pageviews_df = pd.read_sql(sql_in1, connection, parse_dates={'date': '%Y-%m-%d'})
    pageviews_df = pageviews_df.rename(columns={"count": "pageviews_counts"})

    #get min and max dates to create an array with each page and date
    date_min = pd.to_datetime(pageviews_df['date'].min())
    date_max = pd.to_datetime(pageviews_df['date'].max())
    date_list = [date_min + datetime.timedelta(days=x) for x in range((date_max-date_min).days + 1)]

    #these are just dictionaries to make indexing easier later
    col2date = dict(list((array_di,date) for array_di,date in enumerate(date_list)))
    date2col = dict(list((date,array_di) for array_di,date in enumerate(date_list)))
    #This is the same as the dictionary outside of the function, maybe write a class
    row2page = dict(list((array_pi,pindex) for array_pi,pindex in enumerate(pindex_list))) 
    page2row = dict(list((pindex,array_pi) for array_pi,pindex in enumerate(pindex_list)))

    if subtract_scroll == True:
        #get all the scrollevents for the relevent pages
        sql_sub2 = f"(SELECT MAX(id) as mxid,date,pindex FROM pagedata WHERE `key` = 'scroll_events' AND pindex in {sql_pindex_tup} GROUP BY date,pindex)"
        sql_in2 = f"WITH getmax AS {sql_sub2} SELECT pagedata.date,pagedata.pindex,pagedata.count, pagedata.`key` FROM pagedata JOIN getmax ON pagedata.id=getmax.mxid;"
        scroll_events_df = pd.read_sql(sql_in2, connection, parse_dates={'date': '%Y-%m-%d'})
        scroll_events_df = scroll_events_df.rename(columns={"count": "scroll_counts"})  
        #merge the scroll events into the pageviews dataframe
        pageviews_df = pageviews_df.merge(scroll_events_df, on = ['pindex','date'], how = 'left')
        #subtract the scroll events from the page views assuming everything is 0 if a scroll_count is nan
        pageviews_df['net_views'] = pageviews_df['pageviews_counts'].sub(pageviews_df['scroll_counts'], fill_value=0)
    else:
        #net views are the raw pageviews if we don't subtract scroll events
        pageviews_df['net_views'] = pageviews_df['pageviews'].copy()

    netviews_array = np.zeros([len(pindex_list), len(date_list)])
    #group dataframe by the pages
    groups = pageviews_df.groupby('pindex')
    for pindex, group in groups:
        #array row is the pages
        row = page2row[pindex]
        #array columns are the dates
        columns = group['date'].apply(lambda x: date2col[x]).tolist()
        counts = group['net_views'].tolist()
        netviews_array[row,columns] = counts
    return netviews_array, date_list

#def get_searchterm_counts(search_phrases, search_type):