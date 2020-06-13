import numpy as numpy
import pandas as pd

"""
TO DO: 
What about duplicates in get_pagedata
How to deal with connection for sqlalchemy
"""


def get_pagedata(type_of_data,pindex_list):
    """
    #Need list of what kind of pagedata we want (i.e. the key) and a list of all the pindexes we care about
    type_of_data should be a list
    """
    sql_pindex_list = str(tuple(pindex_list))#",".join(list(str(pindex) for pindex in pindex_list))
    #I assume the next part is just to get rid of dupes
    type_of_data = tuple(type_of_data)

    sql_sub = 'SELECT MAX(id) as mxid,date,pindex FROM pagedata WHERE `key` in "%s" AND pindex in (%s) GROUP BY date,pindex' % (type_of_data,sql_pindex_list)
    sql_in = 'WITH getmax AS (%s) SELECT pagedata.date,pagedata.pindex,pagedata.count, pagedata.`key` FROM pagedata JOIN getmax ON pagedata.id=getmax.mxid' % sql_sub
    #sql_sub = 'SELECT date, pindex, `count`, `key` FROM pagedata WHERE `key` in "%s" AND pindex in (%s)' % (type_of_data,sql_pindex_list) # DOES NOT ACCOUNT FOR DUPLICATES

    all_data_df = pd.read_sql(sql_in, connection)
    return all_data_df

def subtract_scroll_events(all_data_df):
    """
    Need a dataframe of all the pindexes and keys we care about (scroll events and pageviews)
    """
    grouped = all_data_df.groupby(['pindex', 'date'])
    final_pv = {}
    def func(group):
        scrolls = group[group['key'].isin(['scroll_events'])].shape[0]
        views = group[group['key'].isin(['page_views'])].shape[0]
        if (views==1)&(scrolls==1):
            net_pageviews = group[group['key']=='pageviews']['count'].iloc[0]-group[group['key']=='scroll_events']['count'].iloc[0]
        elif (views==1)&(scrolls==0):
            net_pageviews = group[group['key']=='pageviews']['count'].iloc[0]
        else:
            net_pageviews = np.nan
            print('error')
    net_pageviews = group.apply(lambda grp: func(grp)) #Pandas series with total counts with pindex and dates as indexes
    return net_pageviews

def get_searchterm_counts(search_phrases, search_type):
    """search_phrases should be a list of what needs to be search
        e.g. "Sauvignon blanc" and "Chile"
        search_type = 'impressions', 'clicks', or 'ctr'
    """
    query = ''
    for i in range(len(search_phrase)):
        query= query + f"sterm like %%{search_phrase[i]%%} "
    tot_counts = pd.read_sql(f"SELECT date, sum(`count`) FROM searchdata WHERE `key` = {search_type} AND {query} group by date") % search_phrase
    return search_counts_bydate

