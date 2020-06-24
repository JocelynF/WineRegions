import numpy as np
import pandas as pd
import datetime




def get_page_indexes(all_categories_filename, term_types, wp_pageterms):
    #import file with groups and term_taxonomy_ids in columns
    all_categories1 = pd.read_csv(all_categories_filename, header = 0, index_col = 'Group Name')
    all_categories2 = all_categories1.to_dict(orient='series')
    #Convert values to lists
    for col in term_types.keys():
        all_categories3 = convert_csv_input(all_categories2,col)
    #Create dictionary with keys = group type and values as list of indexes we care about
    #This depends on the input of what kind of tags matter
        
    if 'level' in all_categories3.keys():
        max_level = all_categories3['level'].max()  
        names = all_categories3['level'].sort_values(ascending=False).index.tolist()
    else:
        names = sorted(all_categories1.index.tolist()) 
    group_tdict = {}

    for i in range(len(names)):
        group_tdict[names[i]] = []
        #Put in a check that at least one is true
        for key, val in term_types.items():
            if val == True:
                if key == 'appellation':
                    app_key = all_categories3[key][names[i]]              
                    while len(app_key)!=0:
                        group_tdict[names[i]].extend(app_key)
                        app_key = list(set(wp_pageterms[wp_pageterms['parent'].isin(app_key)]['term_taxonomy_id']))
                else:
                    group_tdict[names[i]].extend(all_categories3[key][names[i]])
                    
        if ('level' in all_categories3.keys()):
            if (all_categories3['level'][names[i]]<max_level):
                children = all_categories1[all_categories1['parent']==names[i]].index.tolist()
                for child in children:
                    group_tdict[names[i]].extend(all_categories3[key][child])
    #tindex_dict = invert_dict(group_tdict) #map indexes to the group
    all_category_tindexes = set([int(val) for values in group_tdict.values() for val in values])
    category_pages_dict = {}
    for name in names:
        category_pages_dict[name] = set(wp_pageterms[wp_pageterms['term_taxonomy_id'].isin(group_tdict[name])&(wp_pageterms['object_id'].notnull())]['object_id']) #object id is the pages
    all_category_pages = sorted(set([int(value) for values in category_pages_dict.values() for value in values]))
    return category_pages_dict, all_category_pages


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

def get_pageviews(connection, pindex_list, row2page, page2row, type_of_pageviews = "pageviews",subtract_scroll=True):
    """
    type of pageviews: pageviews or unique_pageviews
    pindex_list: list of pages
    subtract_scroll: false if you want raw pageviews, true if you want to subtract scroll events from pageviews
    Only selects page views after 2016 to eliminate the weirdness before 2016
    """
    sql_pindex_tup = str(tuple(pindex_list))#",".join(list(str(pindex) for pindex in pindex_list))
    #query for all the pageviews
    sql_sub1 = f"(SELECT MAX(id) as mxid,date,pindex FROM pagedata WHERE `key` = '{type_of_pageviews}' AND pindex in {sql_pindex_tup} GROUP BY date,pindex)"
    sql_in1 = f"WITH getmax AS {sql_sub1} SELECT pagedata.date,pagedata.pindex,pagedata.count, pagedata.`key` FROM pagedata  JOIN getmax ON pagedata.id=getmax.mxid;" 
    pageviews_df = pd.read_sql(sql_in1, connection, parse_dates={'date': '%Y-%m-%d'})
    pageviews_df = pageviews_df[pageviews_df['date']>=datetime.datetime(2016,1,1)]
    pageviews_df = pageviews_df.rename(columns={"count": "pageviews_counts"})

    #get min and max dates to create an array with each page and date
    date_min = pd.to_datetime(pageviews_df['date'].min())
    date_max = pd.to_datetime(pageviews_df['date'].max())
    date_list = [date_min + datetime.timedelta(days=x) for x in range((date_max-date_min).days + 1)]

    #these are just dictionaries to make indexing easier later
    date2col = dict(list((date,array_di) for array_di,date in enumerate(date_list)))

    if subtract_scroll == True:
        #get all the scrollevents for the relevent pages
        sql_sub2 = f"(SELECT MAX(id) as mxid,date,pindex FROM pagedata WHERE `key` = 'scroll_events' AND pindex in {sql_pindex_tup}  GROUP BY date,pindex)"
        sql_in2 = f"WITH getmax AS {sql_sub2} SELECT pagedata.date,pagedata.pindex,pagedata.count, pagedata.`key` FROM pagedata  JOIN getmax ON pagedata.id=getmax.mxid;"
        scroll_events_df = pd.read_sql(sql_in2, connection, parse_dates={'date': '%Y-%m-%d'})
        scroll_events_df = scroll_events_df[scroll_events_df['date']>datetime.datetime(2016,1,1)]
        scroll_events_df = scroll_events_df.rename(columns={"count": "scroll_counts"})  
        #merge the scroll events into the pageviews dataframe
        pageviews_df = pageviews_df.merge(scroll_events_df, on = ['pindex','date'], how = 'left')

        #subtract the scroll events from the page views assuming everything is 0 if a scroll_count is nan
        pageviews_df['net_views'] = pageviews_df['pageviews_counts'].subtract(pageviews_df['scroll_counts'], fill_value=0)
    else:
        #net views are the raw pageviews if we don't subtract scroll events
        pageviews_df['net_views'] = pageviews_df['pageviews_counts'].copy()

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

def page_outliers(array_to_flag, cutoff = 0.5, sigcut=10,hardcut=5000):
    """
    Test each individual series in each aggregation group agains the group total
    check for values above two cutoffs: 1) more than (cutoff) of daily 
    agg total, 2) more than (sigcut) stddev above timeseries values.

    Separate function to check outliers with respect to daily values
    """
    print("Performing Spike Detection")
    #outlier_subs = np.zeros(array_to_flag.shape)
    # log = NullLog()
    # error_handler = np.seterrcall(log)
    # np.seterr(all='log')

    
    array_to_flag = array_to_flag.copy()
    #Need accurate date of page creation to calculate mean

    array_to_flag[np.cumsum(array_to_flag, axis = 1)==0]=np.nan
    row_means = np.nanmean(array_to_flag, axis = 1)
    row_std = np.nanstd(array_to_flag, axis = 1)
    row_sd_max = sigcut*row_std +row_means

    #any points in a row above sigcut*std plus mean is flagged
    horiz_spike = np.greater(array_to_flag, row_sd_max[:,None]) 
    #must also be above the hard cut
    hard_cut = array_to_flag > hardcut
    page_spike = np.logical_and(horiz_spike,hard_cut)
    spike_index = np.nonzero(page_spike)
    si_pairs = zip(list(spike_index[0]),list(spike_index[1]))
    daily_tots = np.nansum(array_to_flag, axis = 0)
    #replace outlier with the daily max for the group
    daily_max = daily_tots*cutoff
    print("   flagged %i out of %i points as outliers" % (len(spike_index[0]),array_to_flag.shape[1]*array_to_flag.shape[0]))
    num = 0
    for ri,ci in si_pairs:
        #print(array_to_flag[ri,ci])
        #Need this if statement because without it
        #the replaced value is sometimes higher than the original outlier
        if array_to_flag[ri,ci] > daily_max[ci]:
            num+=1
            print(ri,ci)
            array_to_flag[ri,ci] = daily_max[ci]
    print("   Replaced %i out of %i flagged points" % (num, len(spike_index[0])))
    return page_spike, array_to_flag

    # vert_spikes = {}
    # for group, rows in group_rows.items():
    #     dict([sub_row, row for sub_row, row in enumerate()])
    #     daily_tots = np.nansum(array_to_flag[rows,:], axis = 0)
    #     daily_max = daily_tots*cutoff
    #     hard_cut = array_to_flat[rows,:] > hardcut
    #     vert_spike = np.greater(array_to_flag[rows,:],daily_max[:]) #do I need the None?
    #     group_spike = np.logical_and(horiz_spike,np.logical_and(vert_spike,hard_cut))
    #     spike_index = np.nonzero(group_spike)
    #     si_pairs = []
    #     for ind, val in enumerate(spike_index[0]):
    #         if val in rows:
    #             row = val
    #             col = spike_index[1][ind]
    #             outlier_subs[(row, col)] = daily_max[col]
    # print("   flagged %i out of %i points as outliers" % (np.count_nonzero(outlier_subs),outlier_subs.shape[0]*outlier_subs.shape[1]))
    # return out