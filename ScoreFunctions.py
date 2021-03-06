import numpy as np
import pandas as pd
import datetime
from sklearn.ensemble import IsolationForest
from sklearn import preprocessing
import pdb

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
        #TODO: Put in a check that at least one is true
        for key, val in term_types.items():
            #e.g. key is term type such as post_tag, and val is whether it's true (included) or not.
            if val == True:
                if key == 'appellation':
                    #all_categories3 is dictionary of dictionaries
                    #app_key is the tags associated with the appelation of the region in question
                    app_key = all_categories3[key][names[i]]              
                    while len(app_key)!=0:
                        #Get all the children of the appellation
                        group_tdict[names[i]].extend(app_key)
                        app_key = list(set(wp_pageterms[wp_pageterms['parent'].isin(app_key)]['term_taxonomy_id']))
                else:
                    group_tdict[names[i]].extend(all_categories3[key][names[i]])
                    
        if ('level' in all_categories3.keys()):
            if (all_categories3['level'][names[i]]<max_level):
                children = all_categories1[all_categories1['parent']==names[i]].index.tolist()
                for child in children:
                    group_tdict[names[i]].extend(group_tdict[child])
    #tindex_dict = invert_dict(group_tdict) #map indexes to the group
    all_category_tindexes = set([int(val) for values in group_tdict.values() for val in values])
    category_pages_dict = {}
    for name in names:
        category_pages_dict[name] = set(wp_pageterms[wp_pageterms['term_taxonomy_id'].isin(group_tdict[name])&(wp_pageterms['object_id'].notnull())]['object_id'].astype(int)) #object id is the pages
    all_category_pages = sorted(set([int(value) for values in category_pages_dict.values() for value in values]))
    return group_tdict, category_pages_dict, all_category_pages


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
    sql_in1 = f"WITH getmax AS {sql_sub1} SELECT pagedata.date,pagedata.pindex,pagedata.count, pagedata.`key` FROM pagedata JOIN getmax ON pagedata.id=getmax.mxid;" 
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
    elif subtract_scroll == False:
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


def page_outliers(input_array, cutoff = 0.5, sigcut=10,hardcut=5000):
    """
    Test each individual series in each aggregation group agains the group total
    check for values above two cutoffs: 1) more than (cutoff) of daily 
    agg total, 2) more than (sigcut) stddev above timeseries values.

    Separate function to check outliers with respect to daily values
    """
    # log = NullLog()
    # error_handler = np.seterrcall(log)
    # np.seterr(all='log')

    array_to_flag = input_array.copy()
    #Need accurate date of page creation to calculate mean
    #This causes a warning with there is an empty row, but it returns fine. 
    row_means = np.nanmean(array_to_flag, axis = 1)
    row_std = np.nanstd(array_to_flag, axis = 1)
    row_sd_max = sigcut*row_std+row_means

    #any points in a row above sigcut*std plus mean is flagged
    with np.errstate(invalid='ignore'): #IMPORTANT: TURN THIS TO 'warn' WHEN TESTING
        horiz_spike = np.greater(array_to_flag, row_sd_max[:,None]) 
        hard_cut = np.greater(array_to_flag, hardcut)
    page_spikes = np.logical_and(horiz_spike,hard_cut)
    spike_index = np.nonzero(page_spikes)
    si_pairs = zip(list(spike_index[0]),list(spike_index[1]))
    daily_tots = np.nansum(array_to_flag, axis = 0)
    #replace outlier with the daily max for the group
    daily_max = daily_tots*cutoff
    #print(f'flagged {len(spike_index[0])} out of {array_to_flag.size} points as outliers.')

    num = 0
    for ri,ci in si_pairs:
        #print(array_to_flag[ri,ci])
        #Need this if statement because without it
        #the replaced value is sometimes higher than the original outlier
        if array_to_flag[ri,ci] > daily_max[ci]:
            num+=1
            #print(ri,ci)
            array_to_flag[ri,ci] = daily_max[ci]
    #print("Replaced %i out of %i flagged points" % (num, len(spike_index[0])))

    return page_spikes, array_to_flag

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


def iso_forest(input_series, outliers_fraction):
    """
    time series anomalies in series are detected using an Isolation Forest algorithm.
    Arguments:
        input_series: 1d numpy array, series should already have be cleaned such that values before the start
        of data collection are nan's not zeros.
        outliers_fraction: float. Percentage of outliers allowed in the sequence.
    """
    #create mask to deal with deleting nan rows.
    is_valid = np.isfinite(input_series) #T/F
    outlier_series = input_series[~np.isnan(input_series)]
    min_max_scaler= preprocessing.StandardScaler()
    series_scaled = min_max_scaler.fit_transform(outlier_series.reshape(-1, 1))

    model =  IsolationForest(contamination = outliers_fraction)
    model.fit(series_scaled)
    iso_results = model.predict(series_scaled)

    #convert to -1 to true and 1 to false
    iso_results = (iso_results==-1)
    
    full_results= np.full(len(is_valid), False)
    full_results[is_valid]=iso_results

    return full_results


def iso_forest_outliers(input_array, outliers_fraction=0.003, PAGE_LOWER_LIMIT=30, NUM_DAYS_LOWER_LIMIT = 60):
    """
    This function runs through all the rows in the array and finds the outliers
    using an Isolation Forest algorithm. 
    If the outlier is the first point in a series, it is set to np.nan
    Otherwise the outlier is the 
    input_array: 2d numpy array of floats and nans
    outliers_fraction: maximum fraction of points that can be labeled and outlier
    """
    outlier_array = input_array.copy()
    masked = 0
    for i in range(outlier_array.shape[0]):
        if ~np.isnan(outlier_array[i,:]).all():
            #only do outlier filtering if at least some of the pageviews are greater than 30
            condition1 = ~(outlier_array[i,:][~np.isnan(outlier_array[i,:])]<PAGE_LOWER_LIMIT).all()
            condition2 = len(outlier_array[i,:][~np.isnan(outlier_array[i,:])])>NUM_DAYS_LOWER_LIMIT
            if condition1 and condition2:
                min_val = 0#50#np.nanmean(outlier_array[i,:])+np.nanstd(outlier_array[i,:])
                outlier_mask = iso_forest(outlier_array[i,:], outliers_fraction)
                #pdb.set_trace()
                outlier_indexes = np.where(outlier_mask)[0]
                for index in outlier_indexes:
                    if (index==0)&(outlier_array[i,index]>min_val):
                        outlier_array[i,index]=np.nan
                        masked+=1
                    elif (outlier_array[i,index-1]==np.nan)&(outlier_array[i,index]>min_val):
                        outlier_array[i,index] = np.nan
                        masked+=1
                    elif (index==(outlier_array.shape[1]-1))&(outlier_array[i,index]>min_val):
                        outlier_array[i, index] = (outlier_array[i,index]+outlier_array[i,index-1])/2
                        masked+=1
                    elif (outlier_array[i,index]>min_val):
                        next_index = index+1
                        while (next_index in outlier_indexes)&(next_index<(outlier_array.shape[1]-1)):
                            next_index+=1
                        outlier_array[i, index] = (outlier_array[i,index-1]+outlier_array[i,next_index])/2
                        masked+=1
    #TO DO: also output outlier mask to mark points that were 
    return outlier_array


def send_to_pandas(subregion_name, index2region, date_list, sub_views_unfiltered, \
                sub_scores_unfiltered, sub_views_filtered, sub_scores_filtered):

    unfiltview = pd.DataFrame.from_dict(sub_views_unfiltered,orient = 'columns')
    unfiltview = unfiltview.rename(columns = index2region)
    unfiltview.loc[:,'DATE']=date_list
    unfiltview = unfiltview.set_index('DATE')

    filtview = pd.DataFrame.from_dict(sub_views_filtered,orient = 'columns')
    filtview = filtview.rename(columns=index2region)  
    filtview.loc[:,'DATE']=date_list
    filtview = filtview.set_index('DATE')

    #The scores need to be in json files to send to front-end
    unfiltscore = pd.DataFrame.from_dict(sub_scores_unfiltered,orient = 'columns')
    unfiltscore.loc[:,'DATE']=date_list
    unfiltscore = unfiltscore.rename(columns=index2region) 
    unfiltscore = unfiltscore.set_index('DATE')

    #The scores need to be in json files to send to front-end
    filtscore = pd.DataFrame.from_dict(sub_scores_filtered,orient = 'columns')
    filtscore.loc[:,'DATE']=date_list
    filtscore = filtscore.rename(columns=index2region) 
    filtscore = filtscore.set_index('DATE')

    return unfiltview, unfiltscore, filtview, filtscore

def get_sub_views(subregion_name, subregion_list, region_pages, page2index, date_list,\
                wine2index, wine_weights,wine_pageviews_array, filtered_pageviews, \
                wine_views_unfiltered, wine_views_filtered, parent_weights = None):

    #Matrix of Pages for Each Subregion

    sub_pages = {}
    if subregion_name == 'COUNTRY':
        for key in subregion_list:
            sub_pages[key] = region_pages[key].copy()
    else:
        for key in subregion_list:
            sub_pages[key] = region_pages[key].copy()
        all_sub_pages = set([int(value) for values in sub_pages.values() for value in values])
        sub_pages['NO_SUBREGION'] = region_pages[subregion_name] - all_sub_pages

    index2sub= dict(list((mat_si,f'{subregion_name}_{sub_ind}') for mat_si, sub_ind in enumerate(sub_pages.keys())))
    sub2index = dict(list((sub_ind,mat_si) for mat_si,sub_ind in enumerate(sub_pages.keys())))
    sub_mat = np.zeros((len(page2index), len(index2sub)), dtype=np.bool_) # listed as false by default
    for sub, pages in sub_pages.items():
        col = sub2index[sub]
        page_indexes= [page2index[page] for page in pages if page in page2index]
        #if page has region in it's tindex it's listed as True
        sub_mat[page_indexes, col] = True

    #SubRegion Page Weights
    with np.errstate(invalid='ignore'): #IMPORTANT: TURN THIS TO 'warn' WHEN TESTING
        row_vals = np.nansum(sub_mat, axis = 1)    
        page_weights_sub= (sub_mat.T/row_vals).T 

    if parent_weights is not None: 
        page_weights_sub = page_weights_sub*parent_weights
    

    sub_views_unfiltered = np.full((len(date_list),len(sub_pages.keys())), np.nan)
    sub_scores_unfiltered = np.full((len(date_list),len(sub_pages.keys())), np.nan)
    sub_views_filtered = np.full((len(date_list),len(sub_pages.keys())), np.nan)
    sub_scores_filtered = np.full((len(date_list),len(sub_pages.keys())), np.nan)
    temp_weights = (wine_weights*page_weights_sub.T).T
    for sub_group, s_pages in sub_pages.items():
        with np.errstate(invalid='ignore'): #IMPORTANT: TURN THIS TO 'warn' WHEN TESTING
            sub_col = sub2index[sub_group]

            sub_views_unfiltered[:, sub_col] = \
            np.nansum((wine_pageviews_array.T*temp_weights[:,sub_col]).T, axis = 0).T 

            sub_scores_unfiltered[:,sub_col] = \
            100*sub_views_unfiltered[:, sub_col]/wine_views_unfiltered

            sub_views_filtered[:, sub_col] = \
            np.nansum((filtered_pageviews.T*temp_weights[:,sub_col]).T, axis = 0).T 

            sub_scores_filtered[:,sub_col] = \
            100*sub_views_filtered[:, sub_col]/wine_views_filtered

    sub_views_unfiltered, sub_scores_unfiltered, sub_views_filtered, sub_scores_filtered = send_to_pandas( \
                    subregion_name, index2sub, date_list, sub_views_unfiltered, \
                    sub_scores_unfiltered, sub_views_filtered, sub_scores_filtered)


    return sub_views_unfiltered, sub_scores_unfiltered, sub_views_filtered, sub_scores_filtered, sub2index, page_weights_sub



