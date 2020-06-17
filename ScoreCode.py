
import numpy as np 
import pandas as pd 
import datetime
import sqlalchemy as db
import time

sql_connect =  'mysql+pymysql://root:harvey@127.0.0.1:3306/wordpress'
engine = db.create_engine(sql_connect)#, echo = True)
dbconnect = engine.connect()

sql_connect1  =  'mysql+pymysql://root:harvey@127.0.0.1:3306/vinepair1'
engine1 = db.create_engine(sql_connect1)#, echo = True)
vinepair_connect = engine1.connect()



def convert_csv_input(df, column):
    #convert inputs from strings or floats to lists of ints
    df[column]  = df[column].to_dict()
    for key, val in df[column].items():
        if isinstance(val, str):
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

def get_pageviews(pindex_list, type_of_pageviews = "pageviews",subtract_scroll=True):
    """
    type of pageviews: pageviews or unique_pageviews
    pindex_list: list of pages
    subtract_scroll: false if you want raw pageviews, true if you want to subtract scroll events from pageviews
    """
    sql_pindex_tup = str(tuple(pindex_list))#",".join(list(str(pindex) for pindex in pindex_list))
    #query for all the pageviews
    sql_sub1 = f"(SELECT MAX(id) as mxid,date,pindex FROM pagedata WHERE `key` = '{type_of_pageviews}' AND pindex in {sql_pindex_tup} GROUP BY date,pindex)"
    sql_in1 = f"WITH getmax AS {sql_sub1} SELECT pagedata.date,pagedata.pindex,pagedata.count, pagedata.`key` FROM pagedata JOIN getmax ON pagedata.id=getmax.mxid;" 
    pageviews_df = pd.read_sql(sql_in1, vinepair_connect, parse_dates={'date': '%Y-%m-%d'})
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
        scroll_events_df = pd.read_sql(sql_in2, vinepair_connect, parse_dates={'date': '%Y-%m-%d'})
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

def get_searchterm_counts(search_phrases, search_type):
    """search_phrases should be a list of what needs to be search
        e.g. "Sauvignon blanc" and "Chile"
        search_type = 'impressions', 'clicks', or 'ctr'
    """
    query = ''
    for i in range(len(search_phrase)):
        query= query + f"sterm like %%{search_phrase[i]}%% "
    tot_counts = pd.read_sql(f"SELECT date, sum(`count`) FROM searchdata WHERE `key` = {search_type} AND {query} group by date")
    return search_counts_bydate

wp_tax = pd.read_sql("select * from wp_term_taxonomy;", dbconnect)
wp_terms = pd.read_sql("select * from wp_terms;", dbconnect)
wp_term_relat = pd.read_sql("select * from wp_term_relationships;", dbconnect)
post_info = pd.read_sql("select ID, post_date, post_name from wp_posts;", dbconnect)
post_info = post_info.rename(columns={'ID': 'object_id', 'post_name': 'post_slug'})
wp = wp_tax.merge(wp_terms, on = 'term_id', how = 'outer')  #term_taxonomy_id term_id taxonomy description parent count name slug term_group
wp_pageterms = wp_term_relat.merge(wp, on = 'term_taxonomy_id', how = 'outer')
wp_pageterms = wp_pageterms.merge(post_info, on = 'object_id', how = 'outer')
#wp_pageterms columns:
#object_id term_taxonomy_id term_order term_id taxonomy description parent count name slug term_group	post_date post_slug

#delete all dataframes except wp and wp_pageterms

#Importing Wine Indexes
wine_wbs = True
wine_post_tag = True
wine_variety = True
wine_review_goodfor = True

country_appellation = True
country_post_tag = True


start = time.time()


wine_term_types = {'wbs_master_taxonomy_node_type': wine_wbs, 'post_tag':wine_post_tag, 'variety':wine_variety,'review:goodfor':wine_review_goodfor}
#import wine file with term_taxonomy_ids
tracked_wines = pd.read_csv('./track_wines.csv', header = 0, index_col = 'Group Name')
wine_names = sorted(tracked_wines.index.tolist())
print('List of Wines: ', wine_names)
tracked_wines = tracked_wines.to_dict(orient='series')
#Convert values to lists
for col in wine_term_types.keys():
    tracked_wines = convert_csv_input(tracked_wines,col)
#Create dictionary with keys = wine type and values as list of indexes we care about
#This depends on the input of what kind of tags matter
wine_group_tdict = {}
for name in wine_names:
    wine_group_tdict[name] = []
    #Put in a check that at least one is true
    for key, val in wine_term_types.items():
        if val == True:
            wine_group_tdict[name].extend(tracked_wines[key][name])
tindex_wine_dict = invert_dict(wine_group_tdict) #map indexes to the wine
all_wine_tindexes = set([int(val) for values in wine_group_tdict.values() for val in values])


country_term_types = {'appellation': country_appellation, 'post_tag':country_post_tag}
tracked_countries = pd.read_csv('./track_countries.csv', header = 0, index_col = 'Group Name')
country_names = sorted(tracked_countries.index.tolist())
print('List of Countries: ', country_names)
tracked_countries = tracked_countries.to_dict(orient='series')

#Convert values to lists
for col in country_term_types.keys():
    tracked_countries = convert_csv_input(tracked_countries,col)
#Create dictionary with keys = country and values as list of indexes we care about
#This depends on the input of what kind of tags matter
country_group_tdict = {}
for name in country_names:
    country_group_tdict[name] = []
    #Put in a check that at least one is true
    for key, val in country_term_types.items():
        if val == True:
            country_group_tdict[name].extend(tracked_countries[key][name])
tindex_country_dict = invert_dict(country_group_tdict) #map indexes to the wine
all_country_tindexes = set([int(val) for values in country_group_tdict.values() for val in values])

wine_pages = {}
for wine in wine_names:
    wine_pages[wine] = set(wp_pageterms[wp_pageterms['term_taxonomy_id'].isin(wine_group_tdict[wine])&(wp_pageterms['object_id'].notnull())]['object_id']) #object id is the page idea
all_wine_pages = sorted(set([int(value) for values in wine_pages.values() for value in values]))

country_pages = {}
for country in country_names:
    country_pages[country] = set(wp_pageterms[wp_pageterms['term_taxonomy_id'].isin(country_group_tdict[country])&(wp_pageterms['object_id'].notnull())]['object_id'])
all_country_pages = sorted(set([int(value) for values in country_pages.values() for value in values]))


#Get Page Weights
#for all pages that reference specific wines, mark which pages reference which wines
#columns are all the wines
#rows are pages
wine_type_mat = np.zeros((len(all_wine_pages), len(wine_names)), dtype=np.bool_) # listed as false by default
row2page = dict(list((mat_pi,pindex) for mat_pi,pindex in enumerate(all_wine_pages)))
page2row = dict(list((pindex,mat_pi) for mat_pi,pindex in enumerate(all_wine_pages)))
row2wine = dict(list((mat_wi,wine_ind) for mat_wi,wine_ind in enumerate(wine_names)))
wine2row = dict(list((wine_ind,mat_wi) for mat_wi,wine_ind in enumerate(wine_names)))
for wine, pages in wine_pages.items():
    col = wine2row[wine]
    page_rows= [page2row[page] for page in pages]
    #if page has wine in it's tindex it's listed as True
    wine_type_mat[page_rows, col] = True

#page weight just evenly divides pages by wines in them then divides and sqaures
#Ex: page tags chardonnay, pinot noir, and cab franc - would be weighted 1/3^2 for each of these wines
#Squaring is just so it weights the more targeted pages higher
row_vals = np.nansum(wine_type_mat, axis = 1)    
page_weights_wine = (wine_type_mat.T/row_vals).T #could square this to skew weighting better?
#pageweights are 0 if no value exists

wine_pageviews_df, date_list = get_pageviews(all_wine_pages, 'pageviews', True)
all_wine_weighted_netviews = np.zeros((len(date_list),len(wine_names)))
for col in range(page_weights_wine.shape[1]):
    #column is the wine group (e.g. chardonnay, pinot noir), rows are the dates and values are the net pageviews
    all_wine_weighted_netviews[:, col] = np.nansum((wine_pageviews_df.T*page_weights_wine[:,col]).T, axis = 0).T


#probably better to make this a dictionary of matricies to make it easier to incorporate search terms
subgroup_netviews = {}
for wine_group, w_pages in wine_pages.items():
    for country_group, c_pages in country_pages.items():
        subgroup_netviews[wine_group] = {}
        country_weighted_netviews = np.zeros((len(date_list),len(all_country_pages)))
        page_weights = page_weights_wine.copy()
        page_overlaps= w_pages & c_pages #intersection of the two sets
        page_rows = [page2row[page] for page in page_overlaps]
        mask=np.zeros((page_weights.shape[0],page_weights.shape[1]), dtype=np.bool_)
        mask[page_rows,:] = True
        page_weights = page_weights*mask #only want rows where the pages overlap
        country_weight_netviews = np.nansum((wine_pageviews_df.T*page_weights[:,col]).T, axis = 0)
        subgroup_netviews[wine_group][country_group] = country_weight_netviews

stop = time.time()
print('time: ', stop-start)

#save to csv
wine_column_names = [row2wine[i] for i in range(all_wine_weighted_netviews.shape[1])]
pd.DataFrame(all_wine_weighted_netviews, index = date_list, column = wine_column_names].to_csv('WeightedWineViewsTest.csv') 

with open('RegionalWineViews.json', 'w') as json_file:
  json.dump(subgroup_netviews, json_file)

#views = views.rename(columns = {'Unnamed: 0':'DATE'})


#CHECK UP TO THIS POINT
