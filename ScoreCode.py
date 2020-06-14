
import numpy as np 
import pandas as pd 
import 

wp_tax = pd.read_sql("select * from wp_term_taxonomy;", dbconnect)
wp_terms = pd.read_sql("select * from wp_terms;", dbconnect)
wp_term_relat = pd.read_sql("select * from wp_term_relationships;", dbconnect)
post_info = make_query("select ID, post_date, post_name from wp_posts;")
post_info = post_info.rename(columns={'ID': 'object_id', 'post_name': 'post_slug'})
wp = wp_tax.merge(wp_terms, on = 'term_id', how = 'outer')  #term_taxonomy_id term_id taxonomy description parent count name slug term_group
wp_pageterms = wp_term_relat.merge(wp, on = 'term_taxonomy_id', how = 'outer')
wp_pageterms = wp_pageterms.merge(post_info, on = 'object_id', how = 'outer')
#wp_pageterms columns:
#object_id term_taxonomy_id term_order term_id taxonomy description parent count name slug term_group	post_date post_slug

#delete all dataframes except wp and wp_pageterms

#Importing Wine Indexes
wbs = True
post_tag = True
variety = True
review_goodfor = True

term_types = {'wbs_master_taxonomy_node_type': wbs, 'post_tag':post_tag, 'variety':variety,'review:goodfor':review_goodfor}

#import wine file with term_taxonomy_ids
tracked_wines = pd.read_csv('track_wines.csv', header = 0, index_col = 'Group Name')
wine_names = tracked_wines.index.tolist()
tracked_wines = tracked_wines.to_dict(orient='series')
#Convert values to lists
for col in term_types.keys():
    tracked_wines = convert_csv_input(tracked_wines,col)
del tracked_wines['Notes']

#Create dictionary with keys = wine type and values as list of indexes we care about
#This depends on the input of what kind of tags matter
wine_group_tdict = {}
for name in wine_names:
    wine_group_tdict[name] = []
    #Put in a check that at least one is true
    for key, val in term_types.items():
        if val == True:
            wine_group_tdict[name].extend(tracked_wines[key][name])


tindex_wine_dict = invert_dict(wine_group_tdict) #map indexes to the wine
all_wine_tindexes = set([value for values in wine_pages.values() for value in values])
wines = list(wine_group_tdict.keys())

#Importing Country Indexes: May need to change this
country_group_tdict = {}
for country_group, query in country_term_sql.items():
    #gets the indexes from the vinepair1 database
    country_group_tdict = get_tindexes(query)
    country_group_tdict[country_group] = get_tindexes(query)
tindex_country_dict = invert_dict(country_group_tdict) #map indexes to the wine
all_country_tindexes = set([value for values in country_pages.values() for value in values])
country_names = country_group_tdict.keys()

wine_pages = {}
for wine in wine_group_tlist.keys():
    wine_pages[wine] = set(wp_pageterms[wp_pageterms['term_taxonomy_id'].isin(wine_group_tlist[wine])]['object_id']) #object id is the page idea

country_pages = {}
for country in country_names:
    country_pages[wine] = set(wp_pageterms[wp_pageterms['term_taxonomy_id'].isin(country_group_tlist[wine])]['object_id'])

all_wine_pages = sorted(set([value for values in wine_pages.values() for value in values]))
all_country_pages = sorted(set([value for values in country_pages.values() for value in values]))



#Get Page Weights
#for all pages that reference specific wines, mark which pages reference which wines
#columns are all the wines
#rows are pages
wine_type_mat = np.zeros((len(all_wine_pages), len(wines), dtype=np.bool_) # listed as false by default
row2page = dict(list((mat_pi,pindex) for mat_pi,pindex in enumerate(all_wine_pages)))
page2row = dict(list((pindex,mat_pi) for mat_pi,pindex in enumerate(all_wine_pages)))
row2wine = dict(list((mat_wi,wine_ind) for mat_wi,wine_ind in enumerate(wines)))
wine2row = dict(list((wine_ind,mat_wi) for mat_wi,wine_ind in enumerate(wines)))
for wine, page in all_wine_pages.items():
    for term_id in wp_pageterms[wp_pageterms['object_id']==page]['term_taxonomy_id'].tolist():
        if term_id in tindex_wine_dict.keys():
            wine = tindex_wine_dict[term_id]
            for val in wine:
                col = wine2row[val]
                row = page2row[page]
                wine_type_mat[row,col] = True #if page has wine in it's tindex it's listed as True

#page weight just evenly divides pages by wines in them then divides and sqaures
#Ex: page tags chardonnay, pinot noir, and cab franc - would be weighted 1/3^2 for each of these wines
#Squaring is just so it weights the more targeted pages higher
row_vals = np.nansum(wine_type_mat, axis = 1)    
page_weights_wine = (wine_type_mat.T/row_vals).T #could square this to skew weighting better?
#pageweights are 0 if no value exists

wine_pageviews_df, date_list = get_pageviews(all_wine_pages, 'pageviews', True)
all_wine_weighted_netviews = np.zeros((len(date_list),len(all_wine_pages)))
for col in wine_type_mat:
    #column is the wine group (e.g. chardonnay, pinot noir), rows are the dates and values are the net pageviews
    all_wine_weight_netviews[:, col] = np.nansum((wine_pageviews_df.T*page_weights_wine[:,col]).T, axis = 0)


#probably better to make this a dictionary of matricies to make it easier to incorporate search terms
subgroup_netviews = {}
for wine_group, wine_pages in wine_pages.items():
    for country_group, country_pages in country_pages.items():
        subgroup_netviews[wine_group] = {}
        country_weighted_netviews = np.zeros((len(date_list),len(all_country_list)))
        page_weights = page_weight_wine.copy()
        page_overlaps= wine_pages & country_pages #intersection of the two sets
        page_rows = [page2row[page] for page in page_overlaps]
        mask=np.zeros((page_weights.shape[0],page_weights.shape[1]), dtype=np.bool_)
        mask[page_rows,:] = True
        page_weights = page_weights*mask #only want rows where the pages overlap
        country_weight_netviews = np.nansum((wine_pageviews_df.T*page_weights[:,col]).T, axis = 0)
        subgroup_newviews[wine_group][country_group] = country_weight_netviews


#CHECK UP TO THIS POINT


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
        else:
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
    sql_sub1 = f"(SELECT MAX(id) as mxid,date,pindex FROM pagedata WHERE `key` = {type_of_pageviews} AND pindex in {sql_pindex_tup} GROUP BY date,pindex)"
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
        scroll_events = pd.read_sql(sql_in2, connection, parse_dates={'date': '%Y-%m-%d'})
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
        query= query + f"sterm like %%{search_phrase[i]%%} "
    tot_counts = pd.read_sql(f"SELECT date, sum(`count`) FROM searchdata WHERE `key` = {search_type} AND {query} group by date") % search_phrase
    return search_counts_bydate







def create_indexmat():
    pindex_list = sorted(self.wp_pageterms['term_taxonomy_id'].unique())
    tindex_list = sorted(self.wp_pageterms['object_id'].unique())
    n_rows = len(pindex_list)
    n_col = len(tindex_list)
    index_mat = np.zeros((n_rows,n_col),dtype=np.bool_)
    i2p = dict(list((mat_pi,pindex) for mat_pi,pindex in enumerate(pindex_list)))
    p2i = dict(list((pindex,mat_pi) for mat_pi,pindex in enumerate(pindex_list)))
    i2t = dict(list((mat_ti,tindex) for mat_ti,tindex in enumerate(tindex_list)))
    t2i = dict(list((tindex,mat_ti) for mat_ti,tindex in enumerate(tindex_list)))
    for page in pindex_list:
        row = p2i[page]
        for term_id in self.wp_pageterms[self.wp_pageterms['object_id']==page]['term_taxonomy_id'].tolist():
            col = t2i[term_id]
            index_mat[row,col] = True
    return index_mat




def find_pindexes(tindexes, print_counts = False):
    pinds = pterms_long[pterms_long['term_int'].isin(tindexes.values)]
    pinds = pinds['pindex'].unique()
    if print_counts == True:
        print(len(pinds))
    return pinds

def wine_page_counts_indexes(tindexes):
    pind = find_pindexes(tindexes)
    query = "select * from pagedata where `key`='pageviews' and pindex in "+str(tuple(pind.tolist()), )
    wine_page = pd.read_sql(query, dbconnect)
    return wine_page

def wine_page_counts_indexes(tindexes):
    pind = find_pindexes(tindexes)
    query = "select * from pagedata where `key`='pageviews' and pindex in "+str(tuple(pind.tolist()), )
    wine_page = pd.read_sql(query, dbconnect)
    return wine_page