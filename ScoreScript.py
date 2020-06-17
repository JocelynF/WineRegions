import numpy as np 
import pandas as pd 
import datetime
import sqlalchemy as db
import time
import VinePairFunctions as vf
import logging
import os
## ------------START USER INPUT -------------------------------

#Where to log info
os.remove("RegionalWines.log")
logging.basicConfig(filename='RegionalWines.log',level=logging.DEBUG)

#THESE NEED TO BE INCLUDED IN THE TRACKED_WINES.CSV FILE
track_wines = ['CABERNET SAUVIGNON',
                'PINOT NOIR',
                'MALBEC',
                'MERLOT',
                'MOSCATO',
                'PINOT GRIGIO/PINOT GRIS',
                'RED BLEND',
                'RIELSING',
                'ROSE',
                'SAUVIGNON BLANC',
                'SYRAH/SHIRAZ']

#THESE NEED TO BE INCLUDED IN THE TRACKED_REGIONS.CSV FILE
track_countries = ['ARGENTINA',
                    'AUSTRALIA',
                    'AUSTRIA',
                    'CHILE',
                    'ENGLAND',
                    'FRANCE',
                    'GERMANY',
                    'GREECE',
                    'ISRAEL',
                    'ITALY',
                    'NEW ZEALAND',
                    'PORTUGAL',
                    'SOUTH AFRICA',
                    'SPAIN',
                    'UNITED STATES']

#THESE NEED TO BE INCLUDED IN THE TRACKED_REGIONS.CSV FILE
track_regions = ['BEAUJOLAIS',
                'BORDEAUX',
                'BURGUNDY',
                'CALIFORNIA',
                'COTES DU RHONE',
                'LOIRE VALLEY',
                'MONTEPULCIANO',
                'NEW YORK', 
                'OREGON',
                'PIEDMONT',
                'PROVENCE',
                'RHONE VALLEY',
                'RIOJA',
                'RUEDA', 
                'SICILY',
                'TUSCANY',
                'VIRGINIA',
                'WASHINGTON', 
                'SONOMA',
                'NAPA']


#WHAT KIND OF TAXONOMY WOULD YOU LIKE TO INCLUDE
wine_wbs = True
wine_post_tag = True
wine_variety = True
wine_review_goodfor = True

region_appellation = True 
region_post_tag = True

#----------------------END USER INPUT------------------------------------


sql_connect =  'mysql+pymysql://root:harvey@127.0.0.1:3306/wordpress'
engine = db.create_engine(sql_connect)#, echo = True)
wordpress_connect = engine.connect()

sql_connect1  =  'mysql+pymysql://root:harvey@127.0.0.1:3306/vinepair1'
engine1 = db.create_engine(sql_connect1)#, echo = True)
vinepair_connect = engine1.connect()

wp_tax = pd.read_sql("select * from wp_term_taxonomy;", wordpress_connect)
wp_terms = pd.read_sql("select * from wp_terms;", wordpress_connect)
wp_term_relat = pd.read_sql("select * from wp_term_relationships;", wordpress_connect)
post_info = pd.read_sql("select ID, post_date, post_name from wp_posts;", wordpress_connect)
post_info = post_info.rename(columns={'ID': 'object_id', 'post_name': 'post_slug'})
wp = wp_tax.merge(wp_terms, on = 'term_id', how = 'outer')  #term_taxonomy_id term_id taxonomy description parent count name slug term_group
wp_pageterms = wp_term_relat.merge(wp, on = 'term_taxonomy_id', how = 'outer')
wp_pageterms = wp_pageterms.merge(post_info, on = 'object_id', how = 'outer')
#print(wp_pageterms.shape)
wp_pageterms = wp_pageterms[wp_pageterms['post_slug']!='']
#print(wp_pageterms.shape)

##------------------IMPORTING ALL REGIONS,THEIR PAGES, AND THEIR TAGS-----------------------------------------------

all_tracked_regions = track_countries.extend(track_regions)
region_term_types = {'appellation': region_appellation, 'post_tag':region_post_tag}
import_regions = pd.read_csv('./track_regions.csv', header = 0, index_col = 'Group Name')
region_names = sorted(import_regions.index.tolist())
import_regions = import_regions.to_dict(orient='series')


#Convert values to lists
for col in region_term_types.keys():
    tracked_regions = vf.convert_csv_input(import_regions,col)
#Create dictionary with keys = country and values as list of indexes we care about
#This depends on the input of what kind of tags matter
region_group_tdict = {}
for name in region_names:
    region_group_tdict[name] = []
    ## NOTE: Put in a check that at least one is true
    for key, val in region_term_types.items():
        #key is the tag type, value is true or false
        if val == True:
            if key == 'appellation':
                app_key = tracked_regions[key][name]              
                while len(app_key)!=0:
                    region_group_tdict[name].extend(app_key)
                    app_key = wp[wp['parent'].isin(app_key)]['term_taxonomy_id'].tolist()
                # if len(app_key)==0:
                #     region_group_tdict.extend([])
            else:
                region_group_tdict[name].extend(tracked_regions[key][name])

tindex_region_dict = vf.invert_dict(region_group_tdict) #map indexes to the wine
all_region_tindexes = set([int(val) for values in region_group_tdict.values() for val in values])


region_pages = {}
for region in region_names:
    region_pages[region] = set(wp_pageterms[wp_pageterms['term_taxonomy_id'].isin(region_group_tdict[region])&(wp_pageterms['object_id'].notnull())]['object_id'])
all_region_pages = sorted(set([int(value) for values in region_pages.values() for value in values]))

##------------------IMPORTING ALL WINES ,THEIR PAGES, AND THEIR TAGS-------------------------------------------------------


wine_term_types = {'wbs_master_taxonomy_node_type': wine_wbs,
                    'post_tag':wine_post_tag, 
                    'variety':wine_variety,
                    'review:goodfor':wine_review_goodfor}
                    
#import wine file with term_taxonomy_ids
all_wines = pd.read_csv('./track_wines.csv', header = 0, index_col = 'Group Name')
wine_names = sorted(all_wines.index.tolist())
all_wines = all_wines.to_dict(orient='series')
#Convert values to lists
for col in wine_term_types.keys():
    all_wines = vf.convert_csv_input(all_wines,col)
#Create dictionary with keys = wine type and values as list of indexes we care about
#This depends on the input of what kind of tags matter
wine_group_tdict = {}
for name in wine_names:
    wine_group_tdict[name] = []
    #Put in a check that at least one is true
    for key, val in wine_term_types.items():
        if val == True:
            wine_group_tdict[name].extend(all_wines[key][name])
tindex_wine_dict = vf.invert_dict(wine_group_tdict) #map indexes to the wine
all_wine_tindexes = set([int(val) for values in wine_group_tdict.values() for val in values])

wine_pages = {}
for wine in wine_names:
    wine_pages[wine] = set(wp_pageterms[wp_pageterms['term_taxonomy_id'].isin(wine_group_tdict[wine])&(wp_pageterms['object_id'].notnull())]['object_id']) #object id is the page idea
all_wine_pages = sorted(set([int(value) for values in wine_pages.values() for value in values]))

for wine in wine_pages:
    l_pages = len(wine_pages[wine])
    logging.info(f"Number of {wine} pages: {l_pages}")

#--------------------CREATE 2D ARRAY TO MARK WHICH WINE GROUPS HAVE WHICH PAGES----------------------------------

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



#----------------WEIGHT PAGES BY THE NUMBER OF WINE TYPES IN THE TAGS OF A PAGE---------------------------------

#page weight just evenly divides pages by wines in them then divides and sqaures
#Ex: page tags chardonnay, pinot noir, and cab franc - would be weighted 1/3 for each of these wines
row_vals = np.nansum(wine_type_mat, axis = 1)    
page_weights_wine = (wine_type_mat.T/row_vals).T #could square this to skew weighting better?
#pageweights are 0 if no value exists


#------------------AGGREGATE PAGES BY WINE TYPE and PAGE WEIGHTS FOR NET VIEWS----------------------------------------------

wine_pageviews_df, date_list = vf.get_pageviews(vinepair_connect, all_wine_pages, 'pageviews', True)
all_wine_weighted_netviews = np.zeros((len(date_list),len(wine_names)))
for col in range(page_weights_wine.shape[1]):
    #column is the wine group (e.g. chardonnay, pinot noir), rows are the dates and values are the net pageviews
    all_wine_weighted_netviews[:, col] = np.nansum((wine_pageviews_df.T*page_weights_wine[:,col]).T, axis = 0).T

pd.DataFrame(wine_pageviews_df.T, columns = all_wine_pages, index = date_list).to_csv('AllPageViewsRaw.csv')


#--------------CREATE DICTIONARY OF DICTIONARIES OF WEIGHTED PAGEVIEWS OF WINE BY GROUP AND REGION---------------- 
#probably better to make this a dictionary of matricies to make it easier to incorporate search terms
logging.info("\n NUMBER OF WINES FOR EACH REGION")
subgroup_netviews = {}
for wine_group, w_pages in wine_pages.items():
    for region_group, r_pages in region_pages.items():
        subgroup_netviews[wine_group] = {}
        region_weighted_netviews = np.zeros((len(date_list),len(all_region_pages)))
        page_weights = page_weights_wine.copy()
        page_overlaps= w_pages & r_pages #intersection of the two sets
        if wine_group in track_wines:
            logging.info(f"Number of {region_group} & {wine_group} pages: {len(page_overlaps)}")
        # if len(page_overlaps)==0:
        #     print(f'No pages for {region_group} and {wine_group}')
        page_rows = [page2row[page] for page in page_overlaps]
        mask=np.zeros((page_weights.shape[0],page_weights.shape[1]), dtype=np.bool_)
        mask[page_rows,:] = True
        page_weights = page_weights*mask #only want rows where the pages overlap
        region_weight_netviews = np.nansum((wine_pageviews_df.T*page_weights[:,col]).T, axis = 0)
        subgroup_netviews[wine_group][region_group] = region_weight_netviews

wine_column_names = [row2wine[i] for i in range(all_wine_weighted_netviews.shape[1])]
pd.DataFrame(all_wine_weighted_netviews, index = date_list, columns = wine_column_names).to_csv('WeightedWineViewsTest.csv') 

with open('RegionalWineViews.json', 'w') as json_file:
  json.dump(subgroup_netviews, json_file)