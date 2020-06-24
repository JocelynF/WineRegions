import numpy as np 
import pandas as pd 
import datetime
import sqlalchemy as db
import time
import ScoreFunctions as vf
import logging
import json
import itertools
import csv
import os
## ------------START USER INPUT -------------------------------

#Where to log info
#os.remove("RegionalWines.log")
logging.basicConfig(filename='RegionalWines.log',level=logging.DEBUG)

#THESE NEED TO BE INCLUDED IN THE TRACKED_WINES.CSV FILE
track_wines = ['CABERNET SAUVIGNON',
                'CHARDONNAY',
                'MALBEC',
                'MERLOT',
                'MOSCATO',
                'PINOT GRIGIO/PINOT GRIS',
                'PINOT NOIR',
                'RED BLEND',
                'RIESLING',
                'ROSE',
                'SAUVIGNON BLANC',
                'SHIRAZ/SYRAH']

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

#WEIGHTING_SCHEME (INV OR SQUARE OR NONE) DETERMINES WHETHER A PAGE IS 
#WEIGHTED BY THE INVERSE NUMBER OF TRACKED TAGS OR THE SQUARE OF THE INVERSE
# OR NOT WEIGHTED AT ALL
WEIGHT_SCHEME = 'INV' #OR 'SQUARE' OR 'NONE'

#WHAT KIND OF TAXONOMY WOULD YOU LIKE TO INCLUDE
wine_wbs = True
wine_post_tag = False
wine_variety = False
wine_review_goodfor = False

region_appellation = True 
region_post_tag = True
region_wbs = True

SUBTRACT_SCROLL = False

#----------------------END USER INPUT------------------------------------
start = time.time()

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
#Long Island is listed as being a parent but should be under New York
wp.loc[wp['name']=='Long Island','parent'] = 4331

wp_pageterms = wp_term_relat.merge(wp, on = 'term_taxonomy_id', how = 'outer')
wp_pageterms = wp_pageterms.merge(post_info, on = 'object_id', how = 'outer')

#print(wp_pageterms.shape)
wp_pageterms = wp_pageterms[wp_pageterms['post_slug']!='']
#print(wp_pageterms.shape)
print('Mark 1: ', time.time()-start)
##------------------IMPORTING ALL REGIONS,THEIR PAGES, AND THEIR TAGS-----------------------------------------------

#all_tracked_regions = track_countries.extend(track_regions)
region_term_types = {'appellation': region_appellation, 'post_tag':region_post_tag}


region_pages, all_region_pages = vf.get_page_indexes('./track_regions.csv', region_term_types, wp_pageterms)
region_names = list(region_pages.keys())
index2region = dict(list((mat_ri,region_ind) for mat_ri,region_ind in enumerate(region_names)))
region2index = dict(list((region_ind,mat_ri) for mat_ri,region_ind in enumerate(region_names)))



print('Mark 2: ', time.time()-start)

##------------------IMPORTING ALL WINES ,THEIR PAGES, AND THEIR TAGS-------------------------------------------------------


wine_term_types = {'wbs_master_taxonomy_node_type': wine_wbs,
                    'post_tag':wine_post_tag, 
                    'variety':wine_variety,
                    'review:goodfor':wine_review_goodfor}

wine_pages, all_wine_pages = vf.get_page_indexes('./track_wines.csv', wine_term_types, wp_pageterms)                
wine_names = list(wine_pages.keys())
index2wine = dict(list((mat_wi,wine_ind) for mat_wi,wine_ind in enumerate(wine_names)))
wine2index = dict(list((wine_ind,mat_wi) for mat_wi,wine_ind in enumerate(wine_names)))

with open("WinePages.csv", "w") as outfile:
    writer = csv.writer(outfile)
    writer.writerow(wine_pages.keys())
    writer.writerows(itertools.zip_longest(*wine_pages.values()))

for wine in wine_pages:
    l_pages = len(wine_pages[wine])
    logging.info(f"Number of {wine} pages: {l_pages}")

print('Mark 3: ', time.time()-start)

#--------------------CREATE 2D ARRAY TO MARK WHICH WINE GROUPS HAVE WHICH PAGES----------------------------------

#for all pages that reference specific wines, mark which pages reference which wines
#columns are all the wine group names and rows are pages

wine_type_mat = np.zeros((len(all_wine_pages), len(wine_names)), dtype=np.bool_) # listed as false by default
index2page = dict(list((mat_pi,pindex) for mat_pi,pindex in enumerate(all_wine_pages)))
page2index = dict(list((pindex,mat_pi) for mat_pi,pindex in enumerate(all_wine_pages)))


for wine, pages in wine_pages.items():
    col = wine2index[wine]
    page_indexes= [page2index[page] for page in pages]
    #if page has wine in it's tindex it's listed as True
    wine_type_mat[page_indexes, col] = True


print('Mark 4: ', time.time()-start)

#----------------WEIGHT PAGES BY THE NUMBER OF WINE TYPES IN THE TAGS OF A PAGE---------------------------------
if WEIGHT_SCHEME == 'INV':
#page weight just evenly divides pages by wines in them then divides and sqaures
#Ex: if a page tags chardonnay, pinot noir, and cab franc - would be weighted 1/3 for each of these wines
    row_vals = np.nansum(wine_type_mat, axis = 1)    
    page_weights_wine = (wine_type_mat.T/row_vals).T 
elif WEIGHT_SCHEME=='SQUARE':
    row_vals = np.nansum(wine_type_mat, axis = 1)    
    page_weights_wine = (wine_type_mat.T/(row_vals**2)).T 
else:
    page_weights_wine = wine_type_mat.copy()
#pageweights are 0 if no value exists

print('Mark 5: ', time.time()-start)

#------------------AGGREGATE PAGES BY WINE TYPE and PAGE WEIGHTS FOR NET VIEWS AND FILTER OUTLIERS ----------------------------------------------

#May want to do this later to factor in high views on the page creation date
# page_dates = post_info[post_info['object_id'].isin(all_wine_pages)].loc[:, ['object_id', 'post_date']]
# post_dates = page_dates.set_index('object_id')['post_date'].to_dict()
# index2date = dict(list((page2index[p_id],date) for p_id, val in post_dates.items()))


wine_pageviews_array, date_list = vf.get_pageviews(vinepair_connect, all_wine_pages, index2page, page2index, 'pageviews', SUBTRACT_SCROLL) #not aggregated
page_spikes, filtered_pageviews = vf.page_outliers(wine_pageviews_array, cutoff = 0.5, sigcut=5, hardcut=5000) #only filtered in the horizontal direction, not by group

all_wine_weighted_netviews_unfiltered = np.zeros((len(date_list),len(wine_names)))
all_wine_weighted_netviews_filtered = np.zeros((len(date_list),len(wine_names)))
for col in range(page_weights_wine.shape[1]):
    #column is the wine group (e.g. chardonnay, pinot noir), rows are the dates and values are the net pageviews
    all_wine_weighted_netviews_unfiltered[:, col] = np.nansum((wine_pageviews_array.T*page_weights_wine[:,col]).T, axis = 0).T #aggregated
    all_wine_weighted_netviews_filtered[:, col] = np.nansum((filtered_pageviews.T*page_weights_wine[:,col]).T, axis = 0).T #aggregated



print('Mark 6: ', time.time()-start)

#--------------CREATE DICTIONARY OF DICTIONARIES OF WEIGHTED PAGEVIEWS OF WINE BY GROUP AND REGION---------------- 
logging.info("\n NUMBER OF WINES FOR EACH REGION")
subgroup_netviews_unfiltered = {}
subgroup_netviews_filtered = {}
page_creation_dates = {}
num_pages_subgroup= {}
subgroup_score_unfiltered = {}
subgroup_score_filtered = {}
for wine_group, w_pages in wine_pages.items():
    if wine_group in track_wines:
        #print(wine_group)
        wine_col = wine2index[wine_group]
        subgroup_netviews_unfiltered[wine_group] = np.full((len(date_list),len(region_names)), np.nan)
        subgroup_netviews_filtered[wine_group] = np.full((len(date_list),len(region_names)), np.nan)
        #page_creation_dates[wine_group]= {}
        #num_pages_subgroup[wine_group] = np.zeros(len(region_names))
        subgroup_score_unfiltered[wine_group] = np.full((len(date_list),len(region_names)), np.nan)
        subgroup_score_filtered[wine_group] = np.full((len(date_list),len(region_names)), np.nan)
        for region_group, r_pages in region_pages.items():
            region_col = region2index[region_group]
            page_weights = page_weights_wine.copy()
            page_overlaps= w_pages & r_pages #intersection of the two sets
            #creation_dates = post_info[post_info['object_id'].isin(page_overlaps)]['post_date']
            #num_pages_subgroup[wine_group][region_col] = len(page_overlaps)
            #page_creation_dates[wine_group][region_group] = creation_dates
            logging.info(f"Number of {region_group} & {wine_group} pages: {len(page_overlaps)}")
            page_indexes = [page2index[page] for page in page_overlaps]
            mask=np.zeros((page_weights.shape[0],page_weights.shape[1]), dtype=np.bool_)
            mask[page_indexes,:] = True
            page_weights = page_weights*mask #only want rows where the pages overlap
            region_weight_netviews_unfiltered = np.nansum((wine_pageviews_array.T*page_weights[:,wine_col]).T, axis = 0)
            region_weight_netviews_filtered = np.nansum((filtered_pageviews.T*page_weights[:,wine_col]).T, axis = 0)
            subgroup_netviews_unfiltered[wine_group][:, region_col] = region_weight_netviews_unfiltered
            subgroup_netviews_filtered[wine_group][:, region_col] = region_weight_netviews_filtered
            subgroup_score_unfiltered[wine_group][:,region_col] = np.divide(region_weight_netviews_unfiltered,all_wine_weighted_netviews_unfiltered[:,wine_col])
            subgroup_score_filtered[wine_group][:,region_col] = np.divide(region_weight_netviews_filtered,all_wine_weighted_netviews_filtered[:,wine_col])

print('Mark 7: ', time.time()-start)


#--------------------------------OUTPUT FILES----------------------------------------------------



#Output files for pageviews we care about, columns are the page index, and rows are the dates
pd.DataFrame(wine_pageviews_array.T, columns = all_wine_pages, index = date_list).to_csv('AllPageViews_Unfiltered.csv')
pd.DataFrame(filtered_pageviews.T, columns = all_wine_pages, index = date_list).to_csv('AllPageViews_Filtered.csv')

#Output files for the weighted pageviews for the differet wine groups, column is group name, rows are the dates
#This includes all the wine groups, not just the ones meant to be tracked
wine_column_names = [index2wine[i] for i in range(all_wine_weighted_netviews_unfiltered.shape[1])]
pd.DataFrame(all_wine_weighted_netviews_unfiltered, index = date_list, columns = wine_column_names).to_csv('WeightedWineViews_Unfiltered.csv') 
pd.DataFrame(all_wine_weighted_netviews_filtered, index = date_list, columns = wine_column_names).to_csv('WeightedWineViews_Filtered.csv') 

#write the pages associated with each region in a file
# with open("RegionalPages.csv", "w") as outfile:
#     writer = csv.writer(outfile)
#     writer.writerow(region_pages.keys())
#     writer.writerows(itertools.zip_longest(*region_pages.values()))

#for each wine that is tracked, create a csv file with the weighted pageviews for each subgroup
for wine in track_wines:
    # unfilt = pd.DataFrame.from_dict(subgroup_netviews_unfiltered[wine],orient = 'columns')
    # unfilt.loc[:,'DATE']=date_list  
    # w = wine.replace(' ', '').replace('/','')
    # unfilt.to_csv(f'{w}_UnfiltViews.csv')

    filtview = pd.DataFrame.from_dict(subgroup_netviews_filtered[wine],orient = 'columns')
    filtview.loc[:,'DATE']=date_list
    filtview = filtview.rename(columns=index2region)  
    w = wine.replace(' ', '').replace('/','')
    filtview.to_csv(f'{w}_FiltViews.csv')

    # unfilt = pd.DataFrame.from_dict(subgroup_score_unfiltered[wine],orient = 'columns')
    # unfilt.loc[:,'DATE']=date_list  
    # w = wine.replace(' ', '').replace('/','')
    # unfilt.to_csv(f'{w}_UnfiltScore.csv')

    #The scores need to be in json files to send to front-end
    filtscore = pd.DataFrame.from_dict(subgroup_score_filtered[wine],orient = 'columns')
    filtscore.loc[:,'DATE']=date_list
    filtscore = filtscore.rename(columns=index2region) 
    filtscore = filtscore.set_index('DATE')
    tojson = filtscore.resample('M').mean().reset_index()
    tojson['DATE'] = tojson['DATE'].dt.strftime('%Y-%m')
    w = wine.replace(' ', '').replace('/','')
    tojson.to_json(f'{w}_FiltScore.json', orient = 'records', indent = 5)