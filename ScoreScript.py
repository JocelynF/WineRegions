import numpy as np 
import pandas as pd 
import datetime
import sqlalchemy as db
import time
import ScoreFunctions as sf
import logging
import json
import itertools
import csv
import os
from user_input import *
import my_cred
import pdb
## ------------START USER INPUT -------------------------------

#Where to log info
if os.path.exists("RegionalWines.log"):
    os.remove("RegionalWines.log")
logging.basicConfig(filename='RegionalWines.log',level=logging.DEBUG)

#----------------------END USER INPUT------------------------------------
start = time.time()

sql_connect =  my_cred.wordpress_connect
engine = db.create_engine(sql_connect)#, echo = True)
wordpress_connect = engine.connect()

sql_connect1  =  my_cred.vp_connect
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
wp_pageterms = wp_pageterms[wp_pageterms['object_id'].notnull()]

#print(wp_pageterms.shape)
print('Finished Importing Page Information: ', round(time.time()-start,2), 'sec')


##------------------IMPORTING ALL WINES ,THEIR PAGES, AND THEIR TAGS-------------------------------------------------------


wine_term_types = {'wbs_master_taxonomy_node_type': wine_wbs,
                    'post_tag':wine_post_tag, 
                    'variety':wine_variety,
                    'review:goodfor':wine_review_goodfor}

wine_tdict, wine_pages, all_winetype_pages = sf.get_page_indexes('./track_wines.csv', wine_term_types, wp_pageterms)                
#all possible wine pages even ones not labelled with a wine type from the spreadsheed
#wine_total = wp_pageterms[(wp_pageterms['name'].str.contains('wine', case = False))&(wp_pageterms['taxonomy'].notnull())]
#wine_total = set(wine_total[(~wine_total['taxonomy'].str.contains('cocktail', case = False))]['object_id'].unique())

wine_total_pages = all_winetype_pages.copy()
#wine_pages['NO_WINE_TYPE'] = set(wine_total_pages)-set(all_winetype_pages)
wine_names = list(wine_pages.keys())
index2wine = dict(list((mat_wi,wine_ind) for mat_wi,wine_ind in enumerate(wine_names)))
wine2index = dict(list((wine_ind,mat_wi) for mat_wi,wine_ind in enumerate(wine_names)))

for wine in wine_pages:
    l_pages = len(wine_pages[wine])
    logging.info(f"Number of {wine} pages: {l_pages}")

print('Finished Mapping Wine Pages and Terms: ', round(time.time()-start,2), 'sec')

##------------------IMPORTING ALL REGIONS,THEIR PAGES, AND THEIR TAGS-----------------------------------------------

#all_tracked_regions = track_countries.extend(track_regions)
region_term_types = {'appellation': region_appellation, 'post_tag':region_post_tag}


region_tdict, region_pages, all_region_pages = sf.get_page_indexes('./track_regions.csv', region_term_types, wp_pageterms)
region_pages['NO_LOCATION'] = set(wine_total_pages)-set(all_region_pages)
region_names = list(region_pages.keys())
index2region = dict(list((mat_ri,region_ind) for mat_ri,region_ind in enumerate(region_names)))
region2index = dict(list((region_ind,mat_ri) for mat_ri,region_ind in enumerate(region_names)))

for region in region_pages:
    l_pages = len(region_pages[region])
    logging.info(f"Number of {region} pages: {l_pages}")


print('Finished Mapping Region Pages and Terms: ', round(time.time()-start,2), 'sec')

#pdb.set_trace()

#--------------------CREATE 2D ARRAY TO MARK WHICH WINE GROUPS HAVE WHICH PAGES----------------------------------

#for all pages that reference specific wines, mark which pages reference which wines
#columns are all the wine group names and rows are pages

wine_type_mat = np.zeros((len(wine_total_pages), len(wine_names)), dtype=np.bool_) # listed as false by default
index2page = dict(list((mat_pi,pindex) for mat_pi,pindex in enumerate(wine_total_pages)))
page2index = dict(list((pindex,mat_pi) for mat_pi,pindex in enumerate(wine_total_pages)))


for wine, pages in wine_pages.items():
    col = wine2index[wine]
    page_indexes= [page2index[page] for page in pages]
    #if page has wine in it's tindex it's listed as True
    wine_type_mat[page_indexes, col] = True


print('Finished Creating Page Term Matrix: ', round(time.time()-start,2), 'sec')

#----------------WEIGHT PAGES BY THE NUMBER OF WINE TYPES IN THE TAGS OF A PAGE---------------------------------

#page weight just evenly divides pages by wines in them then divides and sqaures
#Ex: if a page tags chardonnay, pinot noir, and cab franc - would be weighted 1/3 for each of these wines
row_vals = np.nansum(wine_type_mat, axis = 1)    
page_weights_wine = (wine_type_mat.T/row_vals).T 

#pageweights are 0 if no value exists

print('Finished Creating Page Weighting Matrix: ', round(time.time()-start,2), 'sec')

#------------------AGGREGATE PAGES BY WINE TYPE and PAGE WEIGHTS FOR NET VIEWS AND FILTER OUTLIERS ----------------------------------------------

#May want to do this later to factor in high views on the page creation date
# page_dates = post_info[post_info['object_id'].isin(wine_total_pages)].loc[:, ['object_id', 'post_date']]
# post_dates = page_dates.set_index('object_id')['post_date'].to_dict()
# index2date = dict(list((page2index[p_id],date) for p_id, val in post_dates.items()))

#wine_pageviews_array: col = dates, row = page
wine_pageviews_array, date_list = sf.get_pageviews(vinepair_connect, wine_total_pages, index2page, page2index, VIEWTYPE, SUBTRACT_SCROLL) #not aggregated
#make beginning nan's if no views yet
wine_pageviews_array[np.cumsum(wine_pageviews_array, axis = 1)==0]=np.nan

if OUTLIER_DETECTION == 'STANDARD':
    print('Performing Spike Detection')
    #only filtered in the horizontal direction, not by group
    page_spikes, filtered_pageviews = sf.page_outliers(wine_pageviews_array, cutoff = CUTOFF, sigcut=SIG_CUT, hardcut=HARDCUT) 
    print('Finished Standard Outlier Detection: ', round(time.time()-start,2), 'sec')

elif OUTLIER_DETECTION == 'ISOLATION FOREST':
    print('Performing Spike Detection')
    filtered_pageviews = sf.iso_forest_outliers(wine_pageviews_array, OUTLIER_FRACTION, PAGE_LOWER_LIMIT, NUM_DAYS_LOWER_LIMIT)
    print('Finished Isolation Forest Outlier Detection: ', round(time.time()-start,2), 'sec')

print('Aggregating Page Views by Wine Type')
all_wine_weighted_netviews_unfiltered = np.zeros((len(date_list),len(wine_names)))
all_wine_weighted_netviews_filtered = np.zeros((len(date_list),len(wine_names)))
for wine in wine_names:
    col = wine2index[wine]
    #column is the wine group (e.g. chardonnay, pinot noir), rows are the dates and values are the net pageviews
    all_wine_weighted_netviews_unfiltered[:, col] = np.nansum((wine_pageviews_array.T*page_weights_wine[:,col]).T, axis = 0).T #aggregated
    all_wine_weighted_netviews_filtered[:, col] = np.nansum((filtered_pageviews.T*page_weights_wine[:,col]).T, axis = 0).T #aggregated



print('Finished Aggregating Pages by Wine Type: ', round(time.time()-start,2), 'sec')

#--------------CREATE DICTIONARY OF DICTIONARIES OF WEIGHTED PAGEVIEWS OF WINE BY GROUP FOR SUBREGION---------------------------- 


for wine_group in track_wines:

    wine_col = wine2index[wine_group]
    wine_weights = page_weights_wine[:,wine_col]
    wine_views_unfiltered = all_wine_weighted_netviews_unfiltered[:,wine_col]
    wine_views_filtered = all_wine_weighted_netviews_filtered[:,wine_col]

    #Country First:
    views_unfilt_all, scores_unfilt_all, views_filt_all, scores_filt_all, country2index, all_country_weights= \
        sf.get_sub_views('COUNTRY', track_countries, region_pages, page2index, \
        date_list, wine2index, wine_weights, wine_pageviews_array, filtered_pageviews, \
        wine_views_unfiltered, wine_views_filtered,  None)

    #subregions next
    for sub_name in all_regions:
        subregion_list = all_regions[sub_name]['REGIONS']
        country = all_regions[sub_name]['PARENT']
        country_column = country2index[country]
        country_weights = all_country_weights[:,country_column][:,np.newaxis]
        views_unfilt_new, scores_unfilt_new, views_filt_new, scores_filt_new, sub2index, page_weights_sub= \
            sf.get_sub_views(sub_name, subregion_list, region_pages, page2index, \
            date_list, wine2index, wine_weights,wine_pageviews_array, filtered_pageviews, \
            wine_views_unfiltered, wine_views_filtered, country_weights)
        if sub_name == 'UNITED STATES':
            cali_column = sub2index['CALIFORNIA']
            california_pageweights = page_weights_sub[:,cali_column][:,np.newaxis]
        #add to dataframe
        views_unfilt_all = pd.concat([views_unfilt_all, views_unfilt_new], axis = 1)
        scores_unfilt_all = pd.concat([scores_unfilt_all, scores_unfilt_new], axis = 1)
        views_filt_all = pd.concat([views_filt_all, views_filt_new], axis = 1)
        scores_filt_all = pd.concat([scores_filt_all, scores_filt_new], axis = 1)

    #california last
    views_unfilt_new, scores_unfilt_new, views_filt_new, scores_filt_new, sub2index, page_weights_sub= \
        sf.get_sub_views('CALIFORNIA', california_subregions['REGIONS'], region_pages, page2index, \
        date_list, wine2index, wine_weights,wine_pageviews_array, filtered_pageviews, \
        wine_views_unfiltered, wine_views_filtered, california_pageweights)
    #add to dataframe
    views_unfilt_all = pd.concat([views_unfilt_all, views_unfilt_new], axis = 1)
    scores_unfilt_all = pd.concat([scores_unfilt_all, scores_unfilt_new], axis = 1)
    views_filt_all = pd.concat([views_filt_all, views_filt_new], axis = 1)
    scores_filt_all = pd.concat([scores_filt_all, scores_filt_new], axis = 1)

    w = wine_group.replace(' ', '').replace('/','')
    views_unfilt_all.to_csv(f'Results/{w}_UnfiltViews.csv')
    views_filt_all.to_csv(f'Results/{w}_FiltViews.csv')
    scores_unfilt_all.to_csv(f'Scores/{w}_UnfiltScoreDaily.csv')
    scores_filt_all.to_csv(f'Scores/{w}_FiltScoreDaily.csv')

    tojson = scores_unfilt_all.resample('M').mean().reset_index()
    tojson['DATE'] = tojson['DATE'].dt.strftime('%Y-%m')
    tojson.to_json(f'Scores/{w}_UnfiltScoreMonthly.json', orient = 'records', indent = 5)

    tojson = scores_filt_all.resample('M').mean().reset_index()
    tojson['DATE'] = tojson['DATE'].dt.strftime('%Y-%m')
    tojson.to_json(f'Scores/{w}_FiltScoreMonthly.json', orient = 'records', indent = 5)
        

print('Finished Aggregating Pages by Region: ', round(time.time()-start,2), 'sec')


#--------------------------------OUTPUT FILES----------------------------------------------------


print("Outputting Final Files")
#Output files for pageviews we care about, columns are the page index, and rows are the dates
pd.DataFrame(wine_pageviews_array.T, columns = wine_total_pages, index = date_list).to_csv('Results/AllPageViews_Unfiltered.csv')
pd.DataFrame(filtered_pageviews.T, columns = wine_total_pages, index = date_list).to_csv('Results/AllPageViews_Filtered.csv')

#Output files for the weighted pageviews for the differet wine groups, column is group name, rows are the dates
#This includes all the wine groups, not just the ones meant to be tracked
wine_column_names = [index2wine[i] for i in range(all_wine_weighted_netviews_unfiltered.shape[1])]
pd.DataFrame(all_wine_weighted_netviews_unfiltered, index = date_list, columns = wine_column_names).to_csv('Results/WeightedWineViews_Unfiltered.csv') 
pd.DataFrame(all_wine_weighted_netviews_filtered, index = date_list, columns = wine_column_names).to_csv('Results/WeightedWineViews_Filtered.csv') 


with open("Results/WinePages.csv", "w") as outfile:
    writer = csv.writer(outfile)
    writer.writerow(wine_pages.keys())
    writer.writerows(itertools.zip_longest(*wine_pages.values()))

with open("Results/RegionalPages.csv", "w") as outfile:
    writer = csv.writer(outfile)
    writer.writerow(region_pages.keys())
    writer.writerows(itertools.zip_longest(*region_pages.values()))


print ("Job Completed: ", round(time.time()-start, 2), 'sec')