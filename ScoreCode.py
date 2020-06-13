
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




wine_group_tlist = {}
for wine_group, query in wine_term_sql.items():
    #gets the indexes from the vinepair1 database
    wine_group_tlist = get_tindexes(query)

country_group_tlist = {}
for country_group, query in country_term_sql.items():
    #gets the indexes from the vinepair1 database
    country_group_tlist = get_tindexes(query)

wine_pages = {}
    for wine in wine_group_tlist.keys():
        wine_pages[wine] = set(wp_pageterms[wp_pageterms['term_taxonomy_id'].isin(wine_group_tlist[wine])]['object_id']) #object id is the page idea

country_pages = {}
    for country in country_groups_tlist.keys():
        wine_pages[wine] = set(wp_pageterms[wp_pageterms['term_taxonomy_id'].isin(country_group_tlist[wine])]['object_id'])

page_overlaps = {}:
    for wine_group, wine_pages in wine_pages.items():
        page_overlaps[wine_group]={}
        for country_group, country_pages in country_pages.items():
            page_overlaps[wine_group][country_group] = wine_pages & country_pages #intersection of the two sets




 def get_tindexes(sql_query_end): 
    tindexes = pd.read_sql('select tindex from tindex where lower(term) '+sql_query_end, dbconnect)['tindex'].unique().tolist() #these are term_taxonomy_id's
    return tindexes

def find_pindexes(tindexes, print_counts = False):
    pinds = pterms_long[pterms_long['term_int'].isin(tindexes.values)]
    pinds = pinds['pindex'].unique()
    if print_counts == True:
        print(len(pinds))
    return pinds

def page_overlap(pinds1, pinds2, print_counts = False):
    p_overlap = np.intersect1d(pinds1, pinds2)
    if print_counts == True:
        print('Pind1: ', len(pinds1), 'Pind2: ', len(pinds2), 'Overlap: ', len(p_overlap))
    return p_overlap

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