
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




wine_group_tdict = {}
for wine_group, query in wine_term_sql.items():
    #gets a set of  indexes from the vinepair1 database for each wine group 
    wine_group_tdict[wine_group] = get_tindexes(query)
tindex_wine_dict = invert_dict(wine_group_tdict) #map indexes to the wine
all_wine_tindexes = set([value for values in wine_pages.values() for value in values])
wines = list(wine_group_tdict.keys())


country_group_tdict = {}
for country_group, query in country_term_sql.items():
    #gets the indexes from the vinepair1 database
    country_group_tdict = get_tindexes(query)
    country_group_tdict[country_group] = get_tindexes(query)
tindex_country_dict = invert_dict(country_group_tdict) #map indexes to the wine
all_country_tindexes = set([value for values in country_pages.values() for value in values])
countries = country_group_tdict.keys()

wine_pages = {}
    for wine in wine_group_tlist.keys():
        wine_pages[wine] = set(wp_pageterms[wp_pageterms['term_taxonomy_id'].isin(wine_group_tlist[wine])]['object_id']) #object id is the page idea

country_pages = {}
    for country in country_groups_tlist.keys():
        country_pages[wine] = set(wp_pageterms[wp_pageterms['term_taxonomy_id'].isin(country_group_tlist[wine])]['object_id'])

all_wine_pages = list(set([value for values in wine_pages.values() for value in values]))
all_country_pages = list(set([value for values in country_pages.values() for value in values]))


#for all pages that reference specific wines, mark which pages reference which wines
#columns are all the wines
#rows are pages
wine_type_mat = np.zeros((len(all_wine_pages), len(wines), dtype=np.bool_) # listed as false by default
i2p = dict(list((mat_pi,pindex) for mat_pi,pindex in enumerate(all_wine_pages)))
p2i = dict(list((pindex,mat_pi) for mat_pi,pindex in enumerate(all_wine_pages)))
i2w = dict(list((mat_wi,wine_ind) for mat_wi,wine_ind in enumerate(wines)))
w2i = dict(list((wine_ind,mat_wi) for mat_wi,wine_ind in enumerate(wines)))
for wine, page in all_wine_pages.items():
    for term_id in wp_pageterms[wp_pageterms['object_id']==page]['term_taxonomy_id'].tolist():
        if term_id in tindex_wine_dict.keys():
            wine = tindex_wine_dict[term_id]
            for val in wine:
                col = w2i[val]
                row = p2i[page]
                wine_type_mat[row,col] = True #if page has wine in it's tindex it's listed as True

#page weight just evenly divides pages by wines in them then divides and sqaures
#Ex: page tags chardonnay, pinot noir, and cab franc - would be weighted 1/3^2 for each of these wines
#Squaring is just so it weights the more targeted pages higher
row_vals = np.nansum(wine_type_mat, axis = 1)    
page_weights_wine = (wine_type_mat.T/row_vals).T**2









page_overlaps = {}:
    for wine_group, wine_pages in wine_pages.items():
        page_overlaps[wine_group]={}
        for country_group, country_pages in country_pages.items():
            page_overlaps[wine_group][country_group] = wine_pages & country_pages #intersection of the two sets

page_weights = np.zeros((len(all_wine_pages), len(wine_group_tdict.keys))

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

 def get_tindexes(sql_query_end): 
    tindexes = pd.read_sql('select tindex from tindex where lower(term) '+sql_query_end, dbconnect)['tindex'].unique().tolist() #these are term_taxonomy_id's
    return tindexes

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