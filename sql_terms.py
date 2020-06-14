import pandas as pd 


files = pd.read_csv(filename = 'track_wines.csv', header = 0)


country_term_sql = {
    'italy': "= 'italy';",
    'france':"like '%%france';",
    'spain':"= 'spain';" ,
    'argentina': " = 'argentina';",
    'chile':"like '%%chile%%';" ,
    'nz':"= 'new zealand';",
    'sa':"= 'south africa';",
    'greece':"= 'greece';",
    'portugal':"= 'portugal';",
    'australia':"like '%%australia';",
    'austria':"= 'austria';",
    'germany':"= 'germany';",
    'england':"= 'england' or lower(term) = 'great britain';",
    'mexico':"= 'mexico';",
    'israel':"like 'israel%%'", 
    'lebanon':"= 'lebanon';",
    'us':"= 'usa' or lower(term) = 'american wine' or lower(term) = 'america';"
}
#Inputs
#wbs=True
#post_tag = True
#variety = True
#review:goodfor = True

tracked_wines = pd.read_csv('track_wines.csv', header = 0, index_col = 'Group Name')
tracked_wines = tracked_wines.to_dict(orient='series')

for col in ['wbs_master_taxonomy_node_type', 'post_tag', 'variety', 'review:goodfor']:
    tracked_wines = convert_csv_input(tracked_wines,col)
del tracked_wines['Notes']


def convert_csv_input(df, column):
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




def get_tindexes(sql_query_end):
    tindexes = pd.read_csv('')['tindex']
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