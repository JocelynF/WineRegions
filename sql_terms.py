import pandas as pd 


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

wine_term_sql = {
    'rose' :"like 'ros_';",
    'malbec':"= 'malbec';",
    'moscato':"= 'moscato';",
    'cab_sav':"= 'cabernet sauvignon';",
    'pin_noir':"= 'pinot noir';",
    'merlot':"= 'merlot';",
    'riesling':"= 'riesling';",
    'sav_blanc':"= 'sauvignon blanc';",
    'red_blend':"= 'red blend';",
    'chard':"= 'chardonnay';",
    'syrah_shiraz':"like '%%syrah%%' or lower(term) like '%%shiraz%%';",
    'pin_gri':"like '%%pinot grigio%%' or lower(term) = '%%pinot gris%%';"
 }


  def get_tindexes(sql_query_end):
    tindexes = pd.read_sql('select tindex from tindex where lower(term) '+sql_query_end, dbconnect)['tindex']
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