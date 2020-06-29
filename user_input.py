
"""
This file contains all of the information that needs to be input for the code to run. 
"""

#WEIGHTING_SCHEME (INV OR SQUARE OR NONE) DETERMINES WHETHER A PAGE IS 
#WEIGHTED BY THE INVERSE NUMBER OF TRACKED TAGS OR THE SQUARE OF THE INVERSE
# OR NOT WEIGHTED AT ALL

WEIGHT_SCHEME = 'INV' #OR 'SQUARE' OR 'NONE'

#WHAT KIND OF TAXONOMY WOULD YOU LIKE TO INCLUDE
wine_wbs = True
wine_post_tag = False
wine_variety = True
wine_review_goodfor = True

region_appellation = True 
region_post_tag = True
region_wbs = True

#Subtract scroll events?
SUBTRACT_SCROLL = True

#Options are "STANDARD" or "ISOLATION FOREST"
#Must choose one of the two, right now it's set up to run both unfiltered and filterd.
OUTLIER_DETECTION = "ISOLATION FOREST"
SIG_CUT = 10 # required for "STANDARD"
HARDCUT = 5000 #required for "STANDARD"
CUTOFF = 0.5 #required for "STANDARD"
PAGE_LOWER_LIMIT = 30 #Required for "ISOLATION FOREST"
NUM_DAYS_LOWER_LIMIT = 60 #Required for "ISOLATION FOREST"
OUTLIER_FRACTION = 0.003 #Required for "ISOLATION FOREST"


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

