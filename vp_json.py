import sys,os,time,datetime,pickle,copy
import numpy as np
#local imports
from vp_data import DataProc,AggScheme,AggFunc,TsArray
from pv_ingest import Utils
from vp_plot import Plotter
from vp_prop import vinepair_creds

###################################################
#    VINEPAIR SPECIFIC INPUTS
#
# Dates (format is YYYY-mm-dd) - inclusive
# all dates are queried, selected range output
# DATA_ dates are for the range to be analyzed
# EXPORT_ dates are for the range to be exported
DATA_START_DATE="2016-01-01"
DATA_END_DATE="2019-11-01"
EXPORT_START_DATE="2016-01-01"
EXPORT_END_DATE="2019-11-13"

# AGGREGATION SCHEME
# AGG_CSV_PATH = PATH to CSV for aggregation_scheme import
# AGG_TYPE = how to aggregate for each month, "median","mean", any other is interpreted as "sum"
# WEIGHT_SCHEME = how to weight pages in each group, inv=inverse page frequency, cos = cosine similarity
#     None or any other value means no weighting (all views counted in all groups)
AGG_CSV_PATH = "search_track.csv"
AGG_MODE = "median"
WEIGHT_SCHEME = "inv"

# JSON OUTPUT CONTROL
# JSON_OUTPUT = Filename Root for JSON output (one file generated for every master category)
# JSON_CAPS = words that should be in ALL CAPS in JSON output (case insensitive here)
JSON_OUTPUT = "test_json"
JSON_CAPS = ["ipa"]

# PAGE AGGREGATION SETTINGS
# PAGE_CATEGORY = What to aggregate for each page? Options are "sessions","pageviews","uniquePageviews",
#         "avgSessionDuration","entrances","bounceRate","exitRate"
# SUBTRACT_SEV = subtract scroll events from page_category?  True/False

PAGE_CATEGORY = "pageviews"
SUBTRACT_SEV = True

# SEARCH TERM INCORPORATION
# SEARCH_TRACK = What to count?  Options are: "clicks","ctr","impressions","position"
# SEARCH_WEIGHT = how much to weight search vs page counts? (float 0.0 to 1.0 inclusive)
SEARCH_TRACK="impressions"
SEARCH_WEIGHT = 0.5

# OUTLIER FILTERING
# Filtering Cutoffs (all three must be met for a point to be filtered)
# Filtered values are set to DAY_CUT
#   DAY_CUT = fraction of day's aggregated total
#   SIG_CUT = #SD above median in entire timeseries
#   HARD_CUT = Absolute cutoff in counts
DAY_CUT = 0.5
SIG_CUT = 10.0
HARD_CUT = 5000

# Create / Use local copy?
# To speed up processing, save all timeseries arrays objects and reload on start
#     timeseries_array_XXX.pkl where XXX is PAGE_CATEGORY, search, agg, etc.
USE_LOCAL_COPY = True

#    END OF USER INPUT (AUTOPILOT FROM HERE)
###################################################

#initialize 
util = Utils()
aggfunc = AggFunc()
dt = datetime.datetime
creds = vinepair_creds()

#get processing/DB connection
proc  = DataProc(**creds)
proc.db_init()
db_session = proc.dbsession
db_session.create_lookups()
pterm_lookup = db_session.pterm_lookup
pindex_lookup = db_session.pindex_lookup
pi2slug = db_session.pi2slug


#Initial setup
#all dates in database
all_dt=proc.dt_list #all possible dates from database

#dates 
start_dt = dt.strptime(DATA_START_DATE,"%Y-%m-%d")
end_dt = dt.strptime(DATA_END_DATE,"%Y-%m-%d")
all_dt = list(dt for dt in all_dt if dt >= start_dt and dt <= end_dt)

#get aggregation scheme
agg1 = AggScheme()
filen = os.path.basename(AGG_CSV_PATH).split('.')[0].strip().upper()
if len(filen) == 0 or filen is None:
    filen = "Default"
agg1.get_agg_scheme(filen,pterm_lookup,csv_in=AGG_CSV_PATH)
agg1.show()
proc.get_index_matrix() # needed for fast indexing

#group by master cat wine,beer,spirit
master_cats = list(set(list(gdict["group_master"] for gdict in agg1.scheme["groups"])))
group_cat_lookup = {}
cat_group_lookup = {}
for gdict in agg1.scheme["groups"]:
    gname = gdict["group_name"]
    master = gdict["group_master"]
    group_cat_lookup[gname] = master
    cg = cat_group_lookup.get(master,[])
    cg.append(gname)
    cat_group_lookup[master] = cg
    
#calculate page weights
agg1.get_page_weights(proc)

#get pindex for all tracked pages
tracked_pages = agg1.get_tracked_pages()


mo_agg = TsArray.load_array("agg_mo")

for cat,groups in cat_group_lookup.iteritems():
    print cat,groups
    filename = JSON_OUTPUT+"_"+cat+"_scores.json"
    util.ts2json(mo_agg,"score_mo",groups,output_file=filename,start=EXPORT_START_DATE,end=EXPORT_END_DATE,all_caps=JSON_CAPS,frozen="score")
    filename = JSON_OUTPUT+"_"+cat+"_subscores.json"
    util.ts2json(mo_agg,"subscore_mo",groups,output_file=filename,start=EXPORT_START_DATE,end=EXPORT_END_DATE,all_caps=JSON_CAPS,frozen="subscore")

sys.exit()





