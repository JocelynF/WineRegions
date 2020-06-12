import sys,os,time,datetime,pickle,copy
import numpy as np
#local imports
from vp_data import DataProc,AggScheme,AggFunc,TsArray
from vp_search import SearchTerms
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
# Either end date can be "TODAY" 
# Either start data can be an integer for # of days prior (e.g. 7 = 1 week before target_end)
# be sure not to quote integer (i.e. 7 not "7")

DATA_START_DATE="2019-01-01"
DATA_END_DATE="2019-12-31" # Can Be TODAY
EXPORT_START_DATE="2019-01-01"
EXPORT_END_DATE="2019-12-31" # Can Be TODAY

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
SEARCH_WEIGHT = 0.16

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
stdata = SearchTerms()
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
end_dt = util.din2dt(DATA_END_DATE)
exp_end_dt = util.din2dt(EXPORT_END_DATE)
start_dt = util.din2dt(DATA_START_DATE,end_dt=end_dt)
exp_start_dt = util.din2dt(EXPORT_START_DATE,end_dt=end_dt)
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

#get all data in tsarray format
#use local data if selected
pv_data = TsArray(tracked_pages,all_dt)
to_update = [PAGE_CATEGORY,]
if SUBTRACT_SEV:
    to_update.append("scroll_events")
for update in to_update:
    aggfunc.update_tsa(proc,pv_data,update,local=USE_LOCAL_COPY,file="pagedata")

if SUBTRACT_SEV:
    no_scroll = np.isnan(pv_data.arrays["scroll_events"])
    with_scroll = np.invert(no_scroll)
    net_array = pv_data.arrays[PAGE_CATEGORY].copy()
    net_array[with_scroll] = net_array[with_scroll] - pv_data.arrays["scroll_events"][with_scroll]
    pv_data.add_array(net_array,"net_pv")
else:
    pv_data.arrays["net_pv"] = pv_data.arrays[PAGE_CATEGORY]

#save ts data as local pkl file
pv_data.store_array("pagedata")

#   Data accumulation and processing
#

#TSA for aggregated categories
pv_agg = TsArray(group_cat_lookup.keys(),all_dt)
self_agg = agg1.get_selfagg()

#search terms
model_tsa = TsArray.load_array("st_imputed")
pv_agg.merge_tsarray(model_tsa,"imputed",new_name="imputed")
stdata.get_search_data(db_session,agg1,pv_agg,SEARCH_TRACK,local=True,file="agg",imputed="imputed")

#filter outliers
outlier_subs = aggfunc.flag_outliers(agg1,pv_data,"net_pv",cutoff=DAY_CUT,sigcut=SIG_CUT,hardcut=HARD_CUT)
outlier_mask = outlier_subs > 0.01
filt_array = pv_data.arrays["net_pv"].copy()
filt_array[outlier_mask] = outlier_subs[outlier_mask]
pv_data.add_array(outlier_subs,"outliers")
plotter = Plotter()
out_plist=plotter.get_outlier_plot(pv_data,"net_pv","outliers",name_lookup=pi2slug)
fig1 = plotter.make_plot(out_plist,master_title="Outliers")

sys.exit()








