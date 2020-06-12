import sys,os,time,datetime
from vp_data import DataProc
from pv_ingest import Ingest,Utils
from vp_prop import vinepair_creds
ingest = Ingest()
creds = vinepair_creds()
###################
# USER INPUT

# Update mode
# SYNC = fill in fields missing from date range
# PULL = ingest data in date range, even if already in the database
#      --> duplicates will be removed after ingest (slow)

UPDATE_MODE = "SYNC"

# Put target tates here in YYYY-mm-dd format
# or use "TODAY" for target_end
# target_start can be an integer for # of days prior (e.g. 7 = 1 week before target_end)
# be sure not to quote integer (i.e. 7 not "7")
target_start="2019-12-01"
target_end="TODAY"

# What Queries to update (Python Bool = True/False)
UPDATE_PV = True #Pageviews (includes sessions, duration, etc)
UPDATE_EV = True #Scroll Events
UPDATE_SP = True #Search data by Page (stored, not currently used?)
UPDATE_ST = True #Search Terms (clicks, impressions, etc. from search API)



# END USER INPUT
##################
util = Utils()
if UPDATE_MODE not in ("SYNC","PULL"):
    print "ERROR, UPDATE_MODE must be either SYNC or PULL"
    sys.exit()

update_tags = []    
for utag,code in ((UPDATE_PV,"pv"),(UPDATE_EV,"ev"),(UPDATE_SP,"sp"),(UPDATE_ST,"st")):
    if utag:
        update_tags.append(code)
        
end_dt = util.din2dt(target_end)
start_dt = util.din2dt(target_start,end_dt=end_dt)
target_ndays = (end_dt - start_dt).days + 1
target_dt_list =  list(start_dt + datetime.timedelta(days=i) for i in range(target_ndays))
proc  = DataProc(**creds)
proc.db_init()
db_session = proc.dbsession
db_session.create_lookups()


if UPDATE_MODE == "SYNC":
    #check day-by-day, each field for missing data

    missing_pv = []
    missing_sp = []
    missing_ev = []
    missing_st = []

    #check pagedata
    for field in ("sessions","pageviews","uniquePageviews","avgSessionDuration","entrances","bounceRate","exitRate"):
        print "Checking data completeness for",field
        sql = 'SELECT DISTINCT date FROM pagedata WHERE `key`="%s"' % field
        with_data= list(tup[0] for tup in db_session.session.execute(sql).fetchall())
        missing = set(target_dt_list) - set(with_data)
        missing_pv.extend(list(missing))
    #check search page results
    for field in ("clicks","ctr","impressions","position"):
        print "Checking data completeness for",field
        sql = 'SELECT DISTINCT date FROM pagedata WHERE `key`="%s"' % field
        with_data= list(tup[0] for tup in db_session.session.execute(sql).fetchall())
        missing = set(target_dt_list) - set(with_data)
        missing_sp.extend(list(missing))
    #check events results
    for field in ("scroll_events",):
        print "Checking data completeness for", field
        sql = 'SELECT DISTINCT date FROM pagedata WHERE `key`="%s"' % field 
        with_data = list(tup[0] for tup in  db_session.session.execute(sql).fetchall())
        missing = set(target_dt_list) - set(with_data)
        missing_ev.extend(list(missing))
    #check for search terms, only kept at google for 16-18mo.
    for field in ("clicks","ctr","impressions","position"):
        print "Checking data completeness for",field
        sql = 'SELECT DISTINCT date FROM searchdata WHERE `key`="%s"' % field
        with_data = list(tup[0] for tup in db_session.session.execute(sql).fetchall())
        missing = set(target_dt_list) - set(with_data)
        missing_st.extend(list(missing))

    #uniquify, omit search queries older than 540 days (about 18mo)
    #               event queries are stored for 26mo
    missing_pv = sorted(list(set(missing_pv)))
    missing_sp = sorted(list(dt for dt in set(missing_sp) if (datetime.datetime.today()-dt).days < 540))
    missing_ev = sorted(list(dt for dt in set(missing_ev) if (datetime.datetime.today()-dt).days < 800))
    missing_st = sorted(list(dt for dt in set(missing_st) if (datetime.datetime.today()-dt).days < 540))

if UPDATE_MODE == "PULL":
    missing_pv = target_dt_list
    missing_sp = target_dt_list
    missing_ev = target_dt_list
    missing_st = target_dt_list 
    
#ingest data identified above
for data_tag,target_dt_list in (("pv",missing_pv),("ev",missing_ev),("sp",missing_sp),("st",missing_st)):
    if data_tag in update_tags:
        print "Ingesting %g days of data for %s:" % (len(target_dt_list),data_tag)
        ingest.get_google_data(db_session,target_dt_list,db_session.pindex_lookup,targets=[data_tag,])

# if data pulled, duplicates are removed, keeping the most recent entries
if UPDATE_MODE == "PULL":
    db_session.deduplicate_sql()

