import sys,os,time,datetime
from vp_data import DataProc
from pv_ingest import Ingest
from vp_prop import vinepair_creds
ingest = Ingest()
creds = vinepair_creds()
###################
# NO USER INPUT REQUIRED

#  this script creates tables in the vinepair1 database
#  from the wordpress copy stored on the same server

##################
proc  = DataProc(**creds)
db_session = proc.dbsession
db_session.ingest_wp_db()
