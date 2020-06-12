import sys,os,time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, Float,String,DateTime,MetaData,Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists,create_database
#local modules
from pv_ingest import Ingest
from pv_ingest import Utils
from vp_prop import vinepair_creds

#instantialize classes
ingest = Ingest()
util = Utils()
creds = vinepair_creds()

Base_write = declarative_base()

# class definitions for sqlalchemy
class Slug(Base_write):
    __tablename__ = 'pindex'
    id = Column(Integer, primary_key=True)
    slug = Column(String(length=256),index=True)
    pindex = Column(Integer)
    def __repr__(self):
        return self.slug

class Term(Base_write):
    __tablename__ = 'tindex'
    id = Column(Integer, primary_key=True)
    term = Column(String(length=256))
    tindex = Column(Integer,index=True)
    def __repr__(self):
        return self.term
    
class Type(Base_write):
    __tablename__ = 'ttype'
    id = Column(Integer, primary_key=True)
    ttype = Column(String(length=256))
    tindex = Column(Integer,index=True)
    def __repr__(self):
        return self.ttype

class PageTerms(Base_write):
    __tablename__ = 'pterms'
    id = Column(Integer, primary_key=True)
    termstr = Column(String(length=4096),default="")
    pindex = Column(Integer,index=True)
    def __repr__(self):
        return self.termstr
    
class Pagedata(Base_write):
    __tablename__ = "pagedata"
    id = Column(Integer, primary_key=True)
    pindex = Column(Integer)
    key = Column(String(length=256))
    date = Column(DateTime)
    count = Column(Float)

class Stermdata(Base_write):
    __tablename__ = "searchdata"
    id = Column(Integer, primary_key=True)
    key = Column(String(length=256))
    sterm = Column(String(length=256))
    date = Column(DateTime)
    count = Column(Integer)

class DBsession:
    """
    Class with sqlalchemy connection engine and dictionaries for page tag lookups
    """
    def __init__(self,mysql_login,mysql_pass,mysql_host,mysql_port,database):
        self.login = mysql_login
        self.password = mysql_pass
        self.port = mysql_port
        self.host = mysql_host
        sql_connect  =  'mysql://%s:%s@%s:%s/%s' % (mysql_login,mysql_pass,mysql_host,mysql_port,database)
        self.engine = create_engine(sql_connect) 
        self.Base = declarative_base()
        self.Base.metadata.create_all(self.engine)
        self.session = Session(self.engine)
        print "Created Database Session:",database
        self.pindex_lookup = None
        self.pterm_lookup = None
        self.tname_lookup = None
        self.ttype_lookup = None


    def create_lookups(self):
        """
        reconstruct dictionaries from SQL
        pindex_lookup: slug (str) --> pindex (page identifier int)
        pterm_lookup: pindex (int) --> pterms (terms associated with a page, serialized list)
        tname_lookup: tindex (int) --> term (name of term str)
        ttype_lookup: tindex (int) --> ttype (taxonomy tag for term str)
        """
        pindex_query = self.session.execute("SELECT slug,pindex FROM pindex")
        self.pindex_lookup = {}
        for result in pindex_query:
            if result[0] != "null":
                self.pindex_lookup[result[0]] = result[1]

        pterm_query = self.session.execute("SELECT pindex,termstr FROM pterms")
        self.pterm_lookup = {}
        for result in pterm_query:
            termstr = result[1]
            tlist = termstr.split(',')
            tlist = list(int(term.strip()) for term in tlist)
            self.pterm_lookup[result[0]] = tlist

        self.tname_lookup = {}
        tname_query = self.session.execute("SELECT tindex,term FROM tindex")
        for result in tname_query:
            self.tname_lookup[result[0]] = result[1]


        self.ttype_lookup = {}
        ttype_query = self.session.execute("SELECT tindex,ttype FROM ttype")
        for result in ttype_query:
            self.ttype_lookup[result[0]] = result[1]

        #reverse lookup for slug
        self.pi2slug = {}
        for slug,pindex in self.pindex_lookup.iteritems():
            self.pi2slug[pindex] = slug

            
    def ingest_wp_db(self):
        """
        Extracts Wordpress data from database vp_wp_data
        and creates tables in vinepair1 
        Database vp_wp_data must exist and be populated from an sqldump
        which can be done using the script /root/bin/ingest_wp.sh
        on the remote host

        Uses two database sessions
           self.session is established connection to vinepair1
           read_session is created to read vp_wp_data
        
        """
        wp_db = "vp_wp_data"
        #direct read
        read_connect  =  'mysql://%s:%s@%s:%s/%s' % (self.login,self.password,self.host,self.port,wp_db)
        read_engine = create_engine(read_connect)
        Base_read = automap_base()
        Base_read.prepare(read_engine, reflect=True)
        read_session = Session(read_engine)
        Base_read.metadata.create_all(read_engine)

        print "Dropping existing Index Tables"
        for table in ('pindex','tindex','ttype','pterms'):
            self.session.execute("DROP TABLE IF EXISTS %s" % table)
        meta = MetaData(self.engine)
        Table('pindex', meta ,Column('id', Integer, primary_key = True), 
              Column('slug', String(length=256)), Column('pindex', Integer))
        Table('tindex', meta ,Column('id', Integer, primary_key = True), 
              Column('term', String(length=256)), Column('tindex', Integer))
        Table('ttype', meta ,Column('id', Integer, primary_key = True), 
              Column('ttype', String(length=256)), Column('tindex', Integer))
        Table('pterms', meta ,Column('id', Integer, primary_key = True), 
              Column('termstr', String(length=4096)), Column('pindex', Integer))
        meta.create_all(self.engine)

        #pindex
        Pages= Base_read.classes.wp_po_plugins
        pages = read_session.query(Pages)
        toadd = []
        for page in pages:
            slug = util.link2slug([page.permalink])
            index = page.post_id
            record = Slug(slug=slug,pindex=index)
            toadd.append(record)
        self.session.bulk_save_objects(toadd)
        self.session.commit()

        #pterms
        Pages = Base_read.classes.wp_term_relationships
        pages = read_session.query(Pages)
        #first store as dictionary of lists
        toadd = []
        pterm_lookup = {}
        for page in pages:
            pindex=page.object_id
            tindex=page.term_taxonomy_id
            pterm_list = pterm_lookup.get(pindex,[])
            pterm_list.append(tindex)
            pterm_lookup[pindex] = pterm_list
        #next, convert lists to strings and store
        for pindex,tlist in pterm_lookup.iteritems():
            termstr = ",".join(str(term) for term in tlist)
            record=PageTerms(pindex=pindex,termstr=termstr)
            toadd.append(record)
        self.session.bulk_save_objects(toadd)
        self.session.commit()

        #tindex
        Pages = Base_read.classes.wp_terms
        pages = read_session.query(Pages)
        toadd = []
        for page in pages:
            tindex = page.term_id
            tname = page.name
            record = Term(tindex=tindex,term=tname)
            toadd.append(record)
        self.session.bulk_save_objects(toadd)
        self.session.commit()

        #ttypes
        Pages = Base_read.classes.wp_term_taxonomy
        pages = read_session.query(Pages)
        toadd = []
        for page in pages:
            tindex = page.term_id
            ttype= page.taxonomy
            record = Type(tindex=tindex,ttype=ttype)
            toadd.append(record)
        self.session.bulk_save_objects(toadd)
        self.session.commit()

        print "Created Pindex,Tindex,Pterm, and Ttype tables from Wordpress DB"
    
    def index_columns(self):
        check_tables = list(tup[0] for tup in self.session.execute("SHOW TABLES").fetchall())
        for table in ("pagedata","searchdata"):
            if table not in check_tables:
                print "Error: %s table does not exist!" % table
            else:
                check_idx = self.session.execute("SHOW INDEX FROM %s" % table).fetchall()
                check_keys = set(list(tup[2][4::] for tup in check_idx))
                if table == "pagedata":
                    mterm = "pindex"
                else:
                    mterm = "sterm"
                col_list = (mterm,'date','key')
                #check/create single key columns
                for col in col_list:
                    if col not in check_keys:
                        if col == "key":
                            col = "`key`"
                        cstr = col.replace('`',"")
                        print "Creating index on:",cstr
                        sql = "CREATE INDEX idx_%s ON %s(%s)" % (cstr,table,col)
                        self.session.execute(sql)
                #add composite column keys
                for i in range(len(col_list)):
                    for j in range(i+1,len(col_list)):
                        col1 = col_list[i]
                        col2 = col_list[j]
                        if col1 == "key":
                            col1 = "`key`"
                        if col2 == "key":
                            col2 = "`key`"
                        comp = col1+","+col2
                        cstr = col_list[i]+"_"+col_list[j]
                        cstr = cstr.replace('`',"")
                        if cstr not in check_keys:
                            print "Creating index on:",cstr
                            sql = "CREATE INDEX idx_%s ON %s(%s)" % (cstr,table,comp)
                            self.session.execute(sql)
                all_col = mterm+",date,`key`"
                all_str = "_".join(col_list)
                if all_str not in check_keys:
                    print "Creating index on:",all_str
                    sql = "CREATE INDEX idx_%s ON %s(%s)" % (all_str,table,all_col)
                    self.session.execute(sql)
                    
    def deduplicate_sql(self):
        """
        ingest may duplicate values, keep values with max id (latest added)
        slow, perhaps too many indexes on database, but functional
        """
        for table in ("pagedata","searchdata"):
            if table == "pagedata":
                mterm = "pindex"
                print "Removing Duplicates from pagedata"
            else:
                mterm = "sterm"
                print "Removing Duplicates from searchdata"
            #much faster to go by date
            sql = "SELECT DISTINCT date FROM %s ORDER BY date ASC" % table
            result = self.session.execute(sql).fetchall()
            dates = list(t[0] for t in result)
            id_to_del = [] #primary key
            for di,date in enumerate(dates):
                #first generate list of records matching all fields where id < max(id)
                dstr = '"'+date.strftime("%Y-%m-%d %H:%M:%S")+'"'
                sql = "WITH indate AS (SELECT * from %s where date=%s)," % (table,dstr) + \
                      "duplicates AS (SELECT MAX(id) AS lastId,%s,`key` FROM indate GROUP BY %s,`key` HAVING count(*) > 1)," % (mterm,mterm) + \
                      "dupId AS (SELECT id FROM indate INNER JOIN duplicates on indate.%s = duplicates.%s " % (mterm,mterm) + \
                      "AND indate.`key` = duplicates.`key` WHERE indate.id < duplicates.lastId) SELECT * FROM dupId" 
                result = self.session.execute(sql).fetchall()
                id_list = list(t[0] for t in result)
                if len(id_list) > 0:
                    id_to_del.extend(id_list)
            id_to_del = list(set(id_to_del)) #uniquify, should be already
            
            if len(id_to_del) > 0:
                print "Found %g duplicates, removing . . . " % len(id_to_del)
                #use small batches for delete to avoid memory/deadlock issues
                batch_size = min((len(id_to_del),200))
                for i in range(0,len(id_to_del),batch_size):
                    to_del = id_to_del[i:i+batch_size]
                    as_sql = "("+",".join(list("%d" % idx for idx in to_del))+")"
                    sql = "DELETE FROM %s WHERE id IN %s" % (table,as_sql)
                    self.session.execute(sql)
                    self.session.commit()
                print "   --> %g records removed!" % len(id_to_del)

            
    def store_data(self,ddlist,flist,sql_timestr,frame_type):
        """
        Takes a list of data dictionaries from ingest
           flist = list of queriy columns to store in db as `key`
           sql_timestr = date as string for sql
           frame_type = pageviews,events, or search
              dictates which table data are stored in
        """
        if len(ddlist) == 0:
            return
        toadd = []
        for ddict in ddlist:
            if frame_type in ["pageviews","events","search"]:
                slug = ddict["slug"]
                if slug=="null":
                    continue
                pindex = self.pindex_lookup.get(slug,-1)
                if pindex < 0:
                    continue
                for field in flist:
                    if frame_type == "pageviews":
                        fname = "ga:"+field
                    elif frame_type == "events":
                        fname = "scroll_events"
                    elif frame_type == "search":
                        fname = field
                    else:
                        print "Error, cannot store %s, key not understood" % field
                        continue
                    count = float(ddict[fname])
                    record = Pagedata(pindex=pindex,key=field,count=count,date=sql_timestr)
                    toadd.append(record)
            elif frame_type == "searchterm":
                #search terms slugs are not page slugs, but search queries instead
                sterm = ddict["slug"]
                if len(sterm) > 250: #hack to truncate long searches
                    sterm = sterm[0:250]
                for field in flist:
                    count = ddict[field]
                    record = Stermdata(sterm=sterm,key=field,count=count,date=sql_timestr)
                    toadd.append(record)
            else:
                print "ERROR, frame_type not understood!"
                return
        self.session.bulk_save_objects(toadd)
        self.session.commit()
