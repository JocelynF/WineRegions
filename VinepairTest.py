import pandas as pd
import sqlalchemy as db
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float,String,DateTime,MetaData,Table
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, mapper, sessionmaker
import seaborn as sns


sql_connect1 =  'mysql+pymysql://root:harvey@127.0.0.1:3306/wordpress'
engine1 = db.create_engine(sql_connect1)#, echo = True)
dbconnect1 = engine1.connect()
wp_tax = pd.read_sql("select * from wp_term_taxonomy;", dbconnect1)
wp_terms = pd.read_sql("select * from wp_terms;", dbconnect1)
wp_term_relat = pd.read_sql("select * from wp_term_relationships;", dbconnect1)
wp.loc[4362,'parent'] = 4331 # makes Long Island a Subset of NY
wp = wp_tax.merge(wp_terms, on = 'term_id', how = 'outer')
wp_pageterms = wp_term_relat.merge(wp, on = 'term_taxonomy_id', how = 'left')


for i in range(1,2000):
    conn = engine.connect()
    #some simple data operations
    conn.close()
engine.dispose()


class PageWeights():
    
    def init(wp_pageterms):
        self.wp_pageterms = wp_pageterms

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
            for term_id in self.wp_pageterms[self.wp_pageterms['object_id']==80856]['term_taxonomy_id'].tolist():
                col = t2i[term_id]
                index_mat[row,col] = True
        return index_mat

    def get_mastervec(list_of_term_ids, master_name):
        mati = list(self.t2i[term] for term in list_of_term_ids if term in self.t2i) 
        col_mask = np.array(list(col in mati for col in range(self.index_mat.shape[1]))) #true where col is one of the tracked columns
        has_term = np.nansum(self.index_mat[:,col_mask],axis=1) > 0
        print(f'{master_name} number of pages: ',has_term[has_term==True].shape[0])
        tracked_pages = self.index_mat[has_term,:] #eliminate pages without term
        tracked_pages = tracked_pages[:,col_mask] #eliminate columns without term
        tracked_sum = np.nansum(tracked_pages,axis=0)
        tracked_norm = np.linalg.norm(tracked_sum)
        tracked_vec = tracked_sum/tracked_norm
        self.master_pages = self.has_term
        self.master_vec = tracked_vec
        self.master_terms = list_of_term_ids
        self.master_name = master_name
        return tracked_vec

    def get_group_vec(list_of_subterm_ids, group_name):
        list_all_terms = self.master_terms.extend(list_of_subterm_ids)
        self.group_pages = list(set(self.wp_pageterms[self.wp_pageterms['term_taxonomy_id'].isin(list_all_terms)]['object_id']))
        if len(self.group_pages)==0:
             print('No pages with both terms')
#         if len(mati) == 0:
        else:
            print(f'{group_name} number of pages: ',has_term[has_term==True].shape[0])        
            col_mask = np.array(list(col in self.master_terms for col in range(self.index_mat.shape[1]))) #true where col is one of the tracked columns
            has_term = np.nansum(self.index_mat[:,col_mask],axis=1) > 0
            sub_pages = self.index_mat[has_term,:] #eliminate pages without term
            sub_pages = sub_pages[:,col_mask] #eliminate columns without term
            sub_sum = np.nansum(sub_pages,axis=0)
            sub_norm = np.linalg.norm(sub_sum)
            sub_vec = sub_sum/sub_norm
            sub_vec = sub_vec
            #self.sub_terms = list_of_subterms
        return sub_vec
    
    
    def group_weight(group_dict, group_vec):
        """
        group_dict should have the name of the group and the list of subterm_ids
        """
        weight_matrix = np.zeros((len(self.master_pages), len(group_dict)))
        tracked_pindex = list(self.i2p[i] for i in np.nonzero(self.master_pages)[0])
        group_keys = list(group_dict.keys())
        self.w_p2r = dict(list((pindex,pi) for pi,pindex in enumerate(tracked_pindex))) #pindex : pi
        self.w_r2p = dict(list((pi,pindex) for pi,pindex in enumerate(tracked_pindex))) #pi : pindex 
        self.w_g2c = dict(list((group,ci) for ci,group in enumerate(group_keys))) #group name, column
        self.w_c2g = dict(list((ci,group) for ci,group in enumerate(group_keys))) #column, group name
        for group_name in group_keys:
            matc = self.w_g2c[group_name] #get matrix column for group
            group_page_vec = self.get_group_vec(group_dict[group_name], group_name) #must run this here to get the self.group_pages below
            group_all_sim = np.dot(group_page_vec,self.master_vec) 
            group_pi = list(self.p2i[pindex] for pindex in self.group_pages) #list of rows for each pindex in the group pages
            group_submat = self.index_mat[group_pi,:] #the rows for all the group pages and all columns
            group_submat = group_submat[:,col_mask]  #eliminate non tracked tindexes
            if group_submat.size == 0:
                print(f'{group_name} is not in {self.master_name}'
            for pindex in self.group_pages:
                page_row = index_mat[self.p2i[pindex]].copy()
                page_row = page_row[col_mask]
                page_total = np.nansum(page_row).astype(np.float64)
                pr_len = np.linalg.norm(page_row)
                page_vec = page_row/pr_len
                #cos1 = np.dot(self.master_vec,page_vec)
                cos2 = np.dot(group_page_vec,page_vec)
                net_diff = np.clip(cos2-group_all_sim,0.05,1.0) #WEIGHTS
                matr = self.w_p2r.get(pindex,None)
                if matr is None:
                    continue
                self.weight_matrix[matr,matc] = net_diff
        return weight_matrix













