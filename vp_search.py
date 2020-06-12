import os,sys,re
import numpy as np
from vp_data import TsArray
#imports for txt cleaning
import ftfy 
import unicodedata
import string
from fuzzywuzzy import fuzz,process

#list of words common in searches but not useful for matching
STOPWORDS = ["us","which","high","use","before","no","by","de","an","should","your",
             "have","that","it","bad","much","years","when","like","whats","i","out",
             "or","old","many","at","from","get","where","cheap","most","are","popular",
             "new","why","buy","gin","do","vs","good","can","does","and","you","on",
             "drink","with","the","how","for","a","of","what","to","in","is","best"]

#common mis-spellings or synonyms
SUBSTITUTIONS = {"whisky":"whiskey",
                 "wiskey":"whiskey",
                 "tennesee":"tennessee",
                 "tennesse":"tennessee",
                 "tennesseee":"tennessee",
                 "tennesy":"tennessee",
                 "vinho verde":"vin verde",
                 "zinfandal":"zinfandel",
                 "zinfadel":"zinfandel",
                 "zinfendel":"zinfandel",
                 "zinfendal":"zinfandel",
                 "zinfindel":"zinfandel",
                 "tequilla":"tequila",
                 "tiquila":"tequila",
                 "procecco":"prosecco"}



STOPWORDS = list(u"%s" % word.lower() for word in STOPWORDS)

class SearchTerms():
    def __init__(self):
        pass

    def clean_word(self,word_as_str):
        """
        Input is string (could be other than a word also)
        --> decoded as utf-8, unicode cleaned, accents, diacritics, and punctuation removed
        Output is unicode
        """
        s_word = word_as_str.strip().decode('utf-8')
        fixed = ftfy.fix_text(s_word) #fix bad unicode
        nfkd_form = unicodedata.normalize('NFKD', fixed) #decompose to letters and diacritics
        cleaned = list(c for c in nfkd_form if unicodedata.category(c) not in ["Mn","Lo","Lm"])
        stripped = list(c for c in cleaned if c not in string.punctuation)
        #return unicode string
        return u"".join(stripped)

    def clean_phrase(self,phrase,use_stop=False,use_subs=False):
        """
        Takes a string, splits on whitespace, optionally filter by stopwords and substitutions
        each split field is passed to clean word above
        """
        
        words = list(w.lower() for w in phrase.strip().split() if len(w)>0)
        fixed_words = list(self.clean_word(w) for w in words)
        if use_stop:
            fixed_words = list(word for word in fixed_words if word not in STOPWORDS)
        if use_subs:
            fixed_words = list(SUBSTITUTIONS.get(word,word) for word in fixed_words)
        return " ".join(list(w.encode('utf-8') for w in fixed_words))

    def match_terms(self,phrase,match_dict,nm_dict,fuzzy=False,prune_dict=None):
        """
        Input is phrase and two dictionaries, the first is key-->group where key is a
        matching term (can be multi-word) and group is the assigned group taken from 
        aggregation scheme (csv input).  Second dictionary is the same but for not-matches
        or excluded terms (e.g. whiskey should not match japanese whiskey, etc.)
        Options:
            fuzzy (bool) = fuzzy matching (requires fuzzywuzzy), very untested.
            prune_dict (dict) = key is match, val is to exclude
                usually master categories .  Example gamay--> wine would exclude
                the match for "wine" but keep the match for "gamay" for a search
                that included "gamay wine"
        """
        matches = list(match_dict.keys())
        not_matches = list(nm_dict.keys())
        mgroups = []
        nmgroups = []
        for match in matches+not_matches:
            to_check = re.compile(r"(\b)(%s)s?(\b)" % match.strip())
            found_match = to_check.search(phrase)
            if found_match:
                matched = found_match.groups(1)[1]
                if matched in matches:
                    mgroups.append(match_dict[matched])
                if matched in not_matches:
                    nmgroups.append(nm_dict[matched])
        direct_match = list(set(mgroups) - set(nmgroups))
        #optional fuzzy matching if direct match not found
        if len(direct_match) == 0:
            if fuzzy:
                try:
                    fz_matches = process.extract(phrase,matches)
                    fz_matches.sort(key = lambda t:t[1],reverse=True)
                    best_fz = fz_matches[0]
                    if best_fz[1] > 89: #cutoff for nonsense
                        gmatch = best_fz[0].strip()
                        matchg = match_dict[gmatch]
                        if matchg not in nmgroups:
                            return [matchg,]
                        else:
                            return []
                    else:
                        return []
                except UnicodeDecodeError:
                    print "Encoding Error for phrase",phrase
                    return []
                except:
                    print "Unknown FuzzyWuzzy Error!",phrase
                    return []
            else:
                return []
        elif len(direct_match) == 1:
            return direct_match
        else: #multiple terms matched?
            if prune_dict is not None: #use passed dict to prune term matches
                to_remove = list(prune_dict.get(dm,None) for dm in direct_match if prune_dict.get(dm,None) is not None)
                to_keep = list(dm for dm in direct_match if dm not in to_remove)
            else:
                to_keep = direct_match
            return to_keep

    def get_searchterm_counts(self,agg_scheme,db_session,dt_list,target="clicks",verbose=False):
        """
        for all terms in an aggregation scheme, tally and weight hits from search queries
        by day, convert to time_series, returns a dict of gterm-->[dates,values]
        """
        search_counts = {}
        ugroup_names = set(gdict["group_name"].lower() for gdict in agg_scheme.scheme["groups"])
        masters = list(set(gdict["group_master"].lower() for gdict in agg_scheme.scheme["groups"]))
        master_lookup = {}
        match_dict = {}
        not_match_dict={}
        for gdict in agg_scheme.scheme["groups"]:
            gname = gdict["group_name"]
            master = gdict["group_master"]
            master_lookup[gname] = master
            matches = gdict["st_inc"]
            for match in matches:
                match_dict[match] = gname
            not_matches = gdict["st_ex"]
            for match in not_matches:
                not_match_dict[match] = gname
        print "Fetching Search Term Data for",target
        term_cache={}
        for dt in dt_list:
            sql_dt = dt
            sql = 'SELECT sterm,count FROM searchdata WHERE `key`="%s" and date="%s"' % (target,sql_dt)
            results = db_session.session.execute(sql).fetchall()
            sterms = list(self.clean_phrase(tup[0],use_stop=True,use_subs=True) for tup in results)
            scounts = list(tup[1] for tup in results)
            term_counts = dict((gdict["group_name"],0.0) for gdict in agg_scheme.scheme["groups"])
            matched=False
            term_cache = {}
            not_matched = []
            matched = []
            for pi,phrase in enumerate(sterms):
                match_list = term_cache.get(phrase,self.match_terms(phrase,match_dict,not_match_dict,prune_dict=master_lookup))
                term_cache[phrase] = match_list
                if len(match_list) == 0:
                    not_matched.append((phrase,scounts[pi]))
                    continue
                matched.append((phrase,scounts[pi],match_list))
                count = scounts[pi]/float(len(match_list))
                for gname in match_list:
                    term_counts[gname] += count
            if verbose: #for testing
                for p,c,ml in matched:
                    print "MATCH",c,p,"||"," ".join(ml)
                for p,c in not_matched:
                    print "NOMATCH",c,p
            search_counts[dt] = term_counts
        #search_counts is dictionary by date, convert to timeseries
        search_ts = {}
        for gdict in agg_scheme.scheme["groups"]:
            gname = gdict["group_name"]
            g_counts = list(search_counts[dt].get(gname,0.0) for dt in dt_list)
            search_ts[gname.upper()] = (dt_list,g_counts)
        return search_ts



    def get_search_data(self,db_session,agg_scheme,target_tsa,search_track,local=False,file=None,imputed=None):
        target_rows = target_tsa.p2r.keys()
        target_dt = target_tsa.dt_list
        if search_track in target_tsa.arrays.keys():
            st_data = target_tsa.arrays[search_track]
            missing_col = list(i for i in range(st_data.shape[1]) if np.all(np.isnan(st_data[:,i])))
            has_data = list(target_tsa.c2d[i] for i in range(st_data.shape[1]) if i not in missing_col)
            print "Existing data found for %g days" % len(has_data)
            missing_dt = list(set(target_dt) - set(has_data))
        else:
            st_data = target_tsa.new_array()
            target_tsa.add_array(st_data,search_track)
            missing_dt = target_dt[:]
            print "No existing searchterm data in current arrays"
        if len(missing_dt) == 0:
            print "Searchterm data complete"
            return
        
        if imputed is not None:
            dest = target_tsa.arrays[search_track]
            imp_st = target_tsa.arrays[imputed]
            imp_col = list(i for i in range(imp_st.shape[1]) if np.all(np.isnan(imp_st[:,i])))
            copy_col = list(i for i in imp_col if target_tsa.c2d[i] in missing_dt)
            print "Copying imputed searchterm data for %g days" % len(copy_col)
            for i in copy_col:
                dest[:,i] = imp_st[:,i]
                
        #get local copy
        if local and file is not None:
            tsa_in = TsArray.load_array(file)
            if search_track in tsa_in.arrays:
                target_tsa.merge_tsarray(tsa_in,search_track)
                st_arr = target_tsa.arrays[search_track]
                missing_col = list(i for i in range(st_arr.shape[1]) if np.all(np.isnan(st_arr[:,i])))
                has_data = list(target_tsa.c2d[i] for i in range(st_arr.shape[1]) if i not in missing_col)
                missing_dt = list(set(missing_dt) - set(has_data))
        print "Calculating searchterm data from query data for %g days" % len(missing_dt)
        st_dict = self.get_searchterm_counts(agg_scheme,db_session,missing_dt,target=search_track)
        target_tsa.insert_by_dict(search_track,st_dict)


    def impute_search_data(self,source_tsa,st_array,alpha=1000.0):
        """
        Uses other metrics to impute search data for dates out of range
        For dates with search data and all pageview data, a model is trained
        using all page data for ALL categories using lasso regression, the
        model is then used to predict missing data.
        """
        from sklearn import linear_model
        all_dt = source_tsa.dt_list
        st_data = source_tsa.arrays[st_array]
        col_with_st = np.count_nonzero(np.invert(np.isnan(st_data)),axis=0)
        
        arrays = ["pageviews","avgSessionDuration","bounceRate","exitRate"]
        valid_dt = np.ones(len(all_dt),dtype=np.bool_)
        for arr in arrays:
            arrn="raw_agg_"+arr
            arr_dat = source_tsa.arrays.get(arrn,None)
            if arr_dat is None:
                print "Missing data for imputation, aborting!"
                return
            with_dat =  np.count_nonzero(np.invert(np.isnan(arr_dat)),axis=0)
            valid_dt = np.logical_and(valid_dt,with_dat)
        to_model = np.logical_and(valid_dt,col_with_st)
        model_coli = list(np.nonzero(to_model)[0])
        timepts = len(model_coli)
        print "Using %g timepoints for imputation model" % len(model_coli)
        mod_start = model_coli[0]
        test_n = int(len(model_coli)*0.1)
        inmod_i = list(i for i in range(timepts))
        test_i = np.random.choice(inmod_i,size=test_n)
        print "using test size of %g for %g timepoints" % (test_n,timepts)
        train_i = list(i for i in inmod_i if i not in test_i)
        st_target = st_data[:,to_model]
        st_dt = list(all_dt[i] for i in model_coli)
        st_dow = list(dt.weekday() for dt in st_dt)
        st_doy = list(int(dt.strftime("%j")) for dt in st_dt)
        all_doy = list(int(dt.strftime("%j")) for dt in all_dt)
        all_dow = list(dt.weekday() for dt in all_dt)
        row_sums = np.nansum(st_target,axis=1)
        valid_col = row_sums > 0.0
        arr_list = []
        all_data = []
        for i in range(st_target.shape[0]):
            idat = np.zeros((len(arrays),timepts))
            alldat = np.zeros((len(arrays),len(all_dt)))
            for ki,key in enumerate(arrays):
                key_data = source_tsa.arrays["raw_agg_"+key][i,to_model]
                alldat[ki] = source_tsa.arrays["raw_agg_"+key][ki]
                idat[ki,:] = key_data
            arr_list.append(idat)
            all_data.append(alldat)
        nf = len(arrays)
        n_rows = len(arr_list)*nf+3
        master_array = np.zeros((n_rows,timepts))
        all_data_array = np.zeros((n_rows,len(all_dt)))
        for ai,arr in enumerate(arr_list):
            master_array[nf*ai:nf*ai+nf:,:] = arr
            all_data_array[nf*ai:nf*ai+nf:,:] = all_data[ai]
        #master_array[-3] = st_doy
        #master_array[-2] = st_dow
        #all_data_array[-3] = all_doy
        #all_data_array[-2] = all_dow
        all_sums = np.nansum(master_array,axis=1)
        all_means = np.nanmean(master_array,axis=1)
        all_std = np.clip(np.nanstd(master_array,axis=1),1.0,np.inf)
        all_z = np.divide(np.subtract(master_array,all_means[:,None]),all_std[:,None])
        ad_sums = np.nansum(all_data_array,axis=1)
        ad_means = np.nanmean(all_data_array,axis=1)
        ad_std = np.nanstd(all_data_array,axis=1)
        ad_z = np.divide(np.subtract(all_data_array,ad_means[:,None]),ad_std[:,None])
        resid_sums = []
        work_sums = []
        zero_sums = []
        coeff_sums = []
        imputed = source_tsa.new_array()
        for rowi,row in enumerate(st_target):
            if np.nansum(st_target[rowi])==0:
                continue
            spikes = np.array(st_target[rowi] >= 0.7*np.amax(st_target[rowi]),dtype=np.int64)
            spki = np.nonzero(spikes)[0]
            in_st = list(st_doy[i] for i in spki)
            master_array[-1] = list(doy in in_st for doy in st_doy)
            all_data_array[-1] = list(doy in in_st for doy in all_doy)
            master_array[-1] = master_array[-1] * 0.5*np.amax(st_target[rowi])
            all_data_array[-1] = all_data_array[-1] * 0.5*np.amax(st_target[rowi])
            X_train = master_array[:,train_i].T
            X_test = master_array[:,test_i].T
            y = st_target[rowi]
            ym = np.nanmean(y)
            ystd1 = np.nanstd(y)
            y = np.clip(y,0.0,ym+10.0*ystd1)
            if np.nansum(y) == 0:
                ys = y
            else:
                ys = np.zeros(y.shape)
                win = 7
                for w in range(3,len(ys)-3,1):
                    ys[w] = np.median(y[w-3:w+3])
                for f in range(3):
                    ys[f] = ys[3]
                    ys[-f-1] = ys[-4]
            y_train = ys[train_i].T
            y_test = ys[test_i].T
            reg = linear_model.Lasso(fit_intercept=True,alpha=alpha).fit(X_train, y_train)
            pred1 = reg.predict(X_test)
            pred2 = reg.predict(X_train)
            pred_all = reg.predict(all_data_array.T)
            pmean = np.nanmean(pred_all[0:mod_start])
            pstd = np.nanstd(pred_all[0:mod_start])
            st_std = np.nanstd(ys)+1
            sratio = st_std/pstd
            pscale = pred_all*sratio
            ijunct = np.nanmedian(pscale[mod_start-30:mod_start])
            sjunct = np.nanmedian(y[0:30:])
            diff = sjunct-ijunct
            pclipped = np.clip(pscale+diff,0.0,np.inf)
            ramp_down = np.linspace(0.0,1.0,mod_start)
            ramp_scale = np.ones(len(all_dt))
            ramp_scale[0:mod_start] = ramp_down
            pramp = np.multiply(pclipped,ramp_scale)
            imputed[rowi] = pramp
            imputed[rowi,mod_start::]=np.nan
            test = np.abs(y_test-pred1)
            work = np.abs(y_train-pred2)
            ysum = np.nansum(ys)
            rval = np.nansum(test)/np.nansum(y_test)
            rwork = np.nansum(work)/np.nansum(y_train)
            resid_sums.append(rval)
            work_sums.append(rwork)
            coef_out = list(x for x in reg.coef_)
            coeff_sums.append(coef_out)
            nzero = np.count_nonzero(coef_out)
            zero_sums.append(nzero)
            coef_out.sort(reverse=True)
            clist = "".join(list(" %4.3f" % cf for cf in coef_out[0:10] ))
            print "CAT %20s NZ %3g CNT %12d CV %4.3f RES %6.4f COEFF %s" % (source_tsa.r2p[rowi],nzero,row_sums[rowi],rval,rwork,clist)
        source_tsa.add_array(imputed,"imputed")
        combined = source_tsa.new_array()
        nan_in = np.isnan(st_data)
        with_st = np.invert(nan_in)
        combined[nan_in] = imputed[nan_in]
        combined[with_st] = st_data[with_st]
        source_tsa.add_array(combined,"combined")
        
        coef_arr = np.array(coeff_sums)
        c_averages = []
        group_avgs = []
        coef_rowsum = np.nansum(coef_arr,axis=1)
        top5 = np.argsort(coef_rowsum)[::-1]
        for ij,array in enumerate(arrays):
            coli = list(len(arrays)*i+ij for i in range(len(arrays)))
            cav = np.nanmean(coef_arr[:,coli])
            c_averages.append(cav)
        work_score = np.nanmean(work_sums)
        print "COEF WEIGHTS"," ".join(list("%20s %5.4f" % (arrays[i],c_averages[i]) for i in range(len(arrays))))
        print "TOP5 GROUPS"," ".join(list("%20s %5.4f" % (source_tsa.r2p[ri],coef_rowsum[ri]) for ri in top5[0:5]))
        print "GRAND RESIDUAL",alpha,np.nanmean(resid_sums),work_score,float(np.nansum(zero_sums))/(n_rows*len(zero_sums))
        source_tsa.store_array("st_model")

    def merge_imputed(self,target_tsa,imputed_source):
        pass
