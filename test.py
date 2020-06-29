import numpy as np
import pandas as pd


wine_pages = pd.read_csv('AllPageViews_Unfiltered.csv')
test_array = wine_pages.iloc[:,1:].values.T


def check_nans(input_array): 
    a = input_array.copy() 
    count = 0 
    count2 = 0 
    for i in range(a.shape[0]): 
        temp = a[i,:] 
        if np.isnan(temp).all(): 
            count += 1 
        elif (temp[~np.isnan(temp)]<30).all():
            print(i)
            count2 += 1
    print(count, count2) 


check_nans(test_array)
#np.isnan(test_array)