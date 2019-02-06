import pandas as pd
import Salesforce_Bulk_Query
import numpy as  np
import os
import math
from geopy.distance import vincenty
import pickle

def Clean_Sales(x):
    if x == 1:
        return 0
    else:
        return x
    
    
    
def Distance(df, lat, long):
    store_lat = df['Lat']
    store_long = df['Long']
    
    distance = vincenty((store_lat, store_long), (lat, long)).miles
    return distance



def SC_Lat_Long(df, temp):
    store1 = df['Store']
    store2 = df['Store2']
    store3 = df['Store3']
    
    if np.isnan(store2):
        store2 = -99999
    
    if np.isnan(store3):
        store3 = -99999
        
    stores = [store1, store2, store3]
    
    temp = temp[temp['Store'].isin(stores)]
    
    lat = temp['Lat'].mean()
    long = temp['Long'].mean()
    return lat, long


def Store_Distances(df, temp):
    lat = df['New Lat']
    long = df['New Long']
    
    store1 = df['Store']
    store2 = df['Store2']
    store3 = df['Store3']
    
    lat1 = temp.loc[temp[temp['Store']==store1].index.tolist()[0], 'Lat']
    long1 = temp.loc[temp[temp['Store']==store1].index.tolist()[0], 'Long']
    distance1 = vincenty((lat, long), (lat1, long1)).miles
    
    if np.isnan(store2):
        store2 = -99999
        distance2 = np.nan
        
    else:
        lat2 = temp.loc[temp[temp['Store']==store2].index.tolist()[0], 'Lat']
        long2 = temp.loc[temp[temp['Store']==store2].index.tolist()[0], 'Long']
        distance2 = vincenty((lat, long), (lat2, long2)).miles
    
    if np.isnan(store3):
        store3 = -99999
        distance3 = np.nan
        
    else:
        lat3 = temp.loc[temp[temp['Store']==store3].index.tolist()[0], 'Lat']
        long3 = temp.loc[temp[temp['Store']==store3].index.tolist()[0], 'Long']
        distance3 = vincenty((lat, long), (lat3, long3)).miles
    
    return distance1, distance2, distance3

def Checking_Working_Locations(df, temp):
    ldap = df['LDAP']
    location = df['Location']
    
    x = temp[temp['LDAP'] == ldap]
    
    items = x['Location'].unique().tolist()
    
    if location in items:
        return True
    else:
        return False




###############################################################################
# =============================================================================
# query_list = ["""
# select CKSW_BASE__Location__r.Owner.Name, User_LDAP__c, Base_Store__r.Geolocation__Latitude__s, Base_Store__r.Geolocation__Longitude__s, Base_Store__r.Store_Code__c, CKSW_BASE__Location__r.Branch__r.Name, CKSW_BASE__Location__r.Name
# from CKSW_BASE__Resource__C
# """,
# """
# select Store_Code__c, Store__r.Geolocation__Latitude__s, Store__r.Geolocation__Longitude__s, Sales_Manager__r.Branch__r.Name, Sales_Manager__r.Name, Sales_Manager__r.Owner.Name
# from Store_Manager__c
# where Sales_Manager__r.LOB__c like '%HDE%'
# """,
# """
# select CKSW_BASE__Resource__r.User_LDAP__c, Opportunity__r.Contract_Amount__c, Opportunity__r.Base_Store_Number__c, Lead__r.Product_Name__c, CKSW_BASE__Geolocation__Latitude__s, CKSW_BASE__Geolocation__Longitude__s
# from CKSW_BASE__Service__c
# where CKSW_BASE__Resource__r.CKSW_BASE__Active__c = true and Opportunity__r.IsClosed = true and RecordType.Name like '%HDE%' and CKSW_BASE__Resource__r.CKSW_BASE__User__r.UserRole.Name like '%Sales%Con%'
# """,
# """
# select CKSW_BASE__Resource__r.User_LDAP__c, CKSW_BASE__Location__r.Name
# from CKSW_BASE__Working_Location__c
# where CKSW_BASE__Resource__r.User_LDAP__c != null
# """]
# 
# object_list = ['CKSW_BASE__Resource__C','Store_Manager__c','CKSW_BASE__Service__c','CKSW_BASE__Working_Location__c']
# 
# query = Salesforce_Bulk_Query.Salesforce_Query()
# dfs = query.SF_Dataframe(query_list, object_list)
# 
# 
# dfs[0] = dfs[0].rename(columns = {'User_LDAP__c':'LDAP',
#                             'Base_Store__r.Geolocation__Latitude__s':'Lat',
#                             'Base_Store__r.Geolocation__Longitude__s':'Long',
#                             'Base_Store__r.Store_Code__c':'Store',
#                             'CKSW_BASE__Location__r.Branch__r.Name':'Branch',
#                             'CKSW_BASE__Location__r.Name':'Location',
#                             'CKSW_BASE__Location__r.Owner.Name':'Manager'})
# 
# dfs[1] = dfs[1].rename(columns = {'Store_Code__c':'Store',
#                                   'Store__r.Geolocation__Latitude__s':'Lat',
#                                   'Store__r.Geolocation__Longitude__s':'Long',
#                                   'Sales_Manager__r.Branch__r.Name':'Branch',
#                                   'Sales_Manager__r.Name':'Location',
#                                   'Sales_Manager__r.Owner.Name':'Manager'})
# 
# dfs[2] = dfs[2].rename(columns = {'CKSW_BASE__Resource__r.User_LDAP__c':'LDAP',
#                                 'Opportunity__r.Contract_Amount__c':'Sales',
#                                 'Opportunity__r.Base_Store_Number__c':'Store',
#                                 'Lead__r.Product_Name__c':'Product',
#                                 'CKSW_BASE__Geolocation__Latitude__s':'Lat',
#                                 'CKSW_BASE__Geolocation__Longitude__s':'Long'})
# 
# dfs[3] = dfs[3].rename(columns = {'CKSW_BASE__Resource__r.User_LDAP__c':'LDAP',
#                            'CKSW_BASE__Location__r.Name':'Location'})
# 
#     
#     
#     
# 
# pickle.dump(dfs, open('DELETE ME.pickle', 'wb'))
# =============================================================================


###############################################################################
# =============================================================================
# 
#     
# dfs = pickle.load(open('DELETE ME.pickle', 'rb'))
# 
# appts = dfs[2].drop(['Store','Product','Lat','Long'], axis = 1)
# stores = dfs[1]
# scs = dfs[0]
# scs['LDAP'] = scs['LDAP'].str.lower()
# 
# temp = pd.read_excel(r'C:\Users\pmc1104\OneDrive - The Home Depot\Documents\Projects\HDE-HDI\Project Arrow\Final List 2-1-19.xlsx')
# temp = temp[temp['STATUS'] == 'REMAINING']
# temp['LDAP'] = temp['LDAP'].str.lower()
# 
# scs = scs[scs['LDAP'].isin(temp['LDAP'].unique().tolist())]
# del temp
# 
# 
#     
# appts['Sales'] = appts['Sales'].apply(Clean_Sales)
# 
# appts = appts.groupby('LDAP').sum().reset_index()
# 
# scs = (scs.merge(appts, on = 'LDAP', how = 'left')
# .sort_values('Sales', ascending = False)
# .reset_index(drop = True)
# .drop('Sales', axis = 1))
# 
# 
# # =============================================================================
# # temp = scs.drop_duplicates(subset = ['Store'], keep = 'first')
# # scs.loc[scs[~scs['LDAP'].isin(temp['LDAP'])].index.tolist(), 'Store'] = np.nan
# # del temp
# # =============================================================================
# 
# 
# stores = stores[~stores['Store'].isin(scs['Store'].unique().tolist())]
# 
# 
# 
# 
# ###############################################################################
# # =============================================================================
# # ### Use this for the model inputs
# # path_name = r'C:\Users\pmc1104\OneDrive - The Home Depot\Documents\Projects\HDE-HDI\Project Arrow\Nationwide Redistricting\SC Assignment\Inputs'
# # files = os.listdir(path_name)
# # 
# # 
# # temp = []
# # for file in files:
# #     print(file)
# #     df = pd.read_excel(f'{path_name}\\{file}')
# #     temp.append(df)
# #     
# # temp = pd.concat(temp, ignore_index = True, sort = True)
# # temp.to_pickle('Total Input.pickle')
# # =============================================================================
# ###############################################################################
# 
# 
# ###############################################################################
# # =============================================================================
# # df = pd.read_pickle('Total Input.pickle')
# # df = df[['Store','Branch','Store Label']].drop_duplicates()
# # 
# # stores = df.merge(stores[['Store','Lat','Long']], on = 'Store', how = 'left')
# # stores['Branch'] = stores['Branch'] + stores['Store Label'].astype(str)
# # 
# # scs = scs.drop('Branch', axis = 1).merge(stores[['Branch','Store']], on = 'Store', how = 'left')
# # =============================================================================
# ###############################################################################
# 
# stores = stores.dropna(subset = ['Lat','Long'])
# 
# 
# scs = scs.dropna(subset = ['Lat','Long','Manager'])
# 
# 
# ###
# for branch in scs['Manager'].unique():
#     print(branch)
#     temp1 = scs[scs['Manager']==branch]
#     temp2 = stores[stores['Manager']==branch].copy()
#     
#     store_count = math.floor(len(temp2) / len(temp1))
#     if store_count > 1:
#         store_count = 1
#     
#     for i in temp1.index.tolist():
#         lat = temp1.loc[i, 'Lat']
#         long = temp1.loc[i, 'Long']
#         
#         store_nan = False
#         if np.isnan(temp1.loc[i, 'Store']):
#             store_nan = True
#             store_count += 1
#         
#         
#         temp2['Distance'] = temp2.apply(Distance, axis = 1, args = (lat, long, ))
#         
#         
#         temp2 = temp2.sort_values('Distance', ascending = True)
#         
#         store_list = list(temp2.loc[temp2.index.tolist()[:store_count], 'Store'])
#         
#         if store_nan:
#             store_count -= 1
#             for item in store_list:
#                 store_value = store_list.index(item) + 1
#                 if store_value == 1:
#                     scs.loc[i, f'Store'] = item
#                 
#                 else:
#                     try:
#                         scs.loc[i, f'Store{store_value}'] = item
#                     except:
#                         scs[f'Store{store_value}'] = np.nan
#                         scs.loc[i, f'Store{store_value}'] = item
#                     
#                 
#                 temp2 = temp2[temp2['Store']!= item].copy()
#             
#             
#         else:
#             for item in store_list:
#                 store_value = store_list.index(item) + 2
#                 
#                 try:
#                     scs.loc[i, f'Store{store_value}'] = item
#                 except:
#                     scs[f'Store{store_value}'] = np.nan
#                     scs.loc[i, f'Store{store_value}'] = item
#                     
#                 
#                 temp2 = temp2[temp2['Store']!= item].copy()
#                 
# 
# if 'Store2' not in list(scs.columns):
#     scs['Store2'] = np.nan
#     
# if 'Store3' not in list(scs.columns):
#     scs['Store3'] = np.nan
# 
# 
# 
# 
# 
# 
# 
# 
# scs['New Lat'], scs['New Long'] = zip(*scs.apply(SC_Lat_Long, axis = 1, args = (dfs[2][dfs[2]['Product']=='Windows'].drop(['Sales','LDAP','Product'], axis = 1), )))
# 
# scs['Store 1 Dist'], scs['Store 2 Dist'], scs['Store 3 Dist'] = zip(*scs.apply(Store_Distances, axis = 1, args = (dfs[1][['Store','Lat','Long']], )))
# 
# scs.to_pickle('Temp.pickle')
# =============================================================================
###############################################################################




###############################################################################
#scs = pd.read_pickle('Temp.pickle')
scs = pd.read_excel('Base Store Assignment - Rollout (post art)(Update).xlsx')


dfs = pickle.load(open('DELETE ME.pickle', 'rb'))

stores = dfs[1].rename(columns = {'Store_Code__c':'Store',
                                  'Store__r.Geolocation__Latitude__s':'Lat',
                                  'Store__r.Geolocation__Longitude__s':'Long',
                                  'Sales_Manager__r.Branch__r.Name':'Branch',
                                  'Sales_Manager__r.Name':'Location',
                                  'Sales_Manager__r.Owner.Name':'Manager'})


    
    
    
###
# =============================================================================
# df = pd.read_pickle('Total Input.pickle')
# df = df[['Store','Branch','Store Label']].drop_duplicates()
# 
# stores = df.merge(stores[['Store','Lat','Long']], on = 'Store', how = 'left')
# stores['Branch'] = stores['Branch'] + stores['Store Label'].astype(str)
# 
# scs = scs.drop('Branch', axis = 1).merge(stores[['Branch','Store']], on = 'Store', how = 'left')
# =============================================================================
###





temp1 = scs[['LDAP','Store']].dropna()
temp2 = scs[['LDAP','Store2']].rename(columns = {'Store2':'Store'}).dropna()
temp3 = scs[['LDAP','Store3']].rename(columns = {'Store3':'Store'}).dropna()

temp4 = scs[['LDAP','Store','New Lat','New Long','Location']].dropna().merge(stores[['Store','Manager']], on = 'Store', how = 'left').rename(columns = {'New Lat':'Lat','New Long':'Long'})
#temp4 = scs[['LDAP','Store','New Lat','New Long']].dropna().merge(stores[['Store','Branch']], on = 'Store', how = 'left').rename(columns = {'New Lat':'Lat','New Long':'Long'})

temp4['Store'] = 'SC'
temp4['Record Type'] = 'SC'
temp5 = stores[(~stores['Store'].isin(temp1['Store'].unique().tolist())) & (~stores['Store'].isin(temp2['Store'].unique().tolist())) & (~stores['Store'].isin(temp3['Store'].unique().tolist()))][['Store']]
temp5['LDAP'] = 'None'

output = pd.concat([temp1, temp2, temp3, temp5], ignore_index = True, sort = True)
output = output.merge(stores, on = 'Store', how = 'left')
output['Record Type'] = 'Store'
output = output.append(temp4, ignore_index = True, sort = True)



locations = dfs[3].append(scs[['LDAP','Location']], ignore_index = True, sort = True)
output['Assigned Location'] = output.apply(Checking_Working_Locations, axis = 1, args = (locations, ))


temp = dfs[1][['Manager','Branch']].drop_duplicates()
output = output.drop('Branch', axis = 1)
output = output.merge(temp, on = 'Manager', how = 'left')






writer = pd.ExcelWriter('Base Store Assignment - Arrow Rollout (Final)(Update).xlsx')
scs.to_excel(writer, 'SC Data', index = False)
output.to_excel(writer, 'Tableau Data', index = False)
writer.save()

