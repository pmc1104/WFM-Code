import Salesforce_Bulk_Query
from geopy.distance import vincenty
import pandas as pd




def Calculate_Inclusion(df, scs):
    lat = df['Lat']
    long = df['Long']
    location = df['Location']
    store = df['Store']
    
    included_10 = 0
    included_20 = 0
    included_25 = 0
    included_30 = 0
    included_50 = 0
    
    green_included_10 = 0
    green_included_20 = 0
    green_included_25 = 0
    green_included_30 = 0
    green_included_50 = 0
    
    green_dot = 0
    
    
    scs = scs[scs['Location'] == location]
    for i in scs.index.tolist():
        base_lat = scs.loc[i, 'Lat']
        base_long = scs.loc[i, 'Long']
        
        store1 = scs.loc[i, 'Store1']
        store2 = scs.loc[i, 'Store2']
        store3 = scs.loc[i, 'Store3']
        stores = [store1, store2, store3]
        
        distance = vincenty((base_lat, base_long), (lat, long)).miles
        
        
        if distance <= 10 and store in stores:
            green_included_10 = 1
        elif distance <= 10:
            included_10 = 1
            
        if distance <= 20 and store in stores:
            green_included_20 = 1
        elif distance <= 20:
            included_20 = 1
            
        if distance <= 25 and store in stores:
            green_included_25 = 1
        elif distance <= 25:
            included_25 = 1
            
        if distance <= 30 and store in stores:
            green_included_30 = 1
        elif distance <= 30:
            included_30 = 1
            
        if distance <= 50 and store in stores:
            green_included_50 = 1
        elif distance <= 50:
            included_50 = 1
            
        if store in stores:
            green_dot = 1
    
    return included_10, included_20, included_25, included_30, included_50, green_included_10, green_included_20, green_included_25, green_included_30, green_included_50, green_dot
    





queries = ["""
select Opportunity__r.Base_Store_Number__c, CKSW_BASE__Geolocation__Latitude__s, CKSW_BASE__Geolocation__Longitude__s
from CKSW_BASE__Service__c
where CKSW_BASE__Appointment_Start__c = LAST_N_MONTHS:12 and CKSW_BASE__Appointment_Start__c != TODAY and Opportunity__r.IsClosed = true and CKSW_BASE__Status__c != 'Canceled' and RecordType.name like '%HDE%'
""",
"""
select Store_Code__c, Sales_Manager__r.Branch__r.name, Sales_Manager__r.Name
from Store_Manager__c
where (Sales_Manager__r.Branch__r.Name like '%Detroit%' and Sales_Manager__r.LOB__c like '%HDE%')
""",
"""
select CKSW_BASE__Resource__r.Name, CKSW_BASE__Resource__r.User_LDAP__c, CKSW_BASE__Resource__r.CKSW_BASE__Homebase__Latitude__s, CKSW_BASE__Resource__r.CKSW_BASE__Homebase__Longitude__s, CKSW_BASE__Resource__r.Base_Store__r.Store_Code__c, CKSW_BASE__Resource__r.Additional_store_1__r.Store_Code__c, CKSW_BASE__Resource__r.Additional_store_2__r.Store_Code__c, CKSW_BASE__Location__r.name
from CKSW_BASE__Working_Location__c
where CKSW_BASE__Resource__r.CKSW_BASE__Active__c = true and CKSW_BASE__Resource__r.CKSW_BASE__User__r.User_Role_Name__c like '%Sales%Con%' and CKSW_BASE__Resource__r.RecordType.Name like '%HDE%' and CKSW_BASE__Location__r.name like '%Detroit%'
"""]

objects = ['CKSW_BASE__Service__c','Store_Manager__c','CKSW_BASE__Working_Location__c']

query = Salesforce_Bulk_Query.Salesforce_Query()
dfs = query.SF_Dataframe(queries, objects)



df = dfs[0]
df.columns = ['Store','Lat','Long']

temp = dfs[1]
temp.columns = ['Store','Branch','Location']

scs = dfs[2]
scs.columns = ['SC','LDAP','Lat','Long','Store1','Store2','Store3','Location']

df = df.merge(temp, on = 'Store', how = 'left').dropna()

writer = pd.ExcelWriter('Detroit Inputs.xlsx')
df.to_excel(writer, 'appts', index = False)
scs.to_excel(writer, 'SCs', index = False)
writer.save()



# =============================================================================
# df['Included 10'], df['Included 20'], df['Included 25'], df['Included 30'], df['Included 50'], df['Included 10 - Green Dot'], df['Included 20 - Green Dot'], df['Included 25 - Green Dot'], df['Included 30 - Green Dot'], df['Included 50 - Green Dot'], df['Green Dot'] = zip(*df.apply(Calculate_Inclusion, axis = 1, args = (scs, )))
# 
# df = df.drop(['Store','Lat','Long'], axis = 1)
# df['Appts'] = 1
# df = df.groupby(['Branch','Location']).sum().reset_index()
# 
# for column in df.columns:
#     if 'Included' in column and 'Green' in column:
#         radius = column.split(' ')[1]
#         df[f'% Coverage {radius} - Green Dot'] = df[column] / df['Green Dot']
#         
#     elif 'Included' in column:
#         radius = column.split(' ')[1]
#         df[f'% Coverage {radius}'] = df[column] / df['Appts']
# 
# 
# #scs = scs[scs['Location'].isin(df['Location'].unique().tolist())]
# scs = scs[scs['Branch'].isin(df['Branch'].unique().tolist())]
# writer = pd.ExcelWriter('Max travel Results - Detroit.xlsx')
# scs.to_excel(writer, 'SC Bases', index = False)
# df.to_excel(writer, 'Preliminary Analytics', index = False)
# writer.save()
# 
# =============================================================================
