import os
import datetime
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from itertools import combinations, combinations_with_replacement
os.chdir('C:/Users/obriene/Projects/Wait Lists/Multiple Wait List Analysis/Outputs/Multi Comorb/Comorb Correlations')
run_date = datetime.datetime.today().strftime('%Y-%m-%d')


# =============================================================================
# # Read in data
# =============================================================================
#create connection
sdmart_engine = create_engine('mssql+pyodbc://@SDMartDataLive2/InfoDB?'\
                           'trusted_connection=yes&driver=ODBC+Driver+17'\
                               '+for+SQL+Server')
#queries
wl_query = """SET NOCOUNT ON
--------------------------------WAIT LIST
----union wait list and follow up data
SELECT DISTINCT wl.patnt_refno, wl.pasid, wl.list_name,
                wl.wlist_refno, wl.calculateAge AS Age,
                wl.local_spec
INTO #OP_WL
FROM vw_wlist_op  wl
LEFT JOIN Pimsmarts.dbo.patients pat
ON wl.patnt_refno = pat.patnt_refno
WHERE wl.pfmgt_spec NOT IN ('ZZ','ZN')
and wl.list_name NOT LIKE '%(FU)%'
and wl.wlist_refno IS NOT NULL --non wl entries

UNION ALL
SELECT DISTINCT fu.patnt_refno, fu.pasid, fu.list_name,
                fu.wlist_refno, fu.calculateAge AS Age, fu.local_spec
FROM [dbo].[vw_wlist_op_fu] fu

LEFT JOIN Pimsmarts.dbo.patients pat ON fu.patnt_refno = pat.patnt_refno
WHERE fu.pfmgt_spec NOT IN ('ZZ','ZN')
and fu.list_name LIKE '%(FU)%'
and fu.wlist_refno IS NOT NULL

--------------------------------FRAILTY
----First get all inpatient records with CFS recorded
SELECT patnt.HospitalNumber, [HistModifiedDateTime],
	   substring(AttributeStatusDesc,31,1) as CFS ---Get the number of the CFS score and not the text
INTO #temp_frailty
FROM [DWRealtime].[RealTimeReporting].[PCM].[vw_AttributeValueHistory] attval ----Historical values of SALUS attributes
LEFT JOIN [DWRealtime].AsclepeionReplica.PASData.Patient Patnt with (nolock) ----Patient demographic data
ON Patnt.PKPatientId = attval.AsclepeionPatientId
WHERE AttributeCode = 'F' ----Only want to see frailty attributes
AND AttributeStatusDesc <> 'Previously Frail (Carry Out Reassessment of CFS)' ---Don't include need for reassessment

UNION ALL

----Then get all ED attendances with CFS recorded
SELECT HospitalNumber, ArrivalDateTime,
	   substring(ClinicalFrailtyScore,30,1)---Get the number of the CFS score and not the text
FROM [cl3-data].DataWarehouse.ed.vw_EDattendance
WHERE ClinicalFrailtyScore IS NOT NULL ---Only want attendances with a CFS recorded
AND arrivalDateTime > '01-JUN-2022' ---records since Nervecentre was implemented

----Find the most recent version for each patient
SELECT HospitalNumber, max([HistModifiedDateTime]) AS MostRecentVersion
INTO #frailty_mr
FROM #temp_frailty
GROUP BY HospitalNumber

----Get the hospital number, patnt_refno and most recent CFS
SELECT temp.HospitalNumber, pat.patnt_refno, CFS
INTO #frailty
FROM #temp_frailty temp
---inner join to the most recent record, on both the patient detail and the most recent timestamp
INNER JOIN #frailty_mr mr ON mr.HospitalNumber = temp.HospitalNumber
							AND mr.MostRecentVersion = temp.[HistModifiedDateTime]
---Join to the patients table to get the patnt_refno
LEFT JOIN PiMSMarts.dbo.patients pat on pat.pasid = mr.HospitalNumber


--------------------------------TEMP TABLE
----Select the data, add row number for each wait list and local spec.
SET NOCOUNT ON
SELECT pasid, list_name, local_spec, Age,
       indx = ROW_NUMBER () over (partition by pasid order by pasid,list_name)
INTO #temp
FROM #OP_WL

--------------------------------SELECT STATEMENT
----Final select statement with joins to make pivot table.
SELECT list1.pasid, pat.patnt_refno, list1.Age, frail.CFS,
       LD = CASE WHEN alertLD.PATNT_REFNO IS NOT NULL THEN 'LD' ELSE 'No LD Recorded' END,
       ASD = CASE WHEN alertASD.PATNT_REFNO IS NOT NULL THEN 'ASD' ELSE 'No ASD Recorded' END,
       wl0 = list1.list_name, wl1 = list2.list_name, wl2 = list3.list_name,
       wl3 = list4.list_name, wl4 = list5.list_name, wl5 = list6.list_name,
       wl6 = list7.list_name, wl7 = list8.list_name, wl8 = list9.list_name,
       wl9 = list10.list_name, wl10 = list11.list_name, wl11 = list12.list_name,
       wl12 = list13.list_name, wl13 = list14.list_name, wl14 = list15.list_name,
       ls0 = list1.local_spec, ls1 = list2.local_spec, ls2 = list3.local_spec,
       ls3 = list4.local_spec, ls4 = list5.local_spec, ls5 = list6.local_spec,
       ls6 = list7.local_spec, ls7 = list8.local_spec, ls8 = list9.local_spec,
       ls9 = list10.local_spec, ls10 = list11.local_spec, ls11 = list12.local_spec,
       ls12 = list13.local_spec, ls13 = list14.local_spec, ls14 = list15.local_spec

FROM #temp list1 
LEFT JOIN #temp list2 ON list1.pasid=list2.pasid
and list2.indx = 2

LEFT JOIN #temp list3 ON list1.pasid=list3.pasid
and	list3.indx = 3

LEFT JOIN #temp list4 ON list1.pasid=list4.pasid
and list4.indx = 4

LEFT JOIN #temp list5 ON list1.pasid=list5.pasid
and list5.indx = 5

LEFT JOIN #temp list6 ON list1.pasid=list6.pasid
and list6.indx = 6

LEFT JOIN #temp list7 ON list1.pasid=list7.pasid
and list7.indx = 7

LEFT JOIN #temp list8 ON list1.pasid=list8.pasid
and list8.indx = 8

LEFT JOIN #temp list9 ON list1.pasid=list9.pasid
and list9.indx = 9

LEFT JOIN #temp list10 ON list1.pasid=list10.pasid
and list10.indx = 10

LEFT JOIN #temp list11 ON list1.pasid=list11.pasid
and list11.indx = 11

LEFT JOIN #temp list12 ON list1.pasid=list12.pasid
and	list12.indx = 12

LEFT JOIN #temp list13 ON list1.pasid=list13.pasid
and list13.indx = 13

LEFT JOIN #temp list14 ON list1.pasid=list14.pasid
and list14.indx = 14

LEFT JOIN #temp list15 ON list1.pasid=list15.pasid
and list15.indx = 15

--join patients table to convert from patnt refno to pasid
LEFT JOIN [PiMSMarts].[dbo].[patients] pat ON pat.pasid = list1.pasid

--Get LD patients from patient alerts
LEFT JOIN (SELECT DISTINCT patnt_refno
           FROM PiMSMarts.dbo.Patient_Alert_Mart 
           WHERE ODPCD_CODE = 'COM06' 
           and END_DTTM IS NULL) alertLD ON pat.patnt_refno=alertLD.PATNT_REFNO

--Get ASD patients FROM patient alerts
LEFT JOIN (SELECT DISTINCT patnt_refno
           FROM PiMSMarts.dbo.Patient_Alert_Mart 
           WHERE ODPCD_CODE = 'COM15' 
           and END_DTTM IS NULL) alertASD ON pat.patnt_refno=alertASD.PATNT_REFNO

LEFT JOIN #frailty frail ON pat.patnt_refno = frail.patnt_refno
WHERE list1.indx = 1
"""
comorb_query = """SELECT
Pasid as pasid,
Condition_Type as comorbidity
FROM [InfoDB].[dbo].[co_morbidities_listing]"""
#read data
wait_list = pd.read_sql(wl_query, sdmart_engine)
comorb_pat = pd.read_sql(comorb_query, sdmart_engine)
#dispose connection
sdmart_engine.dispose()

# =============================================================================
# # Filter and tidy data
# =============================================================================
#create copy of longform, filter to wait list only
comorb_pat = comorb_pat.loc[comorb_pat['pasid'].isin(wait_list['pasid'].drop_duplicates())].copy()
comorb_pat_long = comorb_pat.copy()
#Create one row per patient
comorb_pat['Comorb No'] = comorb_pat.groupby('pasid', as_index=False).cumcount()
comorb_pat['Comorb No'] = 'Comorb' + comorb_pat['Comorb No'].astype(str)
comorb_pat = comorb_pat.pivot(index='pasid', columns='Comorb No', values='comorbidity')
comorb_pat['No. Comorb'] = comorb_pat[[col for col in comorb_pat.columns if 'Comorb' in col]].count(axis=1)
#Get the number of WL a patient is on.
wait_list['No. WL'] = wait_list[[col for col in wait_list.columns if 'wl' in col]].count(axis=1)
#Merge onto wait list and filter out anyone who is not on a WL
comorb_pat = comorb_pat.merge(wait_list[['pasid', 'No. WL']], on ='pasid', how='left')
#Create Comorbidities crosstab
comorb_crosstab = pd.crosstab(comorb_pat_long['pasid'], comorb_pat_long['comorbidity'])

# =============================================================================
# # Heatmap
# =============================================================================
#List the comorbidities for each patient and get the count of each pair
comorb_list = comorb_pat_long.groupby('pasid')['comorbidity'].agg(list)
comorb_pairs = (comorb_list.apply(lambda x:list(combinations(set(x),2)))
           .explode().value_counts().reset_index()
           .rename(columns={'index':'Comorbidity 1'}))
#Create a df of all possible combinations
unique_values = comorb_pat_long['comorbidity'].unique()
all_combinations = list(combinations_with_replacement(unique_values, 2))
reverse = [(combo[1], combo[0]) for combo in all_combinations]
all_combos = pd.DataFrame({'comorbidity' : all_combinations + reverse}).drop_duplicates()
#add in missing pairs
cat_df = comorb_pairs.merge(all_combos, on=['comorbidity'], how='right')
cat_df['comorbidity' ] = [tuple(sorted(lst)) for lst in cat_df['comorbidity' ]]
cat_df = cat_df.groupby('comorbidity' , as_index=False).sum()
#split group into two separate columns
cat_df[['1', '2']] = cat_df['comorbidity'].tolist()
#create pivot table for heatmap
pivot_ls = cat_df.pivot(index='1', columns='2', values='count')
#Remove any values with less than 10 pairings
pivot_ls[pivot_ls < 1] = np.nan
#Remove any empty rows/columns
keep_cols = pivot_ls.columns[(pivot_ls.sum() > 0)]
keep_rows = pivot_ls.index[pivot_ls.sum(axis=1) > 0]
pivot_ls = pivot_ls.loc[keep_rows, keep_cols]
#plot heatmap
fig, ax = plt.subplots(figsize=(20, 10))
labels = pivot_ls.map(lambda v: v if v else '')
sns.heatmap(pivot_ls, cmap='Blues', robust=True, annot=True, fmt='g',
                linewidths=0.5, linecolor='k', ax=ax)
ax.set(xlabel=f'Comorb 1', ylabel=f'Comorb 2')
plt.title(f'Comorbidity Heatmap')
plt.savefig(f'Comorbidity Heatmap {run_date}.png', bbox_inches='tight')
plt.close()

# =============================================================================
# # Apriori algorithm
# =============================================================================# =============================================================================
def patient_counts(itemsets, patients):
    no_patients = []
    for itemset in itemsets:
        count = 0
        for patient in patients:
            if all(i in patient for i in itemset):
                count += 1
        no_patients.append(count)
    return no_patients

frequent_itemsets = apriori(comorb_crosstab.astype(bool), min_support=0.02,
                                use_colnames=True)
rules = association_rules(frequent_itemsets, metric="lift",
                              min_threshold=1,
                              num_itemsets=len(comorb_crosstab))
#remove frozensets
frequent_itemsets['itemsets'] = [list(lst) for lst in frequent_itemsets['itemsets']]
rules['antecedents'] = [next(iter(lst)) for lst in rules['antecedents']]
rules['consequents'] = [next(iter(lst)) for lst in rules['consequents']]
#Get the number of patients with each itemset in their list of waitlists
frequent_itemsets['No. Patients'] = patient_counts(frequent_itemsets['itemsets'], comorb_list)
rules['No. Patients'] = patient_counts(rules[['antecedents', 'consequents']].apply(list, axis=1).tolist(), comorb_list)
#Format and save frequent itemsets
frequent_itemsets = frequent_itemsets.sort_values(by='support', ascending=False)
frequent_itemsets['len'] = frequent_itemsets['itemsets'].apply(lambda x: len(x))
frequent_itemsets = frequent_itemsets.loc[frequent_itemsets['len'] > 1]
#Format and save association rules
rules = rules.sort_values(["support", "confidence","lift"], axis=0, ascending=False)
#Save to excel
writer = pd.ExcelWriter(f'Comorbidity output {run_date}.xlsx',engine='xlsxwriter')   
frequent_itemsets.to_excel(writer, sheet_name='Frequent Itemsets', index=False)
rules.to_excel(writer, sheet_name='Association Rules', index=False)