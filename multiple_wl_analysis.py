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
os.chdir('C:/Users/obriene/Projects/Wait Lists/Multiple Wait List Analysis/Outputs')
run_date = datetime.datetime.today().strftime('%Y-%m-%d')
#Select group to filter the data on
#group = 'LD and ASD'
#group = 'LD'
#group = 'ASD'
#group = 'Age 65+'
#group = 'CFS 5+'
#group = 'Paeds'
group = 'Over 8 WL'
#group = 'All'

#Settings dict contains the filter strings to filter the data to different
#groups, and a the number of how many pairs to keep for the
#heat mapping (to ensure heatmaps aren't too complicated to read).
settings = {'All':[[], 150],
            'No LD or ASD':[['LD', 'ASD'], np.nan],#only used for summary table
            'LD and ASD':[['LD', 'ASD'], 10],
            'LD':[['LD'], 5],
            'ASD':[['ASD'], 3],
            'Age 65+':[['Age 65+'], 75],
            'CFS 5+' :[['CFS 5+'], 10],
            'Paeds' :[['Paeds'], 25],
            'Over 8 WL':[['Over 8 WL'], 6]}

#Check the inputted group is valid
try:
    options = settings[group]
    filters = options[0]
    pairs_to_keep = options[1]
    print(f'Running analysis for {group}')
except Exception:
    print('Group input not recognised, please enter a recognised grouping')
    raise

# =============================================================================
# # Get data
# =============================================================================
sdmart_engine = create_engine('mssql+pyodbc://@SDMartDataLive2/InfoDB?'\
                           'trusted_connection=yes&driver=ODBC+Driver+17'\
                               '+for+SQL+Server')
#wait list query
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
#local spec names query
local_spec_query = """SELECT local_spec, local_spec_desc
                      FROM PiMSMarts.dbo.cset_specialties"""
#Pull back data
wait_list = pd.read_sql(wl_query, sdmart_engine)
local_spec = pd.read_sql(local_spec_query,sdmart_engine)
#dispose connection
sdmart_engine.dispose()

#Create list of wl and ls columns to simplifty other code
wl_cols = [col for col in wait_list.columns if 'wl' in col]
ls_cols = [col for col in wait_list.columns if 'ls' in col]

#If looking at age 65+ or CFS 5+ add in these columns
wait_list['Age 65+'] = np.where(wait_list['Age'] >= 65, 'Age 65+', 'Age <65')
wait_list['CFS 5+'] = np.where(wait_list['CFS'].astype(float) >= 5, 'CFS 5+', 'CFS <5')
wait_list['Paeds'] = np.where(wait_list['Age'] < 18, 'Paeds', 'Not Paeds')
wait_list['Over 8 WL'] = np.where(wait_list[wl_cols].count(axis=1) >= 8, 'Over 8 WL', 'Under 8 WL')

def filter_data(filters, df):
    if len(filters) == 1:
        df = df.loc[df[filters[0]] == filters[0]].copy()
    elif len(filters) == 2:
        df = df.loc[(df[filters[0]] == filters[0])
                    | (df[filters[1]] == filters[1])].copy()
    return df
# =============================================================================
# # Number of wait list analysis
# =============================================================================
wait_list['Wait Lists'] = [[i for i in row if i]
                           for row in wait_list[wl_cols].values.tolist()]
wait_list['Number of Wait Lists'] = wait_list['Wait Lists'].str.len()

data = []
counts = []
summary = []
for setting in settings.items():
    filter = setting[1][0]
    name = setting[0]
    if name == 'No LD or ASD':
        df_wait = wait_list.loc[(wait_list[filter[0]] != filter[0])
                    & (wait_list[filter[1]] != filter[1])].copy()
    else:
        df_wait = filter_data(filter, wait_list)
    count = df_wait.groupby('Number of Wait Lists')['patnt_refno'].count()
    desc = df_wait['Number of Wait Lists'].describe()
    count.name = name
    desc.name = name
    data.append(df_wait['Number of Wait Lists'].rename(name))
    counts.append(count)
    summary.append(desc)

data_df = pd.DataFrame(data).transpose()
summary_df = pd.DataFrame(summary)
counts_df = pd.DataFrame(counts).transpose()
proportion_df = counts_df/summary_df['count']
counts_df.columns = [col + ' Count' for col in counts_df.columns]
proportion_df.columns = [col + ' Proportion' for col in proportion_df.columns]
counts_df = counts_df.join(proportion_df)

writer = pd.ExcelWriter(f'No. Wait List Summary {run_date}.xlsx',engine='xlsxwriter')   
workbook=writer.book
worksheet=workbook.add_worksheet('Summary')
writer.sheets['Summary'] = worksheet
counts_df.to_excel(writer, sheet_name='Summary', startrow=0 , startcol=0)   
summary_df.to_excel(writer, sheet_name='Summary', startrow=16, startcol=0)
writer.close()


# =============================================================================
# # Filter Data
# =============================================================================
#If only looking at LD/ASD patients, remove records with no LD/ASD here based
#on the filters lists
wait_list = filter_data(filters, wait_list)
#remove empty columns (in case no one is on 14 wait lists)
wait_list = wait_list.dropna(how='all', axis=1)
#Redo list of wl and ls columns incase some of these have been dropped
wl_cols = [col for col in wait_list.columns if 'wl' in col]
ls_cols = [col for col in wait_list.columns if 'ls' in col]

# =============================================================================
# # Format Data
# =============================================================================
#Reformat so that each patients has multiple rows, one for each WL
wl_longform = pd.melt(wait_list[['pasid'] + wl_cols], id_vars='pasid',
                      value_name='WL')[['pasid', 'WL']].dropna()
#Get a df with a list of WL for each patient
wl_list = wl_longform.groupby('pasid')['WL'].agg(list)
#reformat to crosstab table
wl_crosstab = pd.crosstab(wl_longform['pasid'], wl_longform['WL'])
wl_crosstab = wl_crosstab.where(wl_crosstab < 1, 1)

#Reformat so that each patients has multiple rows, one for each LS
ls_longform = pd.melt(wait_list[['pasid'] + ls_cols], id_vars='pasid',
                      value_name='local_spec')[['pasid', 'local_spec']].dropna()
ls_longform['local_spec'] = ls_longform['local_spec'].str.strip()
#join on local spec names
ls_longform = ls_longform.merge(local_spec, on='local_spec', how='left')
#Get a df with a list of WL for each patient
ls_list = ls_longform.groupby('pasid')['local_spec_desc'].agg(list)
#reformat to crosstab table
ls_crosstab = pd.crosstab(ls_longform['pasid'], ls_longform['local_spec_desc'])
ls_crosstab = ls_crosstab.where(ls_crosstab < 1, 1)

# =============================================================================
# # Heatmaps
# =============================================================================
#Grouping up wait list
cats_wl = (wl_list.apply(lambda x:list(combinations(set(x),2)))
           .explode().value_counts().reset_index()
           .rename(columns={'index':'Waiting List 1'}))
#group up local specs
cats_ls = (ls_list.apply(lambda x:list(combinations(set(x),2)))
           .explode().value_counts().reset_index()
           .rename(columns={'index':'Local Spec 1'}))

#longform = wl_longform.copy()
#cat_df = cats_wl.copy()
#colname = 'local_spec_desc'
#data = 'Local Spec'
#keep_lim = 10
#Heatmap function
def heatmap(longform, cat_df, colname, data, keep_lim):
    #Create a df of all possible combinations
    unique_values = longform[colname].unique()
    all_combinations = list(combinations_with_replacement(unique_values, 2))
    reverse = [(combo[1], combo[0]) for combo in all_combinations]
    all_combos = pd.DataFrame({colname : all_combinations + reverse}).drop_duplicates()
    #add in missing pairs
    cat_df = cat_df.merge(all_combos, on=[colname], how='right')
    #sort pairings alphabetically and combine duplicates
    cat_df[colname] = [tuple(sorted(lst)) for lst in cat_df[colname]]
    cat_df = cat_df.groupby(colname, as_index=False).sum()
    #split group into two separate columns
    cat_df[['1', '2']] = cat_df[colname].tolist()
    #create pivot table for heatmap
    pivot_ls = cat_df.pivot(index='1', columns='2', values='count')
    #Remove any values with less than 10 pairings
    pivot_ls[pivot_ls < keep_lim] = np.nan
    #Remove any empty rows/columns
    keep_cols = pivot_ls.columns[(pivot_ls.sum() > 0)]
    keep_rows = pivot_ls.index[pivot_ls.sum(axis=1) > 0]
    pivot_ls = pivot_ls.loc[keep_rows, keep_cols]
    #plot heatmap
    fig, ax = plt.subplots(figsize=(20, 10))
    labels = pivot_ls.map(lambda v: v if v else '')
    sns.heatmap(pivot_ls, cmap='Blues', robust=True, annot=True, fmt='g',
                linewidths=0.5, linecolor='k', ax=ax)
    ax.set(xlabel=f'{data} 1', ylabel=f'{data} 2')
    plt.title(f'Occurances of {data} Pairs for {group}')
    plt.savefig(f'{group}/{group} {data} Heatmap {run_date}.png', bbox_inches='tight')
    plt.close()

heatmap(ls_longform, cats_ls, 'local_spec_desc', 'Local Spec', pairs_to_keep)
heatmap(wl_longform, cats_wl, 'WL', 'Wait List', pairs_to_keep)

# =============================================================================
# # Apriori algorithm
# =============================================================================
def patient_counts(itemsets, patients):
    no_patients = []
    for itemset in itemsets:
        count = 0
        for patient in patients:
            if all(i in patient for i in itemset):
                count += 1
        no_patients.append(count)
    return no_patients

#Apply Apriori algorithm to wl data
def implement_apriori(crosstab, pat_list, data):
    min_support = 0.001 if group != 'Over 8 WL' else 0.02
    frequent_itemsets = apriori(crosstab.astype(bool), min_support=min_support,
                                use_colnames=True)
    rules = association_rules(frequent_itemsets, metric="lift",
                              min_threshold=1,
                              num_itemsets=len(crosstab))
    #remove frozensets
    frequent_itemsets['itemsets'] = [list(lst) for lst
                                     in frequent_itemsets['itemsets']]
    rules['antecedents'] = [next(iter(lst)) for lst in rules['antecedents']]
    rules['consequents'] = [next(iter(lst)) for lst in rules['consequents']]
    #Get the number of patients with each itemset in their list of waitlists
    frequent_itemsets['No. Patients'] = patient_counts(
        frequent_itemsets['itemsets'], pat_list)
    rules['No. Patients'] = patient_counts(rules[['antecedents', 'consequents']]
                                           .apply(list, axis=1).tolist(),
                                           pat_list)
    #Format and save frequent itemsets
    frequent_itemsets = frequent_itemsets.sort_values(by='support',
                                                      ascending=False)
    frequent_itemsets['len'] = frequent_itemsets['itemsets'].apply(lambda x: len(x))
    frequent_itemsets = frequent_itemsets.loc[frequent_itemsets['len'] > 1]
    #Format and save association rules
    rules = rules.sort_values(["support", "confidence","lift"], axis=0,
                              ascending=False)
    #Save to excel
    writer = pd.ExcelWriter(f'{group}/{group} {data} output {run_date}.xlsx',engine='xlsxwriter')   
    frequent_itemsets.to_excel(writer, sheet_name='Frequent Itemsets', index=False)
    rules.to_excel(writer, sheet_name='Association Rules', index=False)
    writer.close()
        
implement_apriori(wl_crosstab, wl_list, 'Wait List')
implement_apriori(ls_crosstab, ls_list, 'Local Spec')