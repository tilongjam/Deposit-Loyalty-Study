import io
import msoffcrypto
import pandas as pd

def show(dataframe, no = 100):
    pd.options.display.width = None
    pd.options.display.max_columns = None
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_columns', 3000)
    return print(dataframe.head(no))

def decrypt_and_read_excel(file_path, password = 'tp'):

    decrypted_workbook = io.BytesIO()
    with open(file_path, 'rb') as file:
        try:
            office_file = msoffcrypto.OfficeFile(file)
            office_file.load_key(password=password)
            office_file.decrypt(decrypted_workbook)
            df = pd.read_excel(decrypted_workbook)
        except:
            df = pd.read_excel(file)
    #print(file_path)
    return df

bigdata = pd.read_csv(r"C:\Users\tilongjam.ext\Downloads\Deposit Loyalty w CIF ID\bigdata.csv")

# bigdata['MONTHS'] = pd.to_datetime(bigdata['MONTHS'], format='%m-%Y')
bigdata['MONTHS'] = bigdata['MONTHS'].astype(str)
bigdata['Occupation'] = bigdata['Occupation'].astype(str)
bigdata['CURRENT_ACCT_STATUS'] = bigdata['CURRENT_ACCT_STATUS'].astype(str)
bigdata['CIF_ID'] = bigdata['CIF_ID'].astype(str)
bigdata['ACCT_CLS_DATE'] = bigdata['ACCT_CLS_DATE'].astype(str)



# cif_counts = bigdata.groupby('CIF_ID').size().reset_index(name='Count')
# showsage= bigdata.groupby('CIF_ID').agg(
#         Unique_Customer_Ages=('Customer Age', lambda x: sorted(set(x))),
#         Unique_Account_Ages=('ACCT_AGING', lambda x: sorted(set(x)))
# )
# ex = cif_counts.merge(showsage, on='CIF_ID')

#print(ex.dtypes)
# show(ex)

differencelist = []
newcustomerage = []

for i in range(len(bigdata)):
    cust_age = bigdata['Customer Age'][i]
    acc_age = bigdata['ACCT_AGING'][i]
    diff = cust_age - acc_age
    differencelist.append(diff)

    if acc_age >= cust_age and acc_age <= 35:
        newcustomerage.append(acc_age)
    elif cust_age > acc_age and cust_age <= 35:
        newcustomerage.append(cust_age)
    elif acc_age < cust_age and cust_age > 35 and acc_age <= 35:
        newcustomerage.append(acc_age)
    elif acc_age < cust_age and acc_age > 35:
        newcustomerage.append(35)
    else:
        newcustomerage.append(35)

bigdata["New Customer Age"] = newcustomerage
bigdata["Difference in Age"] = differencelist

# show(bigdata)

# file_path = 'tester.csv'
#
# bigdata.to_csv(file_path, index=False)

bigbranchlist = list(set(list(bigdata['Geographical Location'])))

def highestage(col):
    diff_ages = bigdata.groupby(col).agg(
        Unique_OLD_Customer_Ages=('Customer Age', lambda x: sorted(set(x))[-1]),
        Unique_COMPUTED_Customer_Ages=('New Customer Age', lambda x: sorted(set(x))[-1]),
        Unique_Account_Ages=('ACCT_AGING', lambda x: sorted(set(x))[-1]),
        Difference_in_Age = ("Difference in Age", lambda x: sorted(set(x))[-1])
    )
    return diff_ages

acid_ages = highestage('ACID')

newdata = bigdata.merge(acid_ages, how='left',on = 'ACID')

newdata = newdata.loc[:,['CUSTOMBER_TYPE','CIF_ID','ACID',
                         'MONTHS', 'Unique_OLD_Customer_Ages',
                         'MONTH_END_BAL',
                         'Unique_COMPUTED_Customer_Ages', 'Unique_Account_Ages', "Difference_in_Age",
                         'PRODUCT_TYPE', 'Product Sub-Type', 'Total Online/Offline Txn',
                         'Asset Relztn scheme', 'Geographical Location',
                         'Occupation',
                         'Rate Card',
                         'SI Flag',
                         'ACCT_OPN_DATE',
                         'ACCT_CLS_DATE',
                         'CURRENT_ACCT_STATUS']].sort_values(['CUSTOMBER_TYPE','CIF_ID','ACID', 'MONTHS'])

subsetdata = bigdata.copy()

def summarizer(col_name):
    acid_counts = subsetdata.groupby(col_name).size().reset_index(name='Count of ACIDs')
    product_types = subsetdata.groupby(col_name).agg(
        Product_Types=('PRODUCT_TYPE', lambda x: ', '.join(sorted(set(x)))),
        Unique_Product_Subtypes_Count=('Product Sub-Type', 'nunique'),
        Product_Subtypes=('Product Sub-Type', lambda x: ', '.join(sorted(set(x)))),
        Total_Transactions_of_ACID=('Total Online/Offline Txn', 'sum'),  # Count distinct PRODUCT_SUBTYPEs
        # Customer_Types=('CUSTOMBER_TYPE', lambda x: ', '.join(sorted(set(x)))),
        # Unique_Customer_Types_Count=('CUSTOMBER_TYPE', 'nunique'),
        Unique_OLD_Customer_Ages=('Customer Age', lambda x: sorted(set(x))[-1]),
        Unique_NEW_Customer_Ages=('New Customer Age', lambda x: sorted(set(x))[-1]),
        Unique_Account_Ages=('ACCT_AGING', lambda x: sorted(set(x))[-1]),
        Difference_in_Age=("Difference in Age", lambda x: sorted(set(x))[-1]),
        # Unique_ACCT_OPN_DATE_Count=('ACCT_OPN_DATE', 'nunique'),  # Count distinct PRODUCT_SUBTYPEs
        Unique_ACCT_OPN_DATE=('ACCT_OPN_DATE', lambda x: ', '.join(map(str, sorted(set(x))))),
        # Unique_ACCT_CLS_DATE_Count=('ACCT_CLS_DATE', 'nunique'),  # Count distinct PRODUCT_SUBTYPEs
        Unique_ACCT_CLS_DATE=('ACCT_CLS_DATE', lambda x: ', '.join(map(str, sorted(set(x))))),
        #Unique_CURRENT_ACCT_STATUS=('CURRENT_ACCT_STATUS', lambda x: ', '.join(sorted(set(x)))),
        Unique_Occupation=('Occupation', lambda x: ', '.join(sorted(set(x)))),
        # Unique_Location_Count=('Geographical Location', 'nunique'),
        Unique_Location = ("Geographical Location", lambda x: ', '.join(sorted(set(x)))),
        Unique_CURRENT_ACCT_STATUS = ('CURRENT_ACCT_STATUS', lambda x: ', '.join(sorted(set(x)))),
        First_Date=('MONTHS', lambda x: x.min()),
        Last_Date=('MONTHS', lambda x: x.max())
    ).reset_index()
    return acid_counts.merge(product_types, on=col_name).sort_values('ACID')

sidedata = summarizer(col_name='ACID')
# test = list(sidedata[sidedata['Unique_Customer_Types_Count'] != 1]['ACID'])
# smallbranchlist = list(set(list(subsetdata['Geographical Location'])))

subsetdata.rename(columns = {'ACCT_AGING': "Account Age"}, inplace = True)
subsetdata = subsetdata.merge(sidedata, on='ACID').loc[:,['Geographical Location',
                                                          'CUSTOMBER_TYPE',
                                                          'CIF_ID',

                                                          'ACID',
                                                          'Product_Types',
                                                          'Product_Subtypes',
                                                          'Total_Transactions_of_ACID',
                                                          'Unique_OLD_Customer_Ages',
                                                          "Unique_Account_Ages",
                                                          'Unique_NEW_Customer_Ages',
                                                          'Difference_in_Age',
                                                          'Unique_Occupation',
                                                          'ACCT_OPN_DATE',
                                                          'ACCT_CLS_DATE',
                                                          'CURRENT_ACCT_STATUS',
                                                          'First_Date',
                                                          'Last_Date'
                                                          ]]
subsetdata = subsetdata.sort_values(['Geographical Location', 'CUSTOMBER_TYPE', 'CIF_ID'])

subsetdata = subsetdata.drop_duplicates()
subsetdata = subsetdata.sort_values(['ACID'])

smallbranchlist = list(set(list(subsetdata['Geographical Location'])))
# print(smallbranchlist)

for branch in bigbranchlist:
    branchlen = len(subsetdata[subsetdata['Geographical Location'] == branch])
    # print(f"{branch} of subsetdata: {branchlen}")

bigsubset = None

for branch in bigbranchlist:
    branchsubset = subsetdata[subsetdata['Geographical Location'] == branch]
    if bigsubset is None:
        bigsubset = branchsubset
    else:
        bigsubset = pd.concat([bigsubset, branchsubset])

age_list = []

for i in range(len(bigsubset)):
    age = bigsubset['Unique_NEW_Customer_Ages'].iloc[i]

    if 0 < age <= 5:
        age_list.append('0-5 years')
    elif 5 < age <= 10:
        age_list.append('5-10 years')
    elif 10 < age <= 15:
        age_list.append('10-15 years')
    elif 15 < age <= 25:
        age_list.append('15-25 years')
    elif 25 < age <= 35:
        age_list.append('25-35 years')

bigsubset['Age_Group'] = age_list


bigsubset.drop_duplicates()


filepath = 'bigsubset_new.csv'
bigsubset.to_csv(filepath, index=False)

corp = bigdata[bigdata['CUSTOMBER_TYPE'] == 'CORPORATE']

filepath = 'corporate_masterdata.csv'
corp.to_csv(filepath, index=False)

# subsetdataRETAIL = subsetdata[subsetdata['CUSTOMBER_TYPE']== 'RETAIL']
# tester = list(set(list(subsetdata['Geographical Location'])))
# for branch in bigbranchlist:
#     if branch in tester:
#         branchlen = len(subsetdataRETAIL[subsetdataRETAIL['Geographical Location'] == branch])
#         print(f"{branch} of retail data: {branchlen}")




def diffcif(col):
    diff_ages = bigdata.groupby(col).agg(
        CIF_IDs=('CIF_ID', lambda x: ', '.join(sorted(set(x)))),
        CIF_ID_Count=('CIF_ID', 'nunique')
    )
    return diff_ages


