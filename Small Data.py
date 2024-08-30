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

age_list = []

for i in range(len(bigdata)):
    age = bigdata["New Customer Age"][i]

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

bigdata['Age_Group'] = age_list

small_data = bigdata.loc[:,['CUSTOMBER_TYPE','CIF_ID','ACID',
                         'MONTHS',
                         'PRODUCT_TYPE', 'Product Sub-Type', 'Geographical Location', 'MONTH_END_BAL',
                         'Age_Group', 'ACCT_CLS_DATE']].sort_values(['CUSTOMBER_TYPE','CIF_ID','ACID', 'MONTHS'])

corp = small_data[small_data['CUSTOMBER_TYPE'] == 'CORPORATE']
retail = small_data[small_data['CUSTOMBER_TYPE'] == 'RETAIL']
global_cop = small_data[small_data['CUSTOMBER_TYPE'] == 'GLOBAL CORPORATE']

filepath = 'CORPORATE_masterdata.csv'
corp.to_csv(filepath, index=False)

filepath = 'RETAIL_masterdata.csv'
retail.to_csv(filepath, index=False)

filepath = 'GLOBAL CORPORATE_masterdata.csv'
global_cop.to_csv(filepath, index=False)





