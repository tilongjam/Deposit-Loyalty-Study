import io
import msoffcrypto
import pandas as pd
import os

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

combinedseries = pd.read_csv(r"C:\Users\tilongjam.ext\Downloads\Deposit Loyalty w CIF ID\serieswithcif.csv")
moredata = decrypt_and_read_excel(r"C:\Users\tilongjam.ext\PycharmProjects\Data Analysis w CIF ID\SBI_Mauritius_Deposits_Loyal_Study_2019_2024_Additional_Data_Report.xlsx")
combinedseries = combinedseries.merge(moredata, how='left',on = 'ACID')

#show(combinedseries)
combinedseries['CIF_ID'] = combinedseries['CIF_ID'].astype(str)
combinedseries['MONTHS'] = pd.to_datetime(combinedseries['MONTHS'], format='%m-%Y')
combinedseries['Customer Age'] = combinedseries['Customer Age']/365
combinedseries['ACCT_AGING'] = combinedseries['ACCT_AGING']/365
combinedseries['CURRENT_ACCT_STATUS'] = combinedseries['CURRENT_ACCT_STATUS'].astype(str)


cif_counts = combinedseries.groupby('CIF_ID').size().reset_index(name='Count')
showsage= combinedseries.groupby('CIF_ID').agg(
        Unique_Customer_Ages=('Customer Age', lambda x: sorted(set(x))),
        Unique_Account_Ages=('ACCT_AGING', lambda x: sorted(set(x)))
)

ex = cif_counts.merge(showsage, on='CIF_ID')

#print(ex.dtypes)
# show(ex)

newcustagelist = []

for i in range(len(ex)):
    cust_age = ex['Unique_Customer_Ages'][i][-1]
    acc_age = ex['Unique_Account_Ages'][i][-1]

    if acc_age >= cust_age and acc_age < 35:
        newcustagelist.append(acc_age)
    elif acc_age < cust_age and cust_age < 35:
        newcustagelist.append(cust_age)
    elif acc_age < cust_age and cust_age > 35 and acc_age < 35:
        newcustagelist.append(acc_age)
    elif acc_age < cust_age and acc_age > 35:
        newcustagelist.append(35)
    else:
        newcustagelist.append(35)

ex = ex.assign(New_Customer_Age = newcustagelist)
ex = ex.drop(['Count', 'Unique_Customer_Ages', 'Unique_Account_Ages'], axis=1)
combinedseries = combinedseries.merge(ex, on='CIF_ID')

def format_table(agg_df, agg_type):
    total_row = pd.DataFrame({
        agg_type: ['Total ='],
        'Total_Transactions': [agg_df['Total_Transactions'].sum()],
        'Number_of_CIFs': [agg_df['Number_of_CIFs'].sum()],
        'Unique_Customer_Types': [agg_df['Unique_Customer_Types'].sum()],
        'CUSTOMER_TYPE': [''],
        'Transaction/Total': [agg_df['Transaction/Total'].sum()]
    })
    return pd.concat([agg_df, total_row])


def summarizer(col_name):
    acid_counts = combinedseries.groupby(col_name).size().reset_index(name='Count')
    product_types = combinedseries.groupby(col_name).agg(
        Product_Types_Count=('PRODUCT_TYPE', 'nunique'),
        Unique_Product_Types_List=('PRODUCT_TYPE', lambda x: ', '.join(sorted(set(x)))),
        Unique_Product_Subtypes_Count=('Product Sub-Type', 'nunique'),  # Count distinct PRODUCT_SUBTYPEs
        Unique_Product_Subtypes=('Product Sub-Type', lambda x: ', '.join(sorted(set(x)))),
        Total_Transactions=('Total Online/Offline Txn', 'sum'),
        Unique_Customer_Types_Count=('CUSTOMBER_TYPE', 'nunique'),  # Count distinct PRODUCT_SUBTYPEs
        Unique_Customer_Types=('CUSTOMBER_TYPE', lambda x: ', '.join(sorted(set(x)))),
        Total_SI_flag = ("SI Flag", "sum"),
        Monthly_Balance_Sum = ("MONTH_END_BAL", "sum"),
        First_Date=('MONTHS', lambda x: x.min().strftime('%m-%Y')),
        Last_Date=('MONTHS', lambda x: x.max().strftime('%m-%Y')),
        Unique_OLD_Customer_Ages=('Customer Age', lambda x: ', '.join(map(str, sorted(set(x))))),
        Unique_NEW_Customer_Ages=('New_Customer_Age', lambda x: ', '.join(map(str, sorted(set(x))))),
        Unique_Account_Ages_Count=('ACCT_AGING', 'nunique'),
        Unique_Account_Ages=('ACCT_AGING', lambda x: ', '.join(map(str, sorted(set(x))))),
        Unique_ACCT_OPN_DATE_Count=('ACCT_OPN_DATE', 'nunique'),  # Count distinct PRODUCT_SUBTYPEs
        Unique_ACCT_OPN_DATE=('ACCT_OPN_DATE', lambda x: ', '.join(map(str, sorted(set(x))))),
        Unique_ACCT_CLS_DATE_Count=('ACCT_CLS_DATE', 'nunique'),  # Count distinct PRODUCT_SUBTYPEs
        Unique_ACCT_CLS_DATE=('ACCT_CLS_DATE', lambda x: ', '.join(map(str, sorted(set(x))))),
        Unique_CURRENT_ACCT_STATUS=('CURRENT_ACCT_STATUS', lambda x: ', '.join(sorted(set(x))))
    ).reset_index()
    return acid_counts.merge(product_types, on=col_name).sort_values(by='Unique_ACCT_OPN_DATE')

cif_id_data = summarizer('CIF_ID')
# filepath = 'cif_id_dataa.csv'
# cif_id_data.to_csv(filepath)

filepath = r"C:\Users\tilongjam.ext\Downloads\Deposit Loyalty w CIF ID\bigdata.csv"
combinedseries.to_csv(filepath, index=False)
# def make_list(list):
#     for branch in list:
#         loc_df = df[df['Geographical Location'] == branch]
#
#         product_type_agg = loc_df.groupby('PRODUCT_TYPE').agg(
#             Total_Transactions=('Total Online/Offline Txn', 'sum'),
#             Number_of_CIFs=('CIF_ID', 'nunique'),
#             Unique_Customer_Types=('CUSTOMBER_TYPE', 'nunique'),
#             CUSTOMER_TYPE=('CUSTOMBER_TYPE', lambda x: ', '.join(sorted(x.unique())))
#         ).reset_index()
#         product_type_agg['Transaction/Total'] = product_type_agg['Total_Transactions'] / product_type_agg['Total_Transactions'].sum()
#         product_type_agg['Transaction/Total'] = product_type_agg['Transaction/Total'].round(6)
#
#         product_subtype_agg = loc_df.groupby('Product Sub-Type').agg(
#             Total_Transactions=('Total Online/Offline Txn', 'sum'),
#             Number_of_CIFs=('CIF_ID', 'nunique'),
#             Unique_Customer_Types=('CUSTOMBER_TYPE', 'nunique'),
#             CUSTOMER_TYPE=('CUSTOMBER_TYPE', lambda x: ', '.join(sorted(x.unique())))
#         ).reset_index()
#         product_subtype_agg['Transaction/Total'] = product_subtype_agg['Total_Transactions'] / product_subtype_agg['Total_Transactions'].sum()
#         product_subtype_agg['Transaction/Total'] = product_subtype_agg['Transaction/Total'].round(6)  # Round to 6 decimal places
#
#         product_type_table = format_table(product_type_agg, 'PRODUCT_TYPE')
#         product_subtype_table = format_table(product_subtype_agg, 'Product Sub-Type')
#
#         output_file = f'{branch}.xlsx'
#         with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
#             product_type_table.to_excel(writer, sheet_name='Product_Type', index=False)
#             product_subtype_table.to_excel(writer, sheet_name='Product_Subtype', index=False)
#
#     return f"Geographical Location = {branch}\n Product Type \n {product_type_table} \nProduct Subtype \n {product_subtype_table}"
#
# make_list(unique_branches)