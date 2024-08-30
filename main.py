import io
import msoffcrypto
import pandas as pd
import os

def decrypt_and_read_excel(file_path, password = 'TP'):

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

os.chdir(r"C:\Users\tilongjam.ext\Downloads\Deposit Loyalty w CIF ID")
dirpath = os.getcwd()
print(dirpath)

combinedseries = None

for sheet in os.listdir():
    if sheet.startswith('FY'):
        filepath = os.path.join(dirpath, sheet)
        for sheet in os.listdir(filepath):
            excelpath = os.path.join(filepath, sheet)
            try:
                exactfile = decrypt_and_read_excel(excelpath)
                # acid_count = exactfile['ACID'].nunique()
                # cif_count = exactfile['CIF_ID'].nunique()
                # print(f'File : {excelpath}, \n Acid_Count : {acid_count}, \n CIF_Count : {cif_count}')
                if combinedseries is None:
                    combinedseries = exactfile
                else:
                    combinedseries = pd.concat([combinedseries, exactfile])

            except:
                exactfile = decrypt_and_read_excel(excelpath, password = 'tp')
                # acid_count = exactfile['ACID'].nunique()
                # cif_count = exactfile['CIF_ID'].nunique()
                # print(f'File : {excelpath}, \n Acid_Count : {acid_count}, \n CIF_Count : {cif_count}')
                combinedseries = pd.concat([combinedseries, exactfile])

# def dataset_to_csv(name):
#     filepath = '"' + name + '.csv"'
#     combinedseries[combinedseries['CUSTOMBER_TYPE']== name].to_csv(filepath, index=False)
#
#     if os.path.exists(filepath):
#         print(f"File {filepath} already exists. Overwriting...")
#     else:
#         print(f"File {filepath} does not exist. Creating...")
#     return None
#
#
# dataset_to_csv('GLOBAL CORPORATE')
# dataset_to_csv('CORPORATE')
# dataset_to_csv('RETAIL')
# file_path = "combinedseries.csv"
#
# # Save DataFrame to CSV file

file_path = r"C:\Users\tilongjam.ext\Downloads\Deposit Loyalty w CIF ID\serieswithcif.csv"

combinedseries.to_csv(file_path, index=False)
#
# if os.path.exists(file_path):
#     print(f"File {file_path} already exists. Overwriting...")
# else:
#     print(f"File {file_path} does not exist. Creating new file...")



