import pandas as pd
import numpy as np


# displays your dataframe with all the columns with the first 100 rows, but you can change no of rows required
def show(dataframe, no=100):
    pd.options.display.width = None
    pd.options.display.max_columns = None
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_columns', 3000)
    return print(dataframe.head(no))


def adjust_sample_table(population_table, sample_table):
    # Iterate through each product type
    for product_type in sample_table.index:
        if product_type == 'Grand Total':
            continue

        # Get the total from the sample table
        grand_total = sample_table.loc[product_type, 'Grand Total']
        # Calculate the sum of age group columns in the sample table
        age_group_sum = sample_table.loc[product_type, sample_table.columns != 'Grand Total'].sum()

        # If the sum matches the grand total, no adjustment is needed
        if age_group_sum == grand_total:
            continue

        # If all values are 0 but grand total is 1, skip this row
        if age_group_sum == 0 and grand_total == 1:
            continue

        # Calculate the difference
        difference = age_group_sum - grand_total

        # Get the corresponding population data row
        population_row = population_table.loc[product_type, sample_table.columns != 'Grand Total']

        # Sort the population data based on the last two digits of the values
        sorted_population_row = population_row.apply(lambda x: int(str(x)[-2:])).sort_values()

        # Get the columns with the smallest last two digits
        columns_to_adjust = sorted_population_row.index[:abs(difference)]

        # Adjust the values in the sample table
        for column in columns_to_adjust:
            if difference > 0:
                sample_table.loc[product_type, column] -= 1
            else:
                sample_table.loc[product_type, column] += 1

    return sample_table


# loading the data
df = pd.read_csv(r"C:\Users\tilongjam.ext\PycharmProjects\Data Analysis w CIF ID\bigsubset_new.csv",
                 dtype={'Geographical Location': object,
                        'CUSTOMBER_TYPE': object,
                        'CIF_ID': int,
                        'ACID': object, 'Product_Types': object,
                        'Product_Subtypes': object,
                        'Total_Transactions_of_ACID': int,
                        'Unique_OLD_Customer_Ages': float,
                        'Unique_Account_Ages': float,
                        'Unique_NEW_Customer_Ages': float,
                        'Difference_in_Age': float,
                        'Unique_Occupation': object,
                        'ACCT_OPN_DATE': object,
                        'ACCT_CLS_DATE': object,
                        'CURRENT_ACCT_STATUS': object,
                        'Age_Group': object})

branches = ['YONO DIGITAL BRANCH', 'LCPC', 'TREASURY DEPARTMENT', 'CORPORATE BANKING DEPT', 'EBENE-HEAD OFFICE']
df = df[~df["Geographical Location"].isin(branches)]
branch_list = df['Geographical Location'].unique()

customer_type = ['RETAIL', 'CORPORATE', 'GLOBAL CORPORATE']

for kind in customer_type:
    data = df[df['CUSTOMBER_TYPE'] == kind]
    with pd.ExcelWriter(f'{kind}.xlsx') as writer:

        # Creating the pivot table for the summary sheet
        pivot_table = pd.pivot_table(data,
                                     values='CIF_ID',
                                     index='Geographical Location',
                                     columns='Product_Types',
                                     aggfunc='count',
                                     fill_value=0,
                                     margins=True,
                                     margins_name='Grand Total')

        # Taking 1% of the summary table
        sample_table = (pivot_table * 0.01).round(0)

        # Writing the pivot table and sample table to the summary sheet
        pivot_table.to_excel(writer, sheet_name='Summary Sheet', startrow=0)
        sample_table.to_excel(writer, sheet_name='Summary Sheet', startrow=pivot_table.shape[0] + 3)

        # Looping through each branch
        for branch in branch_list:
            branch_data = data[data['Geographical Location'] == branch]

            # Creating a pivot table for the branch data
            summary = pd.pivot_table(branch_data,
                                     values='CIF_ID',
                                     index='Product_Types',
                                     columns='Age_Group',
                                     aggfunc='count',
                                     fill_value=0,
                                     margins=True,
                                     margins_name='Grand Total')

            # Taking 1% of the branch summary table
            summary_age = (summary * 0.01).round(0)

            extracted_rows = []

            # print(summary)
            #
            # print(summary_age)
            i = 0

            # Loop through each Product_Type
            for product_type in summary_age.index:
                if product_type == 'Grand Total':
                    continue

                for age_group in summary_age.columns:
                    if age_group == 'Grand Total':
                        continue

                    # Check for the specific case: grand total is 1 but all columns are 0
                    if summary_age.at[product_type, 'Grand Total'] == 1 and summary_age.loc[product_type].drop(
                            'Grand Total').sum() == 0:
                        # Find the age group with the maximum count in the original (unscaled) summary table
                        max_age_group = summary.loc[product_type].drop('Grand Total').idxmax()

                        # Filter the rows for this product type and age group
                        filtered_rows = data[(data['Product_Types'] == product_type) &
                                             (data['Age_Group'] == max_age_group) &
                                             (data['Geographical Location'] == branch)]

                        filter_acid = filtered_rows['ACID'].unique()

                        # Debugging print statements to check the process
                        # print(f"Processing {product_type} for {branch} with Grand Total 1")
                        # print(f"Max Age Group: {max_age_group}, Rows Found: {len(filtered_rows)}")

                        # Ensure that the filtered_rows is not empty before sampling

                        if not filtered_rows.empty and i == 0:

                            # Sample just one row since the grand total is 1
                            sampled_rows = filtered_rows.sample(1, replace=False)
                            sample_acid = sampled_rows['ACID']

                            extracted_rows.append(sampled_rows)

                            i += 1
                            # print(sampled_rows)
                            #

                            # print(branch.upper(), i)
                            # print(extracted_rows)
                            # print('___________________')

                            # print('---------------')
                            # print(extracted_rows)
                            # print('-----------------')
                            # print(extracted_rows[:2])
                            # if len(extracted_rows) > 1:
                            #     continue

                            # print(i, sampled_rows)
                            # print(extracted_rows)
                            # i+=1
                        else:
                            # print(f"No rows found for {product_type} in {branch} under {max_age_group}")
                            continue  # Skip if no rows are found
                    else:
                        # Filter the rows for the current product type and age group
                        filtered_rows = data[(data['Product_Types'] == product_type) &
                                             (data['Age_Group'] == age_group) &
                                             (data['Geographical Location'] == branch)]

                        # Debugging print statements to check the process
                        # print(f"Processing {product_type} for {branch} under {age_group}")
                        # print(
                        #     f"Rows to sample: {int(summary_age.at[product_type, age_group])}, Rows Found: {len(filtered_rows)}")

                        # Sample the required number of rows (count) or get all rows if the count is larger
                        if not filtered_rows.empty:
                            sampled_rows = filtered_rows.sample(n=int(summary_age.at[product_type, age_group]),
                                                                replace=False)
                            extracted_rows.append(sampled_rows)
                        else:
                            # print(f"No rows found for {product_type} in {branch} under {age_group}")
                            continue  # Skip if no rows are found

            if extracted_rows:
                final_extracted_rows = pd.concat(extracted_rows)
                final_extracted_rows.reset_index(drop=True, inplace=True)  # reset the index

                # Write the pivot table to the sheet
                summary.to_excel(writer, sheet_name=branch, startrow=0)

                summary_age.to_excel(writer, sheet_name=branch, startcol=summary.shape[0] + 3)

                # Write the extracted rows to the same sheet, below the pivot table
                final_extracted_rows.to_excel(writer, sheet_name=branch, startrow=summary.shape[0] + 3)
            else:
                # print(f"No extracted rows for {branch}")
                continue
