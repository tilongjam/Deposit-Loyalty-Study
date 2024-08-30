import pandas as pd


# # Function to adjust the sample table based on the grand total difference logic
# def adjust_sample_table_based_on_grand_total(population_table, sample_table):
#     # Print column names and row indices for debugging
#     print("Sample Table Columns:", sample_table.columns)
#     print("Sample Table Rows:", sample_table.index)
#
#     if 'Grand Total' not in sample_table.columns:
#         print("Error: 'Grand Total' column not found in sample_table.")
#         return sample_table
#
#     if 'Grand Total' not in sample_table.index:
#         print("Error: 'Grand Total' row not found in sample_table.")
#         return sample_table
#
#     # Calculate the grand total sum for rows and columns
#     row_grand_total_sum = sample_table.loc['Grand Total', :].sum()
#     column_grand_total_sum = sample_table['Grand Total'].sum()
#
#     # The overall grand total is the bottom right most number in the table
#     overall_grand_total = sample_table.at['Grand Total', 'Grand Total']
#
#     # Determine the larger grand total (rows or columns)
#     larger_sum = max(row_grand_total_sum, column_grand_total_sum)
#     difference = larger_sum - overall_grand_total
#
#     # If difference is zero, no adjustment needed
#     if difference == 0:
#         return sample_table
#
#     # If rows have the larger grand total
#     if row_grand_total_sum == larger_sum:
#         # Extract the last two digits of the population grand totals
#         population_grand_totals = population_table['Grand Total'].apply(lambda x: int(str(x)[-2:]) if x >= 100 else None)
#
#         # Sort by the last two digits and skip None values
#         sorted_population_grand_totals = population_grand_totals.dropna().sort_values()
#
#         # Check if we have enough items to adjust
#         if len(sorted_population_grand_totals) < difference:
#             print(f"Not enough items to adjust in rows. Needed: {difference}, Available: {len(sorted_population_grand_totals)}")
#             return sample_table
#
#         # Adjust the sample table's row(s) with the smallest last two digits
#         for i in range(int(difference)):
#             product_type_to_adjust = sorted_population_grand_totals.index[i]
#             sample_table.at[product_type_to_adjust, 'Grand Total'] -= 1
#
#     # If columns have the larger grand total
#     elif column_grand_total_sum == larger_sum:
#         # Extract the last two digits of the population grand totals for columns
#         population_grand_totals_columns = population_table.loc['Grand Total', :].apply(lambda x: int(str(x)[-2:]) if x >= 100 else None)
#
#         # Sort by the last two digits and skip None values
#         sorted_population_grand_totals_columns = population_grand_totals_columns.dropna().sort_values()
#
#         # Check if we have enough items to adjust
#         if len(sorted_population_grand_totals_columns) < difference:
#             print(f"Not enough items to adjust in columns. Needed: {difference}, Available: {len(sorted_population_grand_totals_columns)}")
#             return sample_table
#
#         # Adjust the sample table's column(s) with the smallest last two digits
#         for i in range(int(difference)):
#             age_group_to_adjust = sorted_population_grand_totals_columns.index[i]
#             sample_table.at['Grand Total', age_group_to_adjust] -= 1
#
#     return sample_table
#
#


# Function to adjust the sample table according to your rules
def adjust_sample_table(population_table, sample_table):
    # Adjust the sample table based on the grand total difference first
    sample_table = adjust_sample_table_based_on_grand_total(population_table, sample_table)

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

        # If all values are 0 but grand_total is 1, skip this row
        if age_group_sum == 0 and grand_total == 1:
            continue

        # Calculate the difference
        difference = age_group_sum - grand_total

        # Get the corresponding population data row
        population_row = population_table.loc[product_type, sample_table.columns != 'Grand Total']

        # Sort the population data based on the last two digits of the values
        sorted_population_row = population_row.apply(lambda x: int(str(x)[-2:])).sort_values()

        # Convert the difference to an integer for slicing
        columns_to_adjust = sorted_population_row.index[:int(abs(difference))]

        # Adjust the values in the sample table
        for column in columns_to_adjust:
            if difference > 0:
                sample_table.loc[product_type, column] -= 1
            else:
                sample_table.loc[product_type, column] += 1

    return sample_table


# Function to handle the Grand Total = 1 case
def handle_grand_total_one(product_type, population_table, data, branch):
    """
    This function handles cases where the Grand Total is 1 but all age group values are 0.
    It identifies the age group with the highest count in the population table and extracts a row accordingly.
    """
    # Find the age group with the highest value in the population table for the given product type
    highest_age_group = population_table.loc[product_type].drop('Grand Total').idxmax()

    # Extract a single row from the original data corresponding to this product type and age group
    extracted_row = data[(data['Product_Types'] == product_type) &
                         (data['Age_Group'] == highest_age_group) &
                         (data['Geographical Location'] == branch)].sample(n=1, replace=False)

    return extracted_row


# Main Code Block
df = pd.read_csv(r"C:\Users\tilongjam.ext\PycharmProjects\Data Analysis w CIF ID\bigsubset_new.csv",
                 dtype={'Geographical Location': object,
                        'CUSTOMBER_TYPE': object,
                        'MONTHS': object,
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
                        'Age_Group': object})  # I specified dtypes because pandas couldn't accurately guess them

# Removing all the branches that we don't want and getting the final list of branches
branches = ['YONO DIGITAL BRANCH', 'LCPC', 'TREASURY DEPARTMENT', 'CORPORATE BANKING DEPT', 'EBENE-HEAD OFFICE']
df = df[~df["Geographical Location"].isin(branches)]
branch_list = df['Geographical Location'].unique()

customer_type = ['RETAIL', 'CORPORATE', 'GLOBAL CORPORATE']

for type in customer_type:
    data = df[df['CUSTOMBER_TYPE'] == type]
    with pd.ExcelWriter(f'{type}.xlsx') as writer:

        pivot_table = pd.pivot_table(data,
                                     values='CIF_ID',
                                     index='Geographical Location',
                                     columns='Product_Types',
                                     aggfunc='count',
                                     fill_value=0,
                                     margins=True,
                                     margins_name='Grand Total')

        # Getting the sample size of the summary table, which is 1% of original numerical values and getting the nearest whole number
        sample_table = (pivot_table * 0.01).round(0)

        pivot_table.to_excel(writer, sheet_name='Summary Sheet', startrow=0)

        # Write the extracted rows to the same sheet, below the pivot table
        sample_table.to_excel(writer, sheet_name='Summary Sheet', startrow=pivot_table.shape[0] + 3)

        for branch in branch_list:
            branch_data = data[data['Geographical Location'] == branch]

            summary = pd.pivot_table(branch_data,
                                     values='CIF_ID',
                                     index='Product_Types',
                                     columns='Age_Group',
                                     aggfunc='count',
                                     fill_value=0,
                                     margins=True,
                                     margins_name='Grand Total')

            summary_age = (summary * 0.01).round(0)

            # Adjust the summary_age table using the function before extracting rows
            adjusted_summary_age = adjust_sample_table(summary, summary_age)

            # Getting the sample data
            extracted_rows = []

            # Loop through each Product_Type
            for product_type in adjusted_summary_age.index:
                if product_type == 'Grand Total':
                    continue

                if adjusted_summary_age.at[product_type, 'Grand Total'] == 1 and adjusted_summary_age.loc[
                    product_type].drop('Grand Total').sum() == 0:
                    # Handle the case where Grand Total is 1 but all other columns are 0
                    single_row = handle_grand_total_one(product_type, summary, data, branch)
                    extracted_rows.append(single_row)
                    continue

                for age_group in adjusted_summary_age.columns:
                    if age_group == 'Grand Total':
                        continue

                    # Get the count of rows for this combination
                    count = adjusted_summary_age.at[product_type, age_group]

                    if count > 0:
                        filtered_rows = data[(data['Product_Types'] == product_type) &
                                             (data['Age_Group'] == age_group) &
                                             (data['Geographical Location'] == branch)]

                        if len(filtered_rows) >= count:
                            sampled_rows = filtered_rows.sample(n=int(count), replace=False)
                        else:
                            sampled_rows = filtered_rows  # If not enough rows, take all available rows

                        extracted_rows.append(sampled_rows)

            # Concatenate all extracted rows and write to Excel
            if extracted_rows:
                final_extracted_rows = pd.concat(extracted_rows)
                final_extracted_rows.reset_index(drop=True, inplace=True)  # reset the index

                # Write the pivot table to the
