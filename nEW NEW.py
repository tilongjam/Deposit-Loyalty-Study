import pandas as pd

def show(dataframe, no = 100):
    pd.options.display.width = None
    pd.options.display.max_columns = None
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_columns', 3000)
    return print(dataframe.head(no))


def safe_to_datetime(date_str):
    try:
        return pd.to_datetime(date_str, format='%Y-%m', errors='coerce')
    except Exception as e:
        print(f"Error converting date {date_str}: {e}")
        return pd.NaT

# Apply safe date conversion\
# Existing code for reading data and preprocessing
df = pd.read_csv(r"C:\Users\tilongjam.ext\PycharmProjects\Data Analysis w CIF ID\bigsubset_new.csv",
                 dtype={'Geographical Location': object,
                        'CUSTOMBER_TYPE': object,
                        'CIF_ID': int,
                        'ACID': object,
                        'Product_Types': object,
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
                        'Age_Group': object,
                        'First_Date': object,
                        'Last_Date': object,
                        })


# Convert dates to datetime
df['First_Date'] = pd.to_datetime(df['First_Date'])
df['Last_Date'] = pd.to_datetime(df['Last_Date'])


# Filter out unwanted branches
branches = ['YONO DIGITAL BRANCH', 'LCPC', 'TREASURY DEPARTMENT', 'CORPORATE BANKING DEPT', 'EBENE-HEAD OFFICE']
df = df[~df["Geographical Location"].isin(branches)]
branch_list = df['Geographical Location'].unique()


# Function to calculate the month-end balance sum for the last year
def calculate_month_end_balance(df, product_type, age_group):
    # Filter rows based on product type and age group
    filtered_df = df[(df['Product_Types'] == product_type) & (df['Age_Group'] == age_group)]

    if filtered_df.empty:
        return pd.DataFrame()  # Return empty DataFrame if no rows match

    # Find the last date in the filtered data
    last_date = filtered_df['Last_Date'].max()

    # Define the start and end date for the last year
    end_date = last_date
    start_date = end_date - pd.DateOffset(years=1)

    # Filter for the last year
    last_year_df = filtered_df[(filtered_df['MONTHS'] >= start_date) & (filtered_df['MONTHS'] <= end_date)]

    # Sum month-end balances by month
    monthly_sums = last_year_df.groupby(last_year_df['MONTHS'])['MONTH_END_BAL'].sum().reset_index()

    return monthly_sums


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


# Main Code Block
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

        # getting the sample size of the summary table, is 1% of original numerical values and getting the nearest whole no
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

            # getting the sample data
            extracted_rows = []

            # Loop through each Product_Type
            for product_type in adjusted_summary_age.index:
                if product_type == 'Grand Total':
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

                        # Calculate month-end balance sums for the last year
                        monthly_sums = calculate_month_end_balance(filtered_rows, product_type, age_group)
                        if not monthly_sums.empty:
                            monthly_sums.to_excel(writer, sheet_name=f"{branch}_{product_type}_{age_group}",
                                                  index=False)

            # Concatenate all extracted rows and write to Excel
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


                # Write the pivot table to
