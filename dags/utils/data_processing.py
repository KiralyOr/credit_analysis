import pandas as pd

def retrieve_data(input_path, output_path):
    """
    Retrieves the data from the source CSV file and saves it to the specified output path.
    """
    df = pd.read_csv(input_path)
    df.to_csv(output_path, index=False)

def clean_data(input_path, output_path):
    """
    Cleans the data by removing duplicates based on 'Loan ID' and handling outliers,
    such as placeholder values in the 'Current Loan Amount' column. Saves cleaned data
    to the specified output path.
    """
    credit_df = pd.read_csv(input_path)
    # Remove  empty records.
    credit_df.dropna(how='all', inplace=True)

    # Remove records where Current Loan Amount is 99999999.0
    credit_df = credit_df[credit_df['Current Loan Amount'] != 99999999.0]

    # Fix Credit Score col
    high_credit_scores = credit_df['Credit Score'] > 1000
    ending_in_zero = credit_df['Credit Score'] % 10 == 0

    # Divide by 10 if Credit Score > 1000 and ends in 0
    credit_df.loc[high_credit_scores & ending_in_zero, 'Credit Score'] = credit_df.loc[high_credit_scores & ending_in_zero, 'Credit Score'] // 10
    # Remove records where Credit Score > 1000 but does not end in 0
    credit_df = credit_df[~(high_credit_scores & ~ending_in_zero)]


    # credit_df = credit_df.drop_duplicates(subset=['Loan ID'])
    credit_df.to_csv(output_path, index=False)

def transform_data(input_path, customer_output_path, loan_output_path):
    """
    Transforms the cleaned data by splitting it into separate tables: 'customer_data'
    and 'loan_data', and saves each to the specified output paths.
    """
    df = pd.read_csv(input_path)
    customer_data = df[['Customer ID', 'Credit Score', 'Annual Income']].drop_duplicates()
    loan_data = df
    customer_data.to_csv(customer_output_path, index=False)
    loan_data.to_csv(loan_output_path, index=False)

def load_data(customer_input_path, loan_input_path, customer_output_path, loan_output_path):
    """
    Loads the customer and loan data from specified input paths and saves them as final
    output files.
    """
    customer_data = pd.read_csv(customer_input_path)
    loan_data = pd.read_csv(loan_input_path)
    customer_data.to_csv(customer_output_path, index=False)
    loan_data.to_csv(loan_output_path, index=False)

def check_for_duplicates(input_path):
    """
    Checks if there are any duplicate 'Loan ID' values in the given DataFrame.
    Raises an exception if duplicates are found.
    """
    df = pd.read_csv(input_path)
    if df['Loan ID'].duplicated().any():
        raise ValueError("Data quality check failed: Duplicates found in the 'Loan ID' column.")
    else:
        print("Data quality check passed: No duplicates in the 'Loan ID' column.")
