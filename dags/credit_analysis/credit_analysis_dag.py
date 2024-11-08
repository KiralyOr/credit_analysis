from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from utils.data_processing import retrieve_data, clean_data, transform_data, load_data, check_for_duplicates

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG('loan_data_pipeline', default_args=default_args, schedule_interval=None) as dag:
    start = DummyOperator(task_id='start')

    retrieve = PythonOperator(
    task_id='retrieve_data',
    python_callable=retrieve_data,
    op_kwargs={
        'output_path': '/tmp/raw_data.csv',
        'input_path': '~/code/credit_analysis/credit_train.csv'
    }
)


    clean = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        op_kwargs={'input_path': '/tmp/raw_data.csv', 'output_path': '/tmp/cleaned_data.csv'}
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={
            'input_path': '/tmp/cleaned_data.csv',
            'customer_output_path': '/tmp/customer_data.csv',
            'loan_output_path': '/tmp/loan_data.csv'
        }
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_kwargs={
            'customer_input_path': '/tmp/customer_data.csv',
            'loan_input_path': '/tmp/loan_data.csv',
            'customer_output_path': '~/output/final_customer.csv',  # Updated path
            'loan_output_path': '~/output/final_loan.csv'           # Updated path
        }
    )
    
    # Data quality check task for duplicate Loan IDs
    data_quality_check = PythonOperator(
        task_id='check_for_duplicates',
        python_callable=check_for_duplicates,
        op_kwargs={'input_path': '~/output/final_loan.csv'}  # Path to the final loan data file
    )

    end = DummyOperator(task_id='end')

    # Define the task dependencies
    start >> retrieve >> clean >> transform >> load >> data_quality_check >> end