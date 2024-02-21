import mysql.connector
import pandas as pd
#pip install mysql-connector-python sshtunnel paramiko
from sshtunnel import SSHTunnelForwarder
import paramiko

# MySQL database connection parameters for Finqy database
finqy_db_config = {
    'host': '119.18.54.55',
    'user': 'finqy5uy_remote',
    'password': 'aiYfABQnwK',
    'database': 'finqy5uy_erbprod'
}

# MySQL database connection parameters for PayInpayOut database

# SSH tunnel parameters
ssh_host = '139.5.190.46'
#ssh_port = 22
ssh_username = 'root'
ssh_password = 'JNDHBS@jrztt688'

payin_payout_db_config = {
    'host': '127.0.0.1',
    'user': 'prod_user',
    'password': '1Testmypolicy$',
    'database': 'uat_payinpayout'
}

# Create an SSH tunnel
with SSHTunnelForwarder(
    (ssh_host, 22),  # Default SSH port is 22
    ssh_username=ssh_username,
    ssh_password=ssh_password,
    remote_bind_address=('127.0.0.1', 3306)
) as tunnel:
    if tunnel.is_active:
        print("SSH tunnel connection successful.")
    # Connect to PayInpayOut database through the SSH tunnel
    payin_payout_db_config['host'] = '127.0.0.1'
    payin_payout_db_config['port'] = tunnel.local_bind_port

    try:
        # Connect to Finqy database
        finqy_connection = mysql.connector.connect(**finqy_db_config)
        finqy_cursor = finqy_connection.cursor()

        # Connect to PayInpayOut database
        payin_payout_connection = mysql.connector.connect(**payin_payout_db_config)
        payin_payout_cursor = payin_payout_connection.cursor()
        print("Connected to PayInpayOut database.")

        # Execute a SELECT query to fetch data from Finqy database
        finqy_cursor.execute('''
    SELECT id, customer_name, mobile_no, currentlocation, email, dob, pincode, company, turnover,
    loan_property_city, ref, status, pancard_no, aadharcard_no, modified_date, date, loan_dis_date,
    remark, loan_type, sanction_bank, sanction_date, sanction_amount, disbursed_bank, disbursed_date,
    disbursed_amount, lppincode, lpstate, broker, pd_date, pdd_date, otc_date, ds_account, pd_account,
    pdd_account, otc_account, ds_amount, pd_amount, pdd_amount, otc_amount, ds_appid, pd_appid, pdd_appid,
    otc_appid
    FROM home_loan_form
    WHERE date >= "2023-10-01 00:00:00"
    AND status IN ('Fully Disbursed', 'Partially Disbursed', 'Fully Disbursed (PDD Pending)');
''')
        result_set = finqy_cursor.fetchall()
        # Convert the result set to a Pandas DataFrame
        column_names = [i[0] for i in finqy_cursor.description]
        df = pd.DataFrame(result_set, columns=column_names)
        df.shape
        df.head()
        

        # Iterate through the result set and insert data into PayInpayOut database
        for row in result_set:
            print(f"Inserting data for ID: {row[0]}")
    
            # Get the column names from the cursor description
            columns = [col[0] for col in finqy_cursor.description]

            # Create the INSERT query dynamically
            insert_query = f"INSERT INTO home_loan_form ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) ON DUPLICATE KEY UPDATE {', '.join([f'{col}=VALUES({col})' for col in columns])}"
            
            # Replace None with the actual value or None if the value is missing
            row_with_nulls = tuple(None if value == '' else value for value in row)

            payin_payout_cursor.execute(insert_query, row_with_nulls)
            print(f"Inserted data for ID: {row[0]}")

        # Commit the changes to PayInpayOut database
        payin_payout_connection.commit()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Close the cursors and connections
        if 'finqy_cursor' in locals():
            finqy_cursor.close()
        if 'finqy_connection' in locals() and finqy_connection.is_connected():
            finqy_connection.close()

        if 'payin_payout_cursor' in locals():
            payin_payout_cursor.close()
        if 'payin_payout_connection' in locals() and payin_payout_connection.is_connected():
            payin_payout_connection.close()