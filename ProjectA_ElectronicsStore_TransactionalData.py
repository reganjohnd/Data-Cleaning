import pandas as pd
import re
from datetime import datetime
import logging
import helper_functions as hf

OUTPUT_DIR = hf.create_dir('G:\My Drive\DANIELS ECOSYSTEM\extracurricularActivities\DataScience\datasets\Transactional Retail Dataset of Electronics Store', '')
logging.basicConfig(level=logging.INFO, filename=f'{OUTPUT_DIR}info.log', format="%(asctime)s;%(levelname)s;%(message)s")
logging.basicConfig(level=logging.WARNING, filename=f'{OUTPUT_DIR}warnings.log', format="%(asctime)s;%(levelname)s;%(message)s")

# df = pd.read_csv(hf.create_dir(OUTPUT_DIR, 'dirty_data.csv'))

def date_cleaning(data, correct_date_format):
    '''
    correct date format: YYYY/mm/dd

    @desc: format the dates of the @param data dataframe to be consistent and ready for further analysis
    @param: data, dataframe which requires a "date" column containing dates for the transactions
    @param: correct_date_format, correct format for the date
    '''
    bad_dates = data[~data['date'].str.contains(r'^\d{4}')]
    good_dates = data[data['date'].str.contains(r'^\d{4}')]

    remove = ['-', ' ', '*']
    for i in remove:
        bad_dates['date'] = [x.replace(i, '/') for x in bad_dates['date']]

    try:
        bad_dates['date'] = [datetime.strptime(x, '%m/%d/%Y') for x in bad_dates['date']]
        bad_dates['date'] = [datetime.strftime(x, correct_date_format) for x in bad_dates['date']]
    except:
        error_dates = bad_dates[~bad_dates['date'].str.contains(r'\d{2}/\d{2}/\d{4}')]
        bad_dates = bad_dates[bad_dates['date'].str.contains(r'\d{2}/\d{2}/\d{4}')]

        logging.warning('All faulty date formatting was not identified. Check output of error_dates.csv for the faulty date formats.')
        error_dates.to_csv(f'{OUTPUT_DIR}error_dates.csv')

        bad_dates['date'] = [datetime.strptime(x, '%m/%d/%Y') for x in bad_dates['date']]
        bad_dates['date'] = [datetime.strftime(x, correct_date_format) for x in bad_dates['date']]


    output = pd.concat([bad_dates, good_dates])
    return output

def product_sales_by_month(data):
    # Analyse shopping cart

    # isolate required fields
    tmp_df = data[['order_id', 'customer_id', 'date', 'shopping_cart']]
    # turn string into list variable
    tmp_df['shopping_cart'] = [eval(x) for x in tmp_df['shopping_cart']]

    # create new row for each item in list
    df_stack = tmp_df.explode('shopping_cart').reset_index()

    # extract quantity from tuple and place in separate column
    df_stack['quantity'] = [x[1] for x in df_stack['shopping_cart']]

    # extract product name from tuple
    df_stack['shopping_cart'] = [x[0].upper() for x in df_stack['shopping_cart']]

    # format dates
    df_stack['date'] = [datetime.strptime(x.replace('-', '/'), '%Y/%m/%d') for x in df_stack['date']]
    df_stack = df_stack.sort_values(by='date')

    # reformat table
    tmp = df_stack[['date', 'shopping_cart', 'quantity']]

    # change date to a monthly time period
    tmp['date'] = [x.to_period('M') for x in tmp['date']]

    # aggregate data by month and product
    tmp = tmp.groupby(['date', 'shopping_cart']).sum()
    return tmp.reset_index()

def product_level_data(data):
    data['shopping_cart'] = [eval(x) for x in data['shopping_cart']]
    data = data.explode('shopping_cart').reset_index()
    data['quantity'] = [x[1] for x in data['shopping_cart']]
    data['shopping_cart'] = [x[0].upper() for x in data['shopping_cart']]
    return data


# read data in
df = pd.read_csv(f'{OUTPUT_DIR}dirty_data_test.csv')

# Cleaning: stage 1
cleaned = date_cleaning(df, '%Y/%m/%d')

# Formatting: Stage 1
formatted = product_sales_by_month(cleaned)

# exporting
formatted.to_csv('G:\\My Drive\\DANIELS ECOSYSTEM\\extracurricularActivities\\DataScience\\datasets\\Transactional Retail Dataset of Electronics Store\\clean.csv', index=False)
