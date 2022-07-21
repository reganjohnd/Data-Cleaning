import pandas as pd
import re
from datetime import datetime
import logging

OUTPUT_DIR = ''
logging.basicConfig(level=logging.INFO, filename=f'{OUTPUT_DIR}info.log', format="%(asctime)s;%(levelname)s;%(message)s")
logging.basicConfig(level=logging.WARNING, filename=f'{OUTPUT_DIR}warnings.log', format="%(asctime)s;%(levelname)s;%(message)s")

def date_cleaning(data, correct_date_format):
    '''
    correct date format: YYYY/mm/dd

    @desc: format the dates of the @param data dataframe to be consistent and ready for further analysis
    @param: data, dataframe which requires a "date" column containing dates for the transactions
    @param: correct_date_format, correct format for the date
    '''
    bad_dates = data[~data['date'].str.contains(r'^\d{4}')]
    good_dates = data[data['date'].str.contains(r'^\d{4}')]

    remove = ['-', ' ']
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
    finally:
        bad_dates['date'] = [datetime.strptime(x, '%m/%d/%Y') for x in bad_dates['date']]
        bad_dates['date'] = [datetime.strftime(x, correct_date_format) for x in bad_dates['date']]

    output = pd.concat([bad_dates, good_dates])
    return output

    
df = pd.read_csv(f'{OUTPUT_DIR}dirty_data_test.csv')
date_cleaning(df, '%Y-%m-%d').to_csv(f'{OUTPUT_DIR}test_output.csv', index=False)