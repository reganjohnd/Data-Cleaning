import pandas as pd
import re
from datetime import datetime

def date_cleaning(data, correct_date_format):
    bad_dates = data[~data['date'].str.contains(r'^\d{4}')]
    good_dates = data[data['date'].str.contains(r'^\d{4}')]

    remove = ['-', ' ']
    for i in remove:
        bad_dates['date'] = [x.replace(i, '/') for x in bad_dates['date']]

    bad_dates['date'] = [datetime.strptime(x, '%m/%d/%Y') for x in bad_dates['date']]
    bad_dates['date'] = [datetime.strftime(x, correct_date_format) for x in bad_dates['date']]
    return pd.concat([bad_dates, good_dates])