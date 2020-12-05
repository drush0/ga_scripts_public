## General outline of script
# Want to create a unique line of a dataframe for each unique transaction that occurs in a given GA view.

# This is not the prettiest code, but it works. There are a few steps in here for troubleshooting and to reduce API errors.
# I have personally moved away from this script, but it was the first I wrote for the GA API so I think it's fine for a relative beginner
# I also assume you are able to interface with the GA API .
# TODO - Add guide to the API

from functions import return_ga_data        # This script is located in /ga_cnxn
from datetime import datetime, timedelta
from time import sleep
import pandas as pd
import numpy as np
from apiclient.errors import HttpError

yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

start_date = 'YYYY-MM-DD'
start_date = pd.to_datetime(start_date)
temp_start_date = start_date
df =  pd.DataFrame()
end_date = yesterday
step = 20 # This is a relatively arbitrary value to try to reduce the incidence of API errors
end_date = pd.to_datetime(end_date)

while temp_start_date <= end_date - timedelta(step):
    for n in range(0, 5):
        try:
            df = df.append(return_ga_data(
                start_date= datetime.strftime(temp_start_date, '%Y-%m-%d'),
                end_date = datetime.strftime(temp_start_date + timedelta(step), '%Y-%m-%d'),
                view_id=<YOUR_GA_VIEW_ID>,
                metrics=[
                        {'expression': 'ga:transactionRevenue'},
                        {'expression': 'ga:transactions'},
                        {'expression': 'ga:transactionTax'}
                        ],
                dimensions=[
                        {'name': 'ga:transactionId'},
                        {'name': 'ga:dateHourMinute'},
                        {'name': 'ga:channelGrouping'},
                        {'name': 'ga:source'},
                        {'name': 'ga:medium'},
                        {'name': 'ga:campaign'},
                        {'name': 'ga:keyword'},
                        {'name': 'ga:deviceCategory'},
                        {'name': 'ga:landingPagePath'}
                        ],
                dimensionFilterClauses = [],
                num_results = 100000
                ))
            temp_start_date = temp_start_date + timedelta(step)
            print(temp_start_date)
            sleep(0.1)
        except HttpError as error:
            if error.resp.reason in ['userRateLimitExceeded', 'quotaExceeded', 'internalServerError', 'backendError']:
                print("Increasing rand time")
                sleep((2 ** n) + random.random())
                
# At this point our final dataframe should be complete, we will have to process it to a better format. A lot of this formatting is personal preference

df.columns = df.columns.str.replace('ga:', '')
df.rename(columns={'transactionRevenue': 'revenue', 'transactionId': 'order_id', 'dateHourMinute': 'date', 'deviceCategory': 'device', 'transactionTax': 'tax', 'landingPagePath': 'landing_page', 'channelGrouping': 'channel'}, inplace = True)
df = df.drop_duplicates(subset="order_id", keep="first")
df['date'] = pd.to_datetime(df['date'])
df = df.astype({'order_id': 'int64'}) # It should go without saying that this step only makes sense when your order_id is actually an integer
df.campaign = df.campaign.str.lower()
df.channel = df.channel.str.lower()

# Personally, one thing I like about taking data from the API like this is that you can define and redefine a channel based on any of the factors we have here. 
# In my production code, this step is a lot more involved, an example is provided.

#Create adjusted channel groups - here we want to "fail safe" by setting the default first and then moving channels away from that default
df['adj_channel_lc'] = 'other'
df.adj_channel_lc = np.where(df.medium == 'email', 'email', df.adj_channel_lc)
df.adj_channel_lc = np.where(df.medium == 'cpc', 'cpc', df.adj_channel_lc)
df.adj_channel_lc = np.where(np.logical_and(df.medium == 'cpc', df.campaign.str.contains('brand')), 'cpc_brand', df.adj_channel_lc)
df.adj_channel_lc = np.where(np.logical_and(df.medium == 'cpc', df.campaign.str.contains('shopping')), 'cpc_shopping', df.adj_channel_lc)


df['process_ts'] = pd.to_datetime('now')
df['process_ts'] = df['process_ts'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

# This is where I write the resulting table to whatever SQL/BQ database I am pushing to
print("Done")
