#General guide: https://github.com/googleads/google-ads-python
#When I use this script, it runs on a cron job every hour. The dataframe is uploaded to SQL (this code is not provided)
# and if the pct_of_budget exceeds a given value, it sends me an email with a list of campaigns to check on

import pandas as pd
import io
from googleads import adwords
from datetime import datetime, timedelta

acc_id = 'YOUR_ADS_ACCOUNT_NUMBER' 
output = io.StringIO()

adwords_client = adwords.AdWordsClient.LoadFromStorage('googleads.yaml') #You will need to generate this file yourself
adwords_client.SetClientCustomerId(acc_id)

report_downloader = adwords_client.GetReportDownloader(version='v201809')

report_query = (adwords.ReportQueryBuilder()
                  .Select('CampaignName', 'Cost', 'Amount')
                  .From('CAMPAIGN_PERFORMANCE_REPORT')
                  .Where('CampaignStatus').In('ENABLED')
                  .During('TODAY') 
                  .Build())

report_downloader.DownloadReportWithAwql(report_query, 'CSV', output, skip_report_header=True,
          skip_column_header=False, skip_report_summary=True,
          include_zero_impressions=True)

output.seek(0)

types= {'Cost': pd.np.float64, 'Budget': pd.np.float64}
df = pd.read_csv(output,low_memory=False, dtype= types, na_values=[' --'])
df.columns = map(str.lower, df.columns)
df = df[df.cost> 0]
df.cost=(df.cost/1000000).round(2)
df.budget=df.budget/1000000
df['pct_of_budget'] = (df.cost*100/df.budget).round(2)
df.sort_values('pct_of_budget', ascending=False, inplace=True)
df['process_ts'] = pd.to_datetime('now')
df['process_ts'] = df['process_ts'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
df.to_csv('hour_output')
