import connect
import pandas as pd
import numpy as np
from googleapiclient.discovery import build

def convert_reponse_to_df(response):
    list = []
    # get report data
    for report in response.get('reports', []):
        # set column headers
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', []) 
        for row in rows:
            # create dict for each row
            dict = {}
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            # fill dict with dimension header (key) and dimension value (value)
            for header, dimension in zip(dimensionHeaders, dimensions):
                dict[header] = dimension

        # fill dict with metric header (key) and metric value (value)
            for i, values in enumerate(dateRangeValues):
                for metric, value in zip(metricHeaders, values.get('values')):
                    #set int as int, float a float
                    if ',' in value or '.' in value:
                        dict[metric.get('name')] = float(value)
                    else:
                        dict[metric.get('name')] = int(value)

            list.append(dict)
    
    df = pd.DataFrame(list)
    return df
    
def get_report(analytics, start_date, end_date, view_id, metrics, dimensions, dimensionFilterClauses, num_results):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                {
                'viewId': view_id,
                'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                'metrics': metrics,
                'dimensions': dimensions,
                "dimensionFilterClauses": dimensionFilterClauses,
                "samplingLevel": "LARGE",
                "pageSize": num_results,
                }
                ]
            }    
            ).execute()
    
def get_mcf(view_id, start_date, end_date, metrics, dimensions, filters):
    analytics = build('analytics', 'v3', credentials=connect.credentials)
    return analytics.data().mcf().get(
                ids = view_id, start_date = start_date, end_date = end_date , metrics = metrics, sort=None, dimensions=dimensions, filters=filters, max_results=100000, samplingLevel=None, start_index=None
            )
    
def return_ga_data(start_date, end_date, view_id, metrics, dimensions, dimensionFilterClauses, num_results):
    return convert_reponse_to_df(get_report(connect.service, start_date, end_date, view_id, metrics, dimensions, dimensionFilterClauses, num_results))

def return_mcf_data(start_date, end_date, view_id, metrics, dimensions):
    return get_mcf(connect.service, start_date, end_date, view_id, metrics, dimensions)
  
def convert_to_df(res):
    for key in list(res['query'].keys()):
        res['query'][key.replace('-', '_')] = res['query'].pop(key)
        
    rows = len(res['rows'])
    cols = [col['name'].replace('mcf:','') for col in res['columnHeaders']]

    try:
        df = pd.DataFrame(np.array(\
        [list(i.values()) for row in res['rows'] for i in row]).\
        reshape(rows, len(cols)), columns=cols)

    except KeyError:
        df = pd.DataFrame(columns=cols)
        pass
    return df
