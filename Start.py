"""The function's docstring"""
import os
import json
import datetime
from datetime import datetime, date, timedelta
import calendar
import pandas as pd
import numpy as np
from Utils import Classifier_file_creater

# By using this we can actually map out the metric and kind of
# Insight that can be pulled

sample_files_loc = "C:/Users/makanra2/Desktop/Kitchen/Insight/Sample_files"

# creating file for Classifier -- Need to keep thejson config file for this.
# # not to be run everytime
# Classifier_file_creater.file_formation(Locations=sample_files_loc)

def read_file(data):
    """The function's docstring"""
    col = dict(data.columns.to_series().groupby(data.dtypes).groups)
    date_col = None
    metric_col = []
    dimension_col = []
    for key, value in col.items():
        if key == 'datetime64[ns]': # this part can be more flexible based on few more rules
            date_col = value.values.tolist()[0]

        elif key in ['int64', 'float64']: # this part can be more flexible based on few more rules
            for _ in value.values.tolist():
                metric_col.append(_.strip().lower().replace(' ', '_').replace('sum_', '').replace('left_', '').replace('right_', '')) 
        else:
            for _ in value.values.tolist():
                dimension_col.append(_)
    cols_dict = {'date_col':date_col, 'metric_col':tuple(metric_col), 'dimension_col':tuple(dimension_col)}
    return cols_dict

def kpi_json(file_type, channel, columns):
    """The function's docstring"""
    kpi_mapper = open("C:/Users/makanra2/Desktop/Kitchen/Insight/Utils/kpi_mapper.json")
    channel_kpi = open("C:/Users/makanra2/Desktop/Kitchen/Insight/Utils/Channel_kpi.json")
    channel_dict = json.loads(channel_kpi.read())
    kpi_dict = json.loads(kpi_mapper.read())
    for kpi in channel_dict[file_type][channel]:
        if all(column in columns for column in kpi_dict[file_type][kpi]['columns']):
            print(kpi)




def group(cols_dict, data, required='monthly'):
    date_col = cols_dict['date_col']
    metric_col = cols_dict['metric_col']
    dim_group_result = {}
    for dim in cols_dict['dimension_col']:
        metric_list = list(metric_col)
        metric_list.append(dim)
        metric_list.append(date_col)
        df = data[metric_list]
        if required == 'monthly':
            agg_dict = dict.fromkeys(cols_dict['metric_col'], 'sum')
            df_grouped = df.groupby([date_col]).agg(agg_dict)
            df_grouped = df_grouped.reset_index()
            df = pd.merge(df[[dim, date_col]], df_grouped, how='left', on=[date_col])
            for metric in metric_col:
                df_in = df[[dim, date_col, metric]].sort_values(date_col)
                df_in = df_in.groupby(date_col).agg({metric : 'sum'})
                df_in = df_in.iloc[-2:].reset_index()
                max_val = df_in.loc[1, :][metric]
                min_val = df_in.loc[0, :][metric]
                result = percent_change(min_val=min_val, max_val=max_val)
                dim_group_result.update({'{0} across {1} than previous month'.format(metric, dim) : result})
        if required == 'YTD':
            print('nothing is there')

    return dim_group_result



def percent_change(max_val, min_val):
    change_perc = ((max_val - min_val) / min_val ) * 100
    return change_perc


def date_analyser(data, date_col):
    date_df = pd.DataFrame()
    date_df[date_col] = data[date_col]
    date_df['year'] = date_df[date_col].dt.year
    date_df['month'] = date_df[date_col].dt.month
    date_df['month_year'] = [date(year=date_df['year'].iloc[i],month=date_df['month'].iloc[i], day=1) for i in range(len(date_df[date_col].values))]
    # print(date_df)
    total_months = len(set(date_df['month_year'].values))
    total_years = len(set(date_df['year'].values))
    # print(total_months)
    date_df = date_df.sort_values(by=date_col)
    sorted_months = sorted(set(date_df['month_year'].values))
    # print(sorted_months)
    next_date = None
    count = 1
    missing_months = []
    try:
        for mon in sorted_months:
            days_in_month = calendar.monthrange(mon.year, mon.month)[1]

            next_date =  mon + timedelta(days=days_in_month)
            if next_date != sorted_months[count]:
                missing_months.append(next_date)
            count = count + 1
    except Exception as e:
        print('sorted_months '+str(e))
    if len(missing_months) == 0:
        month_continuity = True
    else:
        month_continuity = False

    month_range = [max(missing_months) + timedelta(days=calendar.monthrange(max(missing_months).year, max(missing_months).month)[1]), max(sorted_months)]
    max_year = max(set(date_df['year'].values))
    if max_year - 1 in set(date_df['year'].values):
        year_series = True
    date_analyser_dict = {"year_cont" : year_series,
                          "month_continuity" : month_continuity,
                          "missing_months" : missing_months,
                          "num_of_month" : total_months,
                          "num_of_year"  : total_years,
                          "month_range" : month_range,
                          "max_year" :  max(sorted_months).year,
                          "max_month" : max(sorted_months).month}

    # print(date_analyser_dict)
# {
# date_col : 'str',
# num_of_month : int,
# num_of_year : int,
# continuity : true / false,
# missing_month : list,
# missing year : list,
# year_continous_avl : [],
# continous_month_year : [], 
# granuality : monthly/daily,
# top_two_dates : [],
# last_two_month : []
# }

    return date_analyser_dict


def comparison_group( data, date_col, col_dict, _from = 0, to = 0):
    # in octuber, 2019 there are two campaigns running, while campaign1  has 23 impressions which is 10% more than campaign2
    # in octuber, 2019 there are four ads running in Paid Search, out of which ad1 has highest impressions which is 30% more than average.
    date_dict = date_analyser(data=data, date_col=date_col)
    months = []
    if to > 0:
        for i in range(_from, _from+to):
            month = monthdelta(date = date_dict['month_range'][1], delta=-i)
            months.append(month)
    else:
        months.append(date_dict['month_range'][1])
    
    print(months)
    data_filtered = data[data[date_col].isin(months)]
    # print(data_filtered)
    len_dict = {}
    dict_out = {}
    for dim in col_dict['dimension_col']:
        dict_dim_in = {}
        if len(list(data_filtered[dim].unique())) >= 2:
            len_dict.update({dim : len(list(data_filtered[dim].unique()))})
            for dim_val in data_filtered[dim].unique():
                dict_mt_in = {}
                for metric in col_dict['metric_col']:
                    dict_mt_in.update({metric : float(data_filtered[data_filtered[dim] == dim_val][metric].sum())})
                dict_dim_in.update({dim_val : dict_mt_in})
            dict_out.update({dim : dict_dim_in})
        else:
            print('{} has only one unique value'.format(dim))
    print(json.dumps(dict_out, indent = 2))
    print(len_dict)

    sent_list = []
    for dim in col_dict['dimension_col']:
        if len_dict[dim] == 2:
            value  = list(data_filtered[dim].unique())
            for metric in col_dict['metric_col']:
                if dict_out[dim][value[0]][metric] != dict_out[dim][value[1]][metric]:
                    max_val = max(dict_out[dim][value[0]][metric], dict_out[dim][value[1]][metric])
                    min_val = min(dict_out[dim][value[0]][metric], dict_out[dim][value[1]][metric])
                    pc = percent_change(max_val = max_val, min_val = min_val)
                if max_val == dict_out[dim][value[0]][metric]:
                    dim_max = value[0]
                else:
                    dim_max = value[1]
                if to == 0:
                    sent_out = '''In {month}, {year} there are two {dim} running, while {max_dim}  has {max_val} {metric},the other one lag behind by {pc}%'''.format(month = months[0].strftime('%B'), 
                     year = months[0].year,
                     dim = dim,
                     max_dim = dim_max,
                     max_val = max_val,
                     metric = metric,
                     pc = int(pc)
                     ) 
                    print(sent_out)
    return dict_out


def monthdelta(date, delta):
    m, y = (date.month+delta) % 12, date.year + ((date.month)+delta-1) // 12
    if not m: m = 12
    d = min(date.day, [31,
        29 if y%4==0 and not y%400==0 else 28,31,30,31,30,31,31,30,31,30,31][m-1])
    return date.replace(day=d,month=m, year=y)



def grow_decline(data): 
    """Date in date format and can be done in two ways monthly or YTD
       group by col is see data across
       metric is the list of columns
       if there are more than one column then they will be based on the correlation
    """
    cols_dict = read_file(data)
    date_analyser_dict = date_analyser(data,cols_dict['date_col'])  
    group_result = group(cols_dict, data)

    for dim_met, value in group_result.items():
        # print(value)
        if np.sign(value) in [-1, -1.0]:
            trend = 'decline'
        elif np.sign(value) in [1, 1.0]:
            trend = 'growth'
        elif np.sign(value) in [0, 0.0]:
            trend = 'no change'
        
        print('There has been a {0} of {1} in {2}'.format(trend, value, dim_met))
# there has been a growth of {} in impressions across campaigns
    # print(cols_dict['metric_col'])




data = pd.read_excel(os.path.join(sample_files_loc,'Paid_Search(Digital).xlsx' ))
data.columns = map(str.lower, data.columns)
data.columns = data.columns.str.replace(' ', '_')
col_dict = read_file(data=data)
comparison_group(data=data, date_col= 'month', col_dict=col_dict )
exit()
date_analyser(data=data, date_col='Month')

data.columns = map(str.lower, data.columns)
data.columns = data.columns.str.replace(' ', '_')
kpi_json(file_type='Digital', channel='Paid_Search', columns=['impressions', 'clicks','view'])
grow_decline(data=data)

