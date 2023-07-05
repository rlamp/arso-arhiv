#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date, datetime, timedelta
from decorators import retry
from urllib.parse import urlparse, urlencode, urlunparse
from urllib.request import urlopen
import os.path
import pandas as pd
import re
import urllib.error
import dirtyjson


BASE_URL = "http://meteo.arso.gov.si/webmet/archive/data.xml"
PARAM_VARS = "vars"
PARAM_GROUP = "group"
PARAM_TYPE = "type"
PARAM_WSTATION_ID = "id"
PARAM_DATE = "d1"

VARS_HHOUR = "12,15,21,26"
GROUP_HHOUR= "halfhourlyData0"
TYPE_HHOUR = "halfhourly"
WSTATION_ID_HHOUR = "1828"

VARS_DAILY = "56,62,80,70,47,50,75,83"
GROUP_DAILY = "dailyData2"
TYPE_DAILY = "daily"
WSTATION_ID_DAILY = "1895"

START_DATE = datetime(1800,1,1)

class arso_data:
    rgx_json = re.compile(r'AcademaPUJS.set\( (.*)\)]]>')

    @staticmethod
    def parse_date(date_str):
        date_str = date_str.replace("_", "")
        date_minutes = int(date_str)

        d = timedelta(minutes=date_minutes)
        nd = START_DATE + d

        return nd

    @staticmethod
    @retry(urllib.error.HTTPError, tries=4, delay=3, backoff=2)
    def _get_json(vars_p, group_p, type_p, wstation_id_p, date_p):
        params = {
            PARAM_VARS: vars_p,
            PARAM_GROUP: group_p,
            PARAM_TYPE: type_p,
            PARAM_WSTATION_ID: wstation_id_p,
            PARAM_DATE: str(date_p)
        }

        query_url = list(urlparse(BASE_URL))
        query_url[4] = urlencode(params)
        query_url = urlunparse(query_url)

        print("GETTING: ", query_url)

        response = urlopen(query_url).read().decode('utf-8')

        json_str = arso_data.rgx_json.search(response).group(1)
        json = dirtyjson.loads(json_str)

        return json

    @staticmethod
    def _get_data(vars_p, group_p, type_p, wstation_id_p, date_p, fillna=True, cache_files=False):
            # Use cached data
            cache_filename = './vreme/{}/{}.csv'.format(type_p, str(date_p))
            os.makedirs(os.path.dirname(cache_filename), exist_ok=True)
            if os.path.exists(cache_filename):
                if type_p == TYPE_HHOUR:
                    return pd.read_csv(cache_filename, index_col=0, parse_dates=True,)
                elif type_p == TYPE_DAILY:
                    return pd.read_csv(cache_filename, index_col=0, parse_dates=True, header=None)

            # Else query data
            json = arso_data._get_json(vars_p, group_p, type_p, wstation_id_p, date_p)

            point = next(iter(json['points']))
            data = json['points'][point]

            # Format data into DataFrame/Series
            if type_p == TYPE_HHOUR:
                result = pd.DataFrame.from_dict(data, orient='index')
                
                result.index = result.index.map(arso_data.parse_date)
                result.index.name = 'datetime'
                result.columns = result.columns.map(lambda x: json['params'][x]['name'])
    
                if fillna:            
                    result.fillna(method='ffill', inplace=True)
                    result.fillna(method='bfill', inplace=True)

            elif type_p == TYPE_DAILY:
                result = pd.Series(data)
                
                result.index = result.index.map(lambda x: json['params'][x]['name'])
                result = result.map(lambda x: x == 'da')

            # Cache data
            if cache_files:
                result.to_csv(cache_filename, encoding='utf-8')

            return result


    @staticmethod
    def get_data_hhour(dateTime, features, cache_files=False):
        hour = dateTime.hour * 2
        if dateTime.minute >= 30:
            hour = hour + 1
        hour = min(hour, 47)

        data = arso_data._get_data(
            vars_p=VARS_HHOUR,
            group_p=GROUP_HHOUR,
            type_p=TYPE_HHOUR,
            wstation_id_p=WSTATION_ID_HHOUR,
            date_p=dateTime.date(),
            cache_files=cache_files
        )
        
        return data[features].values[hour]

    @staticmethod
    def get_data_daily(date, features, cache_files=False):
        data = arso_data._get_data(
            vars_p=VARS_DAILY,
            group_p=GROUP_DAILY,
            type_p=TYPE_DAILY,
            wstation_id_p=WSTATION_ID_DAILY,
            date_p=date,
            cache_files=cache_files
        )

        return data[features].values


if __name__ == '__main__':
    a=arso_data.get_data_daily(date(2012,11,11), ['sneg','toca', 'padavinski_dan'])
    print(a)
    b=arso_data.get_data_hhour(datetime(2012,11,11,12,30), ['t2m', 'veter_hitrost'], cache_files=True)
    print(b)
