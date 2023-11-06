import requests
import pandas as pd
import json5 as json

from collections import defaultdict

host = 'https://api.census.gov/data'
dataset_acronym = '/acs/acs1'
g = '?get='
location = '&for=state:*'
api_key = 'b7a161728681980d95761fda88a847af71bb9e6a'
usr_key = f"&key={api_key}"
# Data unavailable for 2023 and 2020
all_the_years = [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022]
all_dataframes = []

def merge_data(all_yo_dfs):
    df = pd.DataFrame()
    for i in range(len(all_yo_dfs)):
        df = pd.concat([df, all_yo_dfs[i]], axis=1)
        print(f"hello {i}")
    return df

def pull_data(census_variable, target_variable):
    df = pd.DataFrame()
    for i in all_the_years:
        year = '/' + str(i)
        variables = census_variable
        # Put it all together in one f-string:
        query_url = f"{host}{year}{dataset_acronym}{g}{variables}{location}{usr_key}"
        # Use requests package to call out to the API
        response = requests.get(query_url)
        # Convert the Response to json and export the result to a CSV
        response_data = response.json()
        my_dict = {}
        for sublist in response_data:
            key = sublist[-1]  # Get the last value as the key
            values = sublist[:-1]  # Get all values except the last one
            my_dict[key] = values
        json_data = json.dumps(my_dict)
        my_dict.update({"state": [target_variable + " " + str(i), "State " + str(i)+" "+target_variable]})
        df1 = pd.DataFrame.from_dict(my_dict, orient='index')
        df = pd.concat([df, df1], axis=1)
    return df



csv_file_name = '/Users/maryamtanveer/Desktop/output.csv'

all_dataframes = [pull_data('B01002_002E,NAME', 'Median Age'),
                  pull_data('B19013_001E,NAME', 'Median Household Income'),
                  pull_data('C02003_003E,NAME', 'White Alone'),
                  pull_data('C02003_004E,NAME', 'Black or African American Alone Population'),
                  pull_data('C02003_005E,NAME', 'American Indian and Alaska Native Alone Population'),
                  pull_data('C02003_006E,NAME', 'Asian Alone Population'),
                  pull_data('C02003_007E,NAME', 'Native Hawaiian and Other Pacific Islander Alone'),
                  pull_data('C02003_008E,NAME', 'Some Other Race Alone Population'),
                  pull_data('C02003_009E,NAME', 'Two or More Races')]
df2 = merge_data(all_dataframes)

df2.to_csv(csv_file_name, index=False)

print(f'CSV file "{csv_file_name}" created successfully.')
