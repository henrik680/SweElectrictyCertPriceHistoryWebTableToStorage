import pandas as pd
from main import *
from urllib.request import urlopen
from bs4 import BeautifulSoup


# def hist_table_from_html(html, year):
#     soup = BeautifulSoup(html, "html.parser")
#     tables = soup.findAll('table')
#     for table in tables:
#         if len(table.findAll('tr')) == 55:
#             df_list = pd.read_html(str(table))
#             if len(df_list) == 1:
#                 df = df_list[0]\
#                     .dropna(axis=0, how='all')\
#                     .drop(columns=[2,3,4,5])\
#                     .assign(Year=year)
#                 df.iat[0, 2] = 'Year'
#                 return df


def test1():
    df = pd.DataFrame(columns = ['Year', 'Week', 'Spot'])
    csv = 'Week;Spot;Year\n'
    for y in [i for i in range(2020, 2022)]:
        url = 'http://www.skm.se/priceinfo/history/{}/'.format(y)
        html = urlopen(url).read()
        df = df.append(hist_table_from_html(html, str(y)))
        #print('{} {}'.format(y, df))
    print(df)

def test2():
        url = 'http://www.skm.se/priceinfo/history/2005/'
        html = urlopen(url).read()
        print(hist_table_from_html(html, str(2021)))

test1()
