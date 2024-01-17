# %% Loading Packages
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from moomoo import *
import datetime


#%% 
    """ Get Basic Info (Static) for Securities"""
security_type_lst  = [
                SecurityType.IDX,
                SecurityType.STOCK,
                SecurityType.BOND
                ]
               
market_lst  = [
                Market.HK,
                Market.US,
                Market.SG
                ]

def get_basic_info(market, security_type, host='127.0.0.1', port=11111):
    """_summary_
    Args:
        market (_type_): _description_
        security_type (_type_): _description_
        host (str, optional): _description_. Defaults to '127.0.0.1'.
        port (int, optional): _description_. Defaults to 11111.
    """
    quote_ctx = OpenQuoteContext(host, port)
    ret, data = quote_ctx.get_stock_basicinfo(market, security_type)
    if ret == RET_OK:
        print("*Collection Successful*")
    else:
        print('error:', data)
    quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽
    return data

 #%%
for market in market_lst:
    for security in security_type_lst:
        data = get_basic_info(market, security, host='127.0.0.1', port=11111)
        if isinstance(data,str):
            continue
        else:
            data.to_csv(str(market)+"_"+str(security)+"_listing_info.csv")



#%%
""" Obtain minute time series of HS """
#Get the HS code:
US_IDX_lst = pd.read_csv("US_IDX_listing_info.csv").iloc[:,1:] # From which we know that the code for SP500 is US..SPX
HK_IDX_lst = pd.read_csv("HK_IDX_listing_info.csv").iloc[:,1:] # From which we know that the code for HS Index is HK.800000

#%% """ Obtain minute time series of sp500 """
def fetch_kline_data(symbol, start_date, end_date, max_count=5, frequency = KLType.K_DAY):
    """
    Fetch historical K-line data for a given security.

    :param symbol: The symbol of the security (e.g., 'HK.800000').
    :param start_date: The start date in 'YYYY-MM-DD' format.
    :param end_date: The end date in 'YYYY-MM-DD' format.
    :param max_count: Maximum number of records per request (default is 5).
    :param frequency: The frequency of the K-line data (e.g., 'day', 'week', 'month').
    :return: A pandas DataFrame containing the historical data.
    """
    # Open a quote context
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    dataframes = []
    
    ret, data, page_req_key = quote_ctx.request_history_kline(symbol, start=start_date, end=end_date, ktype=frequency, max_count=max_count)

    # Check if the request was successful and append data
    if ret == RET_OK:
        dataframes.append(data)
    else:
        print('error:', data)
        quote_ctx.close()
        return None
    
    # Request additional pages and append data
    while page_req_key is not None:
        ret, data, page_req_key = quote_ctx.request_history_kline(symbol, start=start_date, end=end_date, ktype=frequency, max_count=max_count, page_req_key=page_req_key)
        if ret == RET_OK:
            dataframes.append(data)
        else:
            print('error:', data)
            quote_ctx.close()
            return None
    combined_df = pd.concat(dataframes)

    quote_ctx.close()
    return combined_df

# %%
D_df = fetch_kline_data('HK.800000', '2019-01-17', '2024-01-17', max_count=1000, frequency = KLType.K_DAY)
if D_df is not None:
    D_df.to_csv("Processed_data/HK.800000_K.Day_17Jan22_17Jan24.csv")

# %%
M_df = fetch_kline_data('HK.800000', '2019-01-17', '2024-01-17', max_count=1000, frequency = KLType.K_1M)
if M_df is not None:
    M_df.to_csv("Processed_data/HK.800000_K.1M_17Jan22_17Jan24.csv")

