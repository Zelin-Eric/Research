# %% Loading Packages
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from moomoo import *



#%% 
# """ Get Basic Info (Static) for all HK stocks"""
quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
ret, data = quote_ctx.get_stock_basicinfo(Market.HK, SecurityType.STOCK)
if ret == RET_OK:
    print("*Connection Successful*")
else:
    print('error:', data)
quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽
 

#%%

# %%
