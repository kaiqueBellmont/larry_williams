import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

from yahooquery import Ticker
import pandas_datareader as pdr
import pandas as pd
import time
import concurrent.futures

from datetime import datetime, timedelta
import numpy as np

print((datetime.now() - timedelta(30)).strftime('%Y-%m-%d'))

dfAtivo30Dias = Ticker("ABEV3" + '.SA').history(start=(datetime.now() - timedelta(30)).strftime('%Y-%m-%d'),
                                                end=(datetime.now() - timedelta(0)).strftime(
                                                    '%Y-%m-%d')).dropna()

lPeriodos = (9, 21)
for nPeriodos in lPeriodos:
    dfAtivo30Dias['MME' + str(nPeriodos)] = dfAtivo30Dias.close.ewm(span=nPeriodos).mean().dropna()

print(dfAtivo30Dias)
