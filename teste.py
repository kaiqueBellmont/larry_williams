import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas_datareader as pdr
import pandas as pd
import time
import concurrent.futures

from datetime import datetime, timedelta
import numpy as np

dfAtivo30Dias = pdr.DataReader("GEPA4F.SA", 'yahoo',
                               start=(datetime.now() - timedelta(30)).strftime('%Y, %m, %d'),
                               end=(datetime.now() - timedelta(1)).strftime('%Y, %m, %d')
                               )

print(dfAtivo30Dias)
