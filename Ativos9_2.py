from datetime import datetime, timedelta

import time
import warnings
import concurrent.futures

from yahooquery import Ticker


import pandas as pd
import pandas_datareader as pdr

warnings.simplefilter(action='ignore', category=FutureWarning)

START_TIME = (datetime.now() - timedelta(30)).strftime('%Y-%m-%d')
END_TIME = (datetime.now()).strftime('%Y-%m-%d')
SOURCE = 'yahoo'
PREFIX = '.SA'
AVERAGE_PERIODS = (9, 21)


def stock_analysis(stock):
    """
    :param stock:
        stock that will be analyzed throughout the script
    :return:
        returns a dataframe with stock data according to source
    """
    try:
        df_stock_30d = Ticker(stock + '.SA').history(start=START_TIME, end=END_TIME).dropna()
    except:
        return

    df_stock_30d = df_stock_30d.dropna()

    for periods in AVERAGE_PERIODS:
        df_stock_30d['MME' + str(periods)] = df_stock_30d.close.ewm(span=periods).mean().dropna()

    amostra = df_stock_30d[::-1][0:6][::-1]
    # Verifica se as minimas estao maior do que a media e se a média está crescendo

    media_subindo = (
            amostra.MME9[0] < amostra.MME9[1] < amostra.MME9[2]
            and amostra.low[0] > amostra.MME9[0]
            and amostra.low[1] > amostra.MME9[1]
            and amostra.low[2] > amostra.MME9[2]
    )
    meida_descendo =  (
            amostra.MME9[0] > amostra.MME9[1] > amostra.MME9[2]
            and amostra.low[0] < amostra.MME9[0]
            and amostra.low[1] < amostra.MME9[1]
            and amostra.low[2] < amostra.MME9[2]
    )
    if not media_subindo:
        print(f'ativo {stock} sem setup para 9.2 condiçao 1')
        return

    condition2 = amostra.close[3] < amostra.low[2]
    if not condition2:
        print(f'ativo {stock} sem setup para 9.2 condiçao 2')
        return

    condition3 = amostra.low[4] > amostra.MME9[4] and amostra.close[4] > amostra.high[3] + 0.01
    if condition3:
        stop_loss = amostra.low[3]
        ativacao = amostra.high[3] + 0.01
        data = amostra.index[4][1].strftime("%d/%m/%Y")

        print(f'{stock} | compra acima de: {ativacao:.2f} | stop loss {stop_loss:.2f} | data {data}')
        return

    condition4 = (
            amostra.low[4] > amostra.MME9[4]
            and amostra.close[4] < amostra.low[3]
            and amostra.low[5] > amostra.MME9[5]
            and amostra.high[5] > amostra.high[4]
    )

    if not condition4:
        print(f'ativo {stock} sem setup para 9.2 condiçao 3 ou 4')
        return
    stop_loss = min(amostra.low[3], amostra.low[4])
    ativacao = max(amostra.high[3], amostra.high[4]) + 0.01
    data = amostra.index[4][1].strftime("%d/%m/%Y")

    print(f'Ativo: {stock} | compra acima de: {ativacao} | stop loss {stop_loss} | data {data}')


def teste():
    tInicio = time.perf_counter()

    dfSymbols = pd.read_csv('AtivosB3.csv', sep=';')

    print('Possíveis 9.2 de Compra/Venda')

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(stock_analysis, dfSymbols.Asset)

    tFinal = time.perf_counter()

    print(f'Finalização em {tFinal - tInicio} segundos')


teste()
