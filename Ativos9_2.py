from datetime import datetime, timedelta

import time
import warnings
import concurrent.futures

from Ativos9x import AnaliseAtivo
from yahooquery import Ticker

import pandas as pd
import pandas_datareader as pdr

warnings.simplefilter(action='ignore', category=FutureWarning)

START_TIME = (datetime.now() - timedelta(30)).strftime('%Y-%m-%d')
END_TIME = (datetime.now()).strftime('%Y-%m-%d')
SOURCE = 'yahoo'
PREFIX = '.SA'
AVERAGE_PERIODS = (9, 21)


def stock_analysis_buy(stock):
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

    if not media_subindo:
        return

    fechamento_abaixo_min_candle_anterior = amostra.close[3] < amostra.low[2]

    if not fechamento_abaixo_min_candle_anterior:
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
            # and amostra.close[4] < amostra.low[3]
            and amostra.low[5] > amostra.MME9[5]
            and amostra.high[5] > amostra.high[4] + 0.01
    )

    if not condition4:
        return
    stop_loss = min(amostra.low[3], amostra.low[4])
    ativacao = max(amostra.high[3], amostra.high[4]) + 0.01
    data = amostra.index[5][1].strftime("%d/%m/%Y")

    print(f'Ativo: {stock} | compra acima de: {ativacao:.2f} | stop loss {stop_loss:.2f} | data {data}')


def stock_analysis_sell(stock):
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
    # Verifica se as minimas estao maior do que a media e se a média está descendo

    media_descendo = (
            amostra.MME9[0] > amostra.MME9[1] > amostra.MME9[2]
            and amostra.low[0] < amostra.MME9[0]
            and amostra.low[1] < amostra.MME9[1]
            and amostra.low[2] < amostra.MME9[2]
    )

    if not media_descendo:
        return

    fechamento_acima_max_candle_anterior = amostra.close[3] > amostra.high[2]

    if not fechamento_acima_max_candle_anterior:
        return

    condition3 = amostra.high[4] < amostra.MME9[4] and amostra.close[4] < amostra.low[3] - 0.01
    if condition3:
        stop_loss = amostra.high[3]
        ativacao = amostra.low[3] - 0.01
        data = amostra.index[4][1].strftime("%d/%m/%Y")

        print(f'{stock} | venda abaixo de: {ativacao:.2f} | stop loss {stop_loss:.2f} | data {data}')
        return

    condition4 = (
            amostra.high[4] < amostra.MME9[4]
            # and amostra.close[4] < amostra.low[3]
            and amostra.high[5] < amostra.MME9[5]
            and amostra.low[5] < amostra.low[4] - 0.1
    )

    if not condition4:
        return
    stop_loss = min(amostra.high[3], amostra.high[4])
    ativacao = max(amostra.low[3], amostra.low[4]) - 0.01
    data = amostra.index[4][1].strftime("%d/%m/%Y")

    print(f'Ativo: {stock} | venda abaixo de: {ativacao:.2f} | stop loss {stop_loss:.2f} | data {data}')


def stock_analysis_buy_9_3(stock):
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

    amostra = df_stock_30d[::-1][0:7][::-1]
    # Verifica se as minimas estao maior do que a media e se a média está crescendo

    media_subindo = (amostra.MME9[0] < amostra.MME9[1] < amostra.MME9[2] < amostra.MME9[3] < amostra.MME9[4])

    if not media_subindo:
        return

    candle_referencia_close = amostra.close[2]
    fechamento_abaixo_duplo = amostra.close[3] < candle_referencia_close and amostra.close[4] < candle_referencia_close

    if not fechamento_abaixo_duplo:
        return

    condition3 = amostra.close[5] > amostra.high[4] + 0.01
    if condition3:
        stop_loss = amostra.low[4]
        ativacao = amostra.high[4] + 0.01
        data = amostra.index[5][1].strftime("%d/%m/%Y")

        print(f'{stock} | compra acima de: {ativacao:.2f} | stop loss {stop_loss:.2f} | data {data}')
        return

    condition4 = (amostra.close[6] > amostra.high[5] + 0.01)

    if not condition4:
        return
    stop_loss = amostra.low[5]
    ativacao = amostra.high[5] + 0.01
    data = amostra.index[6][1].strftime("%d/%m/%Y")

    print(f'{stock} | compra acima de: {ativacao:.2f} | stop loss {stop_loss:.2f} | data {data}')
    return


def stock_analysis_sell_9_3(stock):
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

    amostra = df_stock_30d[::-1][0:7][::-1]
    # Verifica se as minimas estao maior do que a media e se a média está crescendo

    media_subindo = (amostra.MME9[0] > amostra.MME9[1] > amostra.MME9[2] > amostra.MME9[3] > amostra.MME9[4])

    if not media_subindo:
        return

    candle_referencia_close = amostra.close[2]
    fechamento_acima_duplo = amostra.close[3] > candle_referencia_close and amostra.close[4] > candle_referencia_close

    if not fechamento_acima_duplo:
        return

    condition3 = amostra.close[5] < amostra.low[4] - 0.01
    if condition3:
        stop_loss = amostra.high[4]
        ativacao = amostra.low[4] - 0.01
        data = amostra.index[5][1].strftime("%d/%m/%Y")

        print(f'{stock} | compra acima de: {ativacao:.2f} | stop loss {stop_loss:.2f} | data {data}')
        return

    condition4 = (amostra.close[6] < amostra.low[5] - 0.01)

    if not condition4:
        return
    stop_loss = amostra.high[5]
    ativacao = amostra.low[5] - 0.01
    data = amostra.index[6][1].strftime("%d/%m/%Y")

    print(f'{stock} | compra acima de: {ativacao:.2f} | stop loss {stop_loss:.2f} | data {data}')
    return


def teste():
    tInicio = time.perf_counter()

    dfSymbols = pd.read_csv('AtivosB3.csv', sep=';')

    print('Possíveis 9.1 de Compra | Venda')

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(AnaliseAtivo, dfSymbols.Asset)

    print('Possíveis 9.2 de Compra')

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(stock_analysis_buy, dfSymbols.Asset)

    print('Possíveis 9.2 de venda')
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(stock_analysis_sell, dfSymbols.Asset)

    print("Possíveis 9.3 de compra")
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(stock_analysis_buy_9_3, dfSymbols.Asset)

    print("Possíveis 9.3 de venda")
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(stock_analysis_sell_9_3, dfSymbols.Asset)

    tFinal = time.perf_counter()

    print(f'Finalização em {tFinal - tInicio:.2f} segundos')


teste()
