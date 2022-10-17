from datetime import datetime, timedelta
from Ativos9x import main

import time
import warnings
import concurrent.futures

import pandas as pd
import pandas_datareader as pdr

warnings.simplefilter(action='ignore', category=FutureWarning)

START_TIME = (datetime.now() - timedelta(30)).strftime('%Y, %m, %d')
END_TIME = (datetime.now()).strftime('%Y, %m, %d')
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
        df_stock_30d = pdr.DataReader(
            stock + PREFIX,
            SOURCE,
            start=START_TIME,
            end=END_TIME
        )
    except:
        print(stock, "Sem informações para esse ativo...")
        return

    df_stock_30d = df_stock_30d.dropna()

    for periods in AVERAGE_PERIODS:
        df_stock_30d['MME' + str(periods)] = df_stock_30d.Close.ewm(span=periods).mean().dropna()

    amostra = df_stock_30d[::-1][0:6][::-1]
    # Verifica se as minimas estao maior do que a media e se a média está crescendo
    condition1 = (
            amostra.MME9[0] > amostra.MME9[1] > amostra.MME9[2]
            and amostra.Low[0] > amostra.MME9[0]
            and amostra.Low[1] > amostra.MME9[1]
            and amostra.Low[2] > amostra.MME9[2]
    )
    if not condition1:
        return

    condition2 = amostra.Close[3] < amostra.Low[2]
    if not condition2:
        return

    condition3 = amostra.Low[4] > amostra.MME9[4] and amostra.Close[4] > amostra.High[3] + 0.01
    if condition3:
        stop_loss = amostra.Low[3]
        ativacao = amostra.High[3] + 0.01
        data = amostra.index[4].strftime("%d/%m/%Y")

        print(f'{stock} | compra acima de: {ativacao:.2f} | stop loss {stop_loss:.2f} | data {data}')
        return

    condition4 = (
            amostra.Low[4] > amostra.MME9[4] and
            amostra.Close[4] < amostra.Low[3] and
            amostra.Low[5] > amostra.MME9[5] and
            amostra.High[5] > amostra.high[3]
    )

    if not condition4:
        return
    stop_loss = min(amostra.low[3], amostra.Low[4])
    ativacao = max(amostra.High[3], amostra.High[4]) + 0.01
    data = amostra.index[4].strftime("%d/%m/%Y")

    print(f'Ativo: {stock} | compra acima de: {ativacao} | stop loss {stop_loss} | data {data}')


def teste():
    tInicio = time.perf_counter()

    dfSymbols = pd.read_csv('AtivosB3.csv', sep=';')

    print('Possíveis 9.2 de Compra/Venda')

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(stock_analysis, dfSymbols.Asset)

    tFinal = time.perf_counter()

    print(f'Finalização em {tFinal - tInicio} segundos')


if __name__ == '__main__':
    main()
    teste()
