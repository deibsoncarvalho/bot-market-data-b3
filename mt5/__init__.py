# -*- coding: utf-8 -*-
"""
@author: Deibson Carvalho
"""

import pytz

import pandas as pd

from threading import Thread, RLock
from multiprocessing import process
from typing import NamedTuple, Dict
from datetime import datetime

import MetaTrader5 as mt5

from general import Logging, DATA_PATH


__all__ = ['MT5', 'SymbolNotSelected', 'Ticks', 'TICKS_ALL', 'TICKS_TRADE', 'TICKS_INFO', 'TIMEFRAME_M1',
           'TIMEFRAME_M2', 'TIMEFRAME_M3', 'TIMEFRAME_M4', 'TIMEFRAME_M5', 'TIMEFRAME_M6', 'TIMEFRAME_M10',
           'TIMEFRAME_M12', 'TIMEFRAME_M15', 'TIMEFRAME_M20', 'TIMEFRAME_M30', 'TIMEFRAME_H1', 'TIMEFRAME_H2',
           'TIMEFRAME_H3', 'TIMEFRAME_H4', 'TIMEFRAME_H6', 'TIMEFRAME_H8', 'TIMEFRAME_H12', 'TIMEFRAME_D1',
           'TIMEFRAME_W1', 'TIMEFRAME_MN1']


TICKS_ALL = mt5.COPY_TICKS_ALL
TICKS_INFO = mt5.COPY_TICKS_INFO
TICKS_TRADE = mt5.COPY_TICKS_TRADE

TIMEFRAME_M1 = mt5.TIMEFRAME_M1
TIMEFRAME_M2 = mt5.TIMEFRAME_M2
TIMEFRAME_M3 = mt5.TIMEFRAME_M3
TIMEFRAME_M4 = mt5.TIMEFRAME_M4
TIMEFRAME_M5 = mt5.TIMEFRAME_M5
TIMEFRAME_M6 = mt5.TIMEFRAME_M6
TIMEFRAME_M10 = mt5.TIMEFRAME_M10
TIMEFRAME_M12 = mt5.TIMEFRAME_M12
TIMEFRAME_M15 = mt5.TIMEFRAME_M15
TIMEFRAME_M20 = mt5.TIMEFRAME_M20
TIMEFRAME_M30 = mt5.TIMEFRAME_M30
TIMEFRAME_H1 = mt5.TIMEFRAME_H1
TIMEFRAME_H2 = mt5.TIMEFRAME_H2
TIMEFRAME_H3 = mt5.TIMEFRAME_H3
TIMEFRAME_H4 = mt5.TIMEFRAME_H4
TIMEFRAME_H6 = mt5.TIMEFRAME_H6
TIMEFRAME_H8 = mt5.TIMEFRAME_H8
TIMEFRAME_H12 = mt5.TIMEFRAME_H12
TIMEFRAME_D1 = mt5.TIMEFRAME_D1
TIMEFRAME_W1 = mt5.TIMEFRAME_W1
TIMEFRAME_MN1 = mt5.TIMEFRAME_MN1


STOCKS = ''
OPTIONS = ''
FUT = ''

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1500)


class SymbolNotSelected(ValueError):
    def __init__(self, message: str):
        super().__init__(message)


class LoginMetaTrader5Failed(Exception):
    pass


class MT5(Logging):

    def __init__(self,
                 account: int,
                 filename: str,
                 verbose: bool = False):
        self._account: int = account
        self._authorized: bool = False
        self._filename: str = filename

        super(MT5, self).__init__(logger_name=filename,
                                  verbose=verbose)
        self.initialize()

    def initialize(self):
        if not mt5.initialize():
            self.error(f'A inicialização do Metatrader 5 falhou {self.account} - {self.filename}')
            self.__del__()
        else:
            self.info(f'O Metatrader 5 foi initializado com sucesso. '
                      f'\n Conta: {self.account} \n Função: {self.filename}')
            if not self.login():
                self.error(f'A tentativa de login na conta  {self.account} falhou.')
                self.__del__()

    @property
    def filename(self):
        return self._filename

    @property
    def account(self):
        return self._account

    @property
    def authorized(self):
        return self._authorized

    @authorized.setter
    def authorized(self, result: bool):
        self._authorized = result

    def symbols_total(self):
        return mt5.symbols_total()

    def get_symbols(self,
                    group: str = None):
        if not group:
            return mt5.symbols_get()
        return mt5.symbols_get(group=group)

    @property
    def account_info(self) -> NamedTuple:
        """
                Return NamedTuple AccountInfo

                Properts :
                       login: int,
                       trade_mode: int,
                       leverage: int,
                       limit_orders: int,
                       margin_so_mode: int,
                       trade_allowed: bool,
                       trade_expert: bool,
                       margin_mode: int,
                       currency_digits: int,
                       fifo_close: bool,
                       balance: float,
                       credit: float,
                       profit: float,
                       equity: float,
                       margin: float,
                       margin_free: float,
                       margin_level: float,
                       margin_so_call: float,
                       margin_so_so: float,
                       margin_initial: float,
                       margin_maintenance: float,
                       assets: float,
                       liabilities: float,
                       commission_blocked: float,
                       name: string,
                       server: string,
                       currency: string,
                       company: string
                """
        return mt5.account_info()

    def login(self) -> bool:
        self.info(f'efetuando login na conta {self.account} ...')
        self.authorized = mt5.login(self.account)
        if self.authorized:
            self.info(f'login na conta {self.account} efetuado com sucesso! \n'
                      f'Nome: {self.account_info.name} \n'
                      f'Servidor: {self.account_info.server} \n'
                      f'Corretora: {self.account_info.company} \n'
                      f'Moeda: {self.account_info.currency} \n'
                      f'Saldo: {self.account_info.balance} \n'
                      f'Tipo de Conta: {self.account_info.trade_mode} \n')
            return True
        else:
            self.error(f"Tentativa de login na conta {self.account} falhou -> {self.authorized}: {mt5.last_error()}")
            return False

    def get_ticks_from(self,
                       symbol: str,
                       ticks_type: int = TICKS_ALL,
                       from_ticks: datetime = 0,
                       num_ticks: int = 100000000) -> pd.DataFrame:
        self.info(f'Buscando os ticks ({ticks_type}) para o ativo {symbol} a partir de {from_ticks} '
                  f'no total de {num_ticks}...')
        if not self.select_symbol(symbol=symbol):
            raise SymbolNotSelected(f"Symbol {symbol} not selected")

        ticks = mt5.copy_ticks_from(symbol, from_ticks, num_ticks, ticks_type)
        _df = pd.DataFrame(ticks)
        _df['time'] = pd.to_datetime(_df['time'], unit='s')
        _df.set_index('time', inplace=True)
        return _df

    def get_rates_from(self,
                       symbol: str,
                       timeframe: int = TIMEFRAME_M1,
                       date_from: datetime = None,
                       date_to: datetime = None):

        self.info(f'Buscando os candles para o ativo {symbol} a partir de {date_from} até {date_to} ...')

        if not self.select_symbol(symbol=symbol):
            raise SymbolNotSelected(f"Symbol {symbol} not selected")

        timezone = pytz.timezone("Etc/UTC")
        if not date_from:
            date_from = datetime(1970, 1, 1, tzinfo=timezone)
        else:
            date_from.replace(tzinfo=timezone)

        if not date_to:
            date_to = datetime.now(tz=timezone)
        else:
            date_to.replace(tzinfo=timezone)

        rates = mt5.copy_rates_range(symbol, timeframe, date_from, date_to)

        _df = pd.DataFrame(rates)
        _df['time'] = pd.to_datetime(_df['time'], unit='s')
        _df.set_index('time', inplace=True)

        return _df

    def select_symbol(self, symbol: str) -> bool:
        if mt5.symbol_select(symbol, True):
            self.info(f"O instrumento/papel {symbol} foi selecionado com sucesso!")
            return True
        else:
            self.error(f"Erro ao selecionar o ativo {symbol} ->", mt5.last_error())
            return False

    def shutdown(self):
        mt5.shutdown()

    def __del__(self):
        self.shutdown()


class Rates(MT5):

    def __init__(self,
                 symbols: list,
                 account: int,
                 from_date: datetime = datetime(1970, 1, 1),
                 verbose: bool = False):
        super(Rates, self).__init__(account=account,
                                    filename='mt5-rates',
                                    verbose=verbose)
        self._symbols: list = symbols
        self._rates = Dict[str, Dict[int, NamedTuple]]
        self._from: datetime = from_date
        self._threads: Thread
        self._rates_lock = RLock()

    @property
    def symbols(self):
        return self._symbols

    @property
    def rates(self, symbol: str, timeframe: int) -> Dict[str, Dict[int, NamedTuple]]:
        with self._rates_lock:
            return self._rates[symbol][timeframe]

    @rates.setter
    def rates(self, symbol: str, timeframe: int, value: NamedTuple):
        with self._rates_lock:
            self._rates[symbol][timeframe] = value

    def copy_rates_from(self,
                        symbol: str,
                        timeframe: int = TIMEFRAME_M1,
                        date_from: datetime = None,
                        date_to: datetime = None):
        self.info(f"Iniciando a cópia dos rates para o ativo {symbol}. por favor, aguarde ...")
        self.rates = self.get_rates_from(symbol=symbol,
                                         timeframe=timeframe,
                                         date_from=date_from,
                                         date_to=date_to)



class Ticks(MT5):

    def __init__(self,
                 symbol: str,
                 account: int = None,
                 verbose: bool = False):
        super(Ticks, self).__init__(account=account, verbose=verbose)
        if account:
            self.login()
        self._symbol: str = symbol
        self._all_ticks: pd.DataFrame = pd.DataFrame()

    def __call__(self, *args, **kwargs):
        return self

    def copy_all_ticks(self, transform: bool = True):
        logger.info(f"Iniciando a cópia dos ticks para o ativo {self._symbol}. por favor, aguarde ...")
        self._all_ticks = self.get_ticks_from(symbol=self._symbol)
        logger.info(f"foram retornados {self._all_ticks.shape[0]} registros")
        if transform:
            logger.info(f"Iniciando a transformação dos ticks. Por favor, aguarde, pois pode demorar um pouco ...")
            self._all_ticks['buy_flag'] = (self._all_ticks['flags'] & mt5.TICK_FLAG_BUY) == mt5.TICK_FLAG_BUY
            self._all_ticks['sell_flag'] = (self._all_ticks['flags'] & mt5.TICK_FLAG_SELL) == mt5.TICK_FLAG_SELL
            self._all_ticks['ask_flag'] = (self._all_ticks['flags'] & mt5.TICK_FLAG_ASK) == mt5.TICK_FLAG_ASK
            self._all_ticks['bid_flag'] = (self._all_ticks['flags'] & mt5.TICK_FLAG_BID) == mt5.TICK_FLAG_BID
            self._all_ticks['last_flag'] = (self._all_ticks['flags'] & mt5.TICK_FLAG_LAST) == mt5.TICK_FLAG_LAST
            self._all_ticks['volume_flag'] = (self._all_ticks['flags'] & mt5.TICK_FLAG_VOLUME) == mt5.TICK_FLAG_VOLUME

            self._all_ticks['buy_flag'] = self._all_ticks['buy_flag'].astype("int")
            self._all_ticks['sell_flag'] = self._all_ticks['sell_flag'].astype("int")
            self._all_ticks['ask_flag'] = self._all_ticks['ask_flag'].astype("int")
            self._all_ticks['bid_flag'] = self._all_ticks['bid_flag'].astype("int")
            self._all_ticks['last_flag'] = self._all_ticks['last_flag'].astype("int")
            self._all_ticks['volume_flag'] = self._all_ticks['volume_flag'].astype("int")

            self._all_ticks.drop(['flags', 'volume_real', 'time_msc'], axis=1, inplace=True)
            logger.info(f"Transformação finalizada com sucesso.")
        return self

    def ticks(self) -> pd.DataFrame:
        return self._all_ticks

    def trade_ticks(self) -> pd.DataFrame:
        return self._all_ticks.loc[((self._all_ticks['buy_flag'] == 1) | (self._all_ticks['sell_flag'] == 1))]


if __name__ == "__main__":
    account = 52142951
    rates = Rates(symbol='ITUB4', account=account, verbose=True).copy_rates_from().rates()
    print(rates)
