import os
from random import randint
from threading import Thread

import pandas as pd

from tqdm import tqdm

from datetime import datetime, timedelta
from time import sleep

from general import Logging, TEMP_PATH, DATA_PATH, HOLLIDAYS, PANDAS_SEP, PANDAS_ENCODING
from b3.request import Request


class FileProcess(Logging):

    def __init__(self,
                 initial_date: datetime,
                 columns: list,
                 table_columns: list,
                 file_name: str,
                 date_columns: list = None,
                 exclude_columns: list = None,
                 filters: dict = None,
                 verbose: bool = False
                 ):
        """

        :param list columns: List with original dataset columns names for correct data extraction
        :param list table_columns:
        :param string file_name:
        :param list exclude_columns:
        :param dict filters:
        """
        self._columns = columns
        self._exclude_columns = exclude_columns
        self._table_columns = table_columns
        self._date_columns = date_columns

        self._filters = filters

        self._file_name = file_name

        self._last_update = None

        self._data_path = DATA_PATH
        self._check_folder()

        self._get_last_update(initial_date=initial_date)

        super(FileProcess, self).__init__(logger_name=file_name,
                                          verbose=verbose)

    def __repr__(self):
        return self.file_name

    def _check_folder(self):
        _path = os.path.join(DATA_PATH, self.file_name)
        if not os.path.isdir(_path):
            os.mkdir(_path)
        self._data_path = _path

    def get_temp_files(self) -> list:
        """
            Method for extract files in data folder for process
        """
        res_files = []
        try:
            for root, dirs, files in os.walk(TEMP_PATH):
                dirs[:] = []
                for file in files:
                    if file.find(self._file_name) >= 0:
                        res_files.append(os.path.join(root, file))
        except Exception as e:
            msg = f'Error in get files {self._file_name} in folder {TEMP_PATH} -> {e}'
            self.error(msg)
            return []
        return sorted(res_files)

    def get_data_files(self) -> list:
        res_files = []
        try:
            for root, dirs, files in os.walk(self._data_path):
                dirs[:] = []
                for file in files:
                    if file.find(self._file_name) >= 0:
                        res_files.append(os.path.join(root, file))
        except Exception as e:
            msg = f'Error in get files {self._file_name} in folder {self._data_path} -> {e}'
            self.error(msg)
            return []
        return sorted(res_files)

    @property
    def file_name(self):
        return self._file_name

    @property
    def last_update(self) -> datetime:
        return self._last_update

    def _get_last_update(self, initial_date: datetime):
        _files = self.get_data_files()
        if not _files:
            self._last_update = initial_date
            return 
        _last = datetime.strptime(_files[-1].split("_")[1].split(".")[0], "%Y%m%d")
        if _last > initial_date:
            self._last_update = _last + timedelta(days=1)

    def get_data(self, file: str) -> pd.DataFrame:
        try:
            _df = pd.read_csv(file, sep=PANDAS_SEP, encoding=PANDAS_ENCODING, usecols=self._columns,
                              decimal=",", thousands=".", index_col=False, engine='c')
            self.info(f'Foi extraído do arquivo {file} uma matrix com {_df.shape[1]} colunas e {_df.shape[0]} linhas')
            if self._exclude_columns:
                self.info(
                    f'Excluíndo as colunas {self._exclude_columns} do arquivo {file}')
                _df = _df.drop(self._exclude_columns, axis=1)

            if self._table_columns:
                tot_cols = len(self._table_columns)
                if _df.shape[1] != tot_cols:
                    raise ValueError(f'The total number of columns in the dataset ({_df.shape[1]}) does not '
                                     f'equal the columns '
                                     f'in the database table informed ({tot_cols}).')
                self.info(
                    f'Renomeando as colunas do arquivo {file} para {self._table_columns}')
                _df = _df.set_axis(self._table_columns, axis=1)
        except Exception as e:
            msg = f'Error in get data from {file} -> {e}'
            self.error(msg)
            return pd.DataFrame()
        else:

            for col in self._date_columns:
                _df[col] = self._transform_date(_df[col])

            _df = _df.fillna(0)

            if self._filters:
                _filtered = _df
                try:
                    for k, v in self._filters.items():
                        _filtered = _filtered[_filtered[k].isin(v)]
                        self.info(
                            f'Aplicado o filtro {k} -> {v} ao dataset {file} retornou {_filtered.shape[0]} linhas')
                        sleep(.1)
                except Exception as e:
                    msg = f'Error in apply filter {self._filters} in dataset {file} -> {e}'
                    self.error(msg)
                    return pd.DataFrame()
                else:
                    return _filtered
            return _df

    @staticmethod
    def _transform_date(serie: pd.Series,
                        date_format: str = '%Y-%m-%d') -> pd.Series:
        """

        :param pd.Series serie: String Series for transform in datetime type
        :param string date_format: String datetime format. Default: '%Y-%m-%d'
        :return: pandas Series
        """
        serie = serie.apply(
            lambda x: datetime.strptime(x, date_format) if type(x) == str else pd.to_datetime('1970-01-01'))
        return serie

    def _make_processed_file(self, file: str):
        self.info(f"Excluindo arquivo {file}  ...")
        os.remove(file)
        self.info(f"O arquivo {file} foi excluído com sucesso.")

    def process(self) -> bool:

        self.info(f'Iniciando o processamento {self._file_name} ...')

        pending_files = self.get_temp_files()

        if not pending_files:
            self.info(f'Não há arquivos de dados de {self._file_name} para processar.')
            return False

        self.info(f'A busca retornou: {pending_files}')

        for file in tqdm(pending_files):
            self.info(f'Processando arquivo {file}')
            df = self.get_data(file=file)

            if df.shape[0] == 0:
                self.error(f'O dataset retornou vazio {file}')
                return False

            name = file.split("\\")[-1].split("_")
            new_file = os.path.join(self._data_path, f'{"_".join(name[:2])}')
            csv = f'{new_file}.csv'
            self.info(f'Salvando arquivo {new_file} ...')
            df.to_csv(csv, sep=PANDAS_SEP, encoding=PANDAS_ENCODING,
                      index=False)

            self.last_update = datetime.strptime(name[1], "%Y%m%d")
            self.info(f'Última atualização em  {self.last_update} ...')

            self._make_processed_file(file)

        return True

    @last_update.setter
    def last_update(self, last_update: datetime):
        self._last_update = last_update

    def get(self):
        if self.last_update >= datetime.now():
            self.info(f'A última atualização do {self.file_name} foi em {self.last_update}.')
            return False

        dates = [self.last_update + timedelta(days=dia) for dia in range((datetime.now() - self.last_update).days + 1)]

        for date in tqdm(dates):
            if date.weekday() > 4 or date in HOLLIDAYS[date.year]:
                continue
            d_s = date.strftime("%Y-%m-%d")
            url = f"https://arquivos.b3.com.br/api/download/requestname?fileName={self.file_name}&date={d_s}&recaptchaToken=" # noqa
            req = self.request(url) # noqa
            if req.status_code > 200:
                self.error(f"Error ao pegar o link de download para {self.file_name} para {date} -> {req.text} - "
                           f"{req.status_code}")
            res = req.json()
            try:
                link = res['redirectUrl']
                token = res['token']
                filename = f"{res['file']['name']}{res['file']['extension']}"
            except KeyError:
                self.error(f'Error in get redirect link for {url} -> {res}')
                continue

            file = os.path.join(TEMP_PATH, filename)

            if os.path.exists(file):
                self.info(f'File {filename} has been downloaded.')
                continue

            url = f"https://arquivos.b3.com.br/api/download/?token={token}"
            req = self.download(url)  # noqa
            if req.status_code == 200:
                self.info(f'Fazendo download do arquivo {filename} ...')
                file = os.path.join(TEMP_PATH, filename)
                with open(file, 'wb') as f:
                    for chunk in req.iter_content(1024):
                        f.write(chunk)
                self.info(f'File {filename} downloaded with succesfull.')
            else:
                self.error(f'Error in download {url} -> {req.status_code} -> {req.text}')

            sleep(randint(1, 15))
        self.process()


class InstrumentsConsolidated(FileProcess, Request):

    def __init__(self, initial_date: datetime = datetime.now()-timedelta(days=15), verbose: bool = False):
        columns = ['RptDt',  # ReportDate
                   'TckrSymb',  # TickerSymbol
                   'Asst',  # Asset
                   'AsstDesc',  # AssetDescription
                   'SgmtNm',  # SegmentName
                   'MktNm',  # MarketName
                   'SctyCtgyNm',  # SecurityCategoryName
                   'XprtnDt',  # ExpirationDate
                   'XprtnCd',  # ExpirationCode
                   'TradgStartDt',  # TradingStartDate
                   'TradgEndDt',  # TradingEndDate
                   'BaseCd',  # BaseCode
                   'ConvsCritNm',  # ConversionCriteriaName
                   'MtrtyDtTrgtPt',  # MaturityDateTargetPoint
                   'ReqrdConvsInd',  # RequiredConversionIndicator
                   'ISIN',  # ISIN
                   'CFICd',  # CFICode
                   'DlvryNtceStartDt',  # DeliveryNoticeStartDate
                   'DlvryNtceEndDt',  # DeliveryNoticeEndDate
                   'OptnTp',  # OptionType
                   'CtrctMltplr',  # ContractMultiplier
                   'AsstQtnQty',  # AssetQuotationQuantity
                   'AllcnRndLot',  # AllocationRoundLot
                   'TradgCcy',  # TradingCurrency
                   'DlvryTpNm',  # DeliveryTypeName
                   'WdrwlDays',  # WithdrawalDays
                   'WrkgDays',  # WorkingDays
                   'ClnrDays',  # CalendarDays
                   'RlvrBasePricNm',  # RolloverBasePriceName
                   'OpngFutrPosDay',  # OpeningFuturePositionDay
                   'SdTpCd1',  # SideTypeCode1
                   'UndrlygTckrSymb1',  # UnderlyingTickerSymbol1
                   'SdTpCd2',  # SideTypeCode2
                   'UndrlygTckrSymb2',  # UnderlyingTickerSymbol2
                   'PureGoldWght',  # PureGoldWeight
                   'ExrcPric',  # ExercisePrice
                   'OptnStyle',  # OptionStyle
                   'ValTpNm',  # ValueTypeName
                   'PrmUpfrntInd',  # PremiumUpfrontIndicator
                   'OpngPosLmtDt',  # OpeningPositionLimitDate
                   'DstrbtnId',  # DistributionIdentification
                   'PricFctr',  # PriceFactor
                   'DaysToSttlm',  # DaysToSettlement
                   'SrsTpNm',  # SeriesTypeName
                   'PrtcnFlg',  # ProtectionFlag
                   'AutomtcExrcInd',  # AutomaticExerciseIndicator
                   'SpcfctnCd',  # SpecificationCode
                   'CrpnNm',  # CorporationName
                   'CorpActnStartDt',  # CorporateActionStartDate
                   'CtdyTrtmntTpNm',  # CustodyTreatmentTypeName
                   'MktCptlstn',  # MarketCapitalisation
                   'CorpGovnLvlNm'  # CorporateGovernance
                   ]
        date_columns = ['ExpirationDate', 'TradingStartDate', 'TradingEndDate', 'DeliveryNoticeStartDate',
                          'DeliveryNoticeEndDate', 'OpeningPositionLimitDate', 'lastUpdate', ]
        cols_db = ['lastUpdate', 'TickerSymbol', 'Asset', 'SegmentName', 'MarketName', 'SecurityCategoryName',
                   'ExpirationDate', 'ExpirationCode', 'TradingStartDate', 'TradingEndDate', 'BaseCode',
                   'ConversionCriteriaName', 'MaturityDateTargetPoint', 'RequiredConversionIndicator', 'ISIN',
                   'CFICode', 'DeliveryNoticeStartDate', 'DeliveryNoticeEndDate', 'OptionType',
                   'ContractMultiplier', 'AssetQuotationQuantity', 'AllocationRoundLot', 'TradingCurrency',
                   'WithdrawalDays', 'WorkingDays', 'CalendarDays', 'SideTypeCode1', 'UnderlyingTickerSymbol1',
                   'SideTypeCode2', 'UnderlyingTickerSymbol2', 'ExercisePrice', 'OptionStyle', 'ValueTypeName',
                   'PremiumUpfrontIndicator', 'OpeningPositionLimitDate', 'DistributionIdentification',
                   'PriceFactor', 'DaysToSettlement', 'SeriesTypeName', 'ProtectionFlag',
                   'AutomaticExerciseIndicator', 'SpecificationCode', 'CorporationName', 'MarketCapitalisation',
                   'CorporateGovernance', ]
        exclude_cols = ['RlvrBasePricNm', 'OpngFutrPosDay', 'PureGoldWght', 'CorpActnStartDt', 'CtdyTrtmntTpNm',
                        'AsstDesc', 'DlvryTpNm', ]
        filters = {'SecurityCategoryName': ['SHARES', 'BDR', 'UNIT', 'STOCK FUTURE', 'ETF EQUITIES', 'INDEX',
                                            'OPTION ON EQUITIES', 'OPTION ON INDEX'],
                   }
        FileProcess.__init__(self,
                             initial_date=initial_date,
                             date_columns=date_columns,
                             file_name='InstrumentsConsolidated',
                             columns=columns,
                             table_columns=cols_db,
                             exclude_columns=exclude_cols,
                             filters=filters,
                             verbose=verbose)

        Request.__init__(self)


class EconomicIndicator(FileProcess, Request):

    def __init__(self, initial_date: datetime = datetime.now()-timedelta(days=15), verbose: bool = False):
        columns = ['RptDt',  # ReportDate
                   'Asst',  # Asset
                   'TckrSymb',  # TickerSymbol
                   'EcncIndDesc',  # EconomicIndicatorDescription
                   'PricVal'  # PriceValue
                   ]
        cols_db = ['ReportDate', 'Asset', 'TickerSymbol', 'EconomicIndicatorDescription', 'PriceValue', ]
        exclude_cols = []
        filters = {}
        date_columns = ['ReportDate', ]
        FileProcess.__init__(self,
                             initial_date=initial_date,
                             date_columns=date_columns,
                             file_name='EconomicIndicatorPrice',
                             columns=columns,
                             table_columns=cols_db,
                             exclude_columns=exclude_cols,
                             filters=filters,
                             verbose=verbose)

        Request.__init__(self)

    def get_economic_indicator(self, from_date: datetime) -> pd.DataFrame:
        """
        Get Economic Indicator from B3
        """
        files = self.get_data_files()
        if not files:
            self.error(f'Not files for Economic Indicators')
            self.get()
            return self.get_economic_indicator(from_date=from_date)
        df = pd.DataFrame()
        for file in files:
            d_file = datetime.strptime(file.split("\\")[-1].split(".")[0].split("_")[1], "%Y%m%d")
            if d_file >= from_date:
                df = df.append(pd.read_csv(file, sep=PANDAS_SEP, encoding=PANDAS_ENCODING), ignore_index=True)
        return df


class LendingOpenPosition(FileProcess, Request):

    def __init__(self, initial_date: datetime = datetime.now()-timedelta(days=15), verbose: bool = False):
        columns = ['RptDt',  # ReportDate
                   'TckrSymb',  # TickerSymbol
                   'ISIN',
                   'Asst',  # Asset
                   'BalQty',  # BalanceQuantity
                   'TradAvrgPric',  # TradeAveragePrice
                   'PricFctr',  # PriceFactor
                   'BalVal'  # BalanceValue
                   ]
        cols_db = ['ReportDate', 'TickerSymbol', 'ISIN', 'Asset', 'BalanceQuantity', 'TradeAveragePrice', 'PriceFactor',
                   'BalanceValue', ]
        exclude_cols = []
        date_columns = ['ReportDate', ]
        filters = {}
        FileProcess.__init__(self,
                             initial_date=initial_date,
                             date_columns=date_columns,
                             file_name='LendingOpenPosition',
                             columns=columns,
                             table_columns=cols_db,
                             exclude_columns=exclude_cols,
                             filters=filters,
                             verbose=verbose)

        Request.__init__(self)


class DerivativesOpenPosition(FileProcess, Request):

    def __init__(self, initial_date: datetime = datetime.now()-timedelta(days=15), verbose: bool = False):
        columns = ['RptDt',  # ReportDate
                   'TckrSymb',  # TickerSymbol
                   'ISIN',  # ISIN
                   'Asst',  # Asset
                   'XprtnCd',  # ExpirationCode
                   'SgmtNm',  # SegmentName
                   'OpnIntrst',  # OpenInterest
                   'VartnOpnIntrst',  # VariationOpenInterest
                   'DstrbtnId',  # DistributionIdentification
                   'CvrdQty',  # CoveredQuantity
                   'TtlBlckdPos',  # TotalBlockedPosition
                   'UcvrdQty',  # UncoveredQuantity
                   'TtlPos',  # TotalPosition
                   'BrrwrQty',  # BorrowerQuantity
                   'LndrQty',  # LenderQuantity
                   'CurQty',  # CurrentQuantity
                   'FwdPric'  # ForwardPrice
                   ]
        cols_db = ['ReportDate', 'TickerSymbol', 'ISIN', 'Asset', 'ExpirationCode', 'SegmentName', 'OpenInterest',
                   'VariationOpenInterest', 'DistributionIdentification', 'CoveredQuantity', 'TotalBlockedPosition',
                   'UncoveredQuantity', 'TotalPosition', 'BorrowerQuantity', 'LenderQuantity',
                   'CurrentQuantity', 'ForwardPrice']
        exclude_cols = []
        date_columns = ['ReportDate', ]
        filters = {'SegmentName': ['FINANCIAL', 'EQUITY CALL', 'EQUITY PUT', 'EQUITY FORWARD']}
        FileProcess.__init__(self,
                             initial_date=initial_date,
                             date_columns=date_columns,
                             file_name='DerivativesOpenPosition',
                             columns=columns,
                             table_columns=cols_db,
                             exclude_columns=exclude_cols,
                             filters=filters,
                             verbose=verbose)

        Request.__init__(self)


class TradeInformationConsolidated(FileProcess, Request):

    def __init__(self, initial_date: datetime = datetime.now()-timedelta(days=15), verbose: bool = False):
        columns = ['RptDt',  # ReportDate
                   'TckrSymb',  # TickerSymbol
                   'ISIN',  # ISIN
                   'SgmtNm',  # SegmentName
                   'MinPric',  # MinimumPrice
                   'MaxPric',  # MaximumPrice
                   'TradAvrgPric',  # TradeAveragePrice
                   'LastPric',  # LastPrice
                   'AdjstdQt',  # AdjustedQuote
                   'AdjstdQtTax',  # AdjustedQuoteTax
                   'RefPric',  # ReferencePrice
                   'TradQty',  # TradeQuantity
                   'FinInstrmQty'  # FinancialInstrumentQuantity
                   ]
        cols_db = ['ReportDate', 'TickerSymbol', 'ISIN', 'SegmentName', 'MinimumPrice', 'MaximumPrice',
                   'TradeAveragePrice', 'LastPrice', 'AdjustedQuote', 'AdjustedQuoteTax', 'ReferencePrice',
                   'TradeQuantity', 'FinancialInstrumentQuantity', ]
        exclude_cols = []
        date_columns = ['ReportDate', ]
        filters = {'SegmentName': ['FINANCIAL', 'EQUITY CALL', 'EQUITY PUT', 'EQUITY FORWARD']}
        FileProcess.__init__(self,
                             initial_date=initial_date,
                             date_columns=date_columns,
                             file_name='TradeInformationConsolidated',
                             columns=columns,
                             table_columns=cols_db,
                             exclude_columns=exclude_cols,
                             filters=filters,
                             verbose=verbose)

        Request.__init__(self)


def update_consolidated_data(verbose: bool = False):
    """
    Function for update consolidated data from B3

    :param verbose: (bool) default False -> For verbose steps
    :return: None
    """
    tasks = [InstrumentsConsolidated, EconomicIndicator, LendingOpenPosition, DerivativesOpenPosition,
             TradeInformationConsolidated]
    for t in tasks:
        f = t(verbose=verbose)
        Thread(target=f.get, name=f).start()


def get_economic_indicator(from_date: datetime = datetime.now() - timedelta(days=10),
                           verbose: bool = False) -> pd.DataFrame:
    return EconomicIndicator(verbose=verbose).get_economic_indicator(from_date=from_date)
