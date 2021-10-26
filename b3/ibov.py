import os

import pandas as pd

from datetime import datetime, timedelta
from collections import namedtuple

from typing import NamedTuple, List
from general import Logging, DATA_PATH
from b3.request import Request


Symbol = namedtuple('Symbol', ['ticker', 'asset', 'type', 'part'])


class Ibov(Logging, Request):

    def __init__(self, verbose: bool = False):
        self._file_name = 'ibov'
        self._data_path = DATA_PATH
        self._columns = ['segment', 'cod', 'asset', 'type', 'part', 'partAcum', 'theoricalQty', ]
        self._exclude_columns = ['segment', 'partAcum', 'theoricalQty', ]
        self._table_columns = ['ticker', 'asset', 'type', 'part']
        self._filters = {}
        self._last_update = None
        self._check_folder()

        Logging.__init__(self,
                         logger_name=self._file_name,
                         verbose=verbose)
        Request.__init__(self)

    def _check_folder(self):
        _path = os.path.join(DATA_PATH, self.file_name)
        if not os.path.isdir(_path):
            os.mkdir(_path)
        self._data_path = _path

    def get_data_files(self) -> list:
        res_files = []
        try:
            for root, dirs, files in os.walk(self._data_path):
                dirs[:] = []
                for file in files:
                    if file.find(self.file_name) >= 0:
                        res_files.append(os.path.join(root, file))
        except Exception as e:
            msg = f'Error in get files {self.file_name} in folder {self._data_path} -> {e}'
            self.error(msg)
            return []
        return sorted(res_files)

    @property
    def file_name(self):
        return self._file_name

    def is_updated(self) -> bool:
        # primeira segunda-feira de Janeiro, Maio e Setembro
        _files = self.get_data_files()
        if not _files:
            return False
        self._last_update = datetime.strptime(_files[-1].split("_")[1].split(".")[0], "%Y%m%d")
        if self._last_update > (datetime.now() + timedelta(days=28)):
            return False
        return True

    def get(self):
        if self.is_updated():
            self.info(f'A última atualização da composição do índice IBOV foi em {self._last_update}. Está atualizado.')
            return

        self.info('Atualizando a composição do IBOV ...')

        url = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMCwiaW5kZXgiOiJJQk9WIiwic2VnbWVudCI6IjEifQ==" # noqa
        req = self.request(url)
        if req.status_code > 200:
            self.error(f'A requisição de dados da composição IBOV falhou: {req.status_code} -> {req.text}')
            return
        data = req.json()
        total = data['page']['totalRecords']
        date = datetime.strptime(data['header']['date'], "%d/%m/%y")
        records = data['results']
        self.info(f'Data da composição IBOV: {date} \n Total de ações: {total}')

        df = pd.DataFrame.from_dict(records)

        if df.shape[0] == 0:
            self.error(f'O dataset retornou vazio')
            return

        if self._exclude_columns:
            df = df.drop(self._exclude_columns, axis=1)
            self.info(f'excluindo as colunas {self._exclude_columns} ...')

        file = os.path.join(self._data_path, f'{self._file_name}_{date.strftime("%Y%m%d")}.csv')

        if self._table_columns:
            tot_cols = len(self._table_columns)
            if df.shape[1] != tot_cols:
                raise ValueError(f'The total number of columns in the dataset ({df.shape[1]}) does not '
                                 f'equal the columns '
                                 f'in the database table informed ({tot_cols}).')
            self.info(
                f'Renomeando as colunas do arquivo {file} para {self._table_columns}')
            df = df.set_axis(self._table_columns, axis=1)

        df['part'] = df['part'].str.replace(",", ".").astype('float')

        self.info(f'Salvando arquivo {file} ...')
        df.to_csv(file, sep=";", encoding='latin1',
                  index=False)
        self.info(f'Arquivo {file} foi salvo com sucesso!')

    def get_composition(self, min_part: float = .0,
                        ascending: bool = False) -> List[NamedTuple]:
        files = self.get_data_files()
        if not files or not self.is_updated():
            self.info(f'Não há arquivos de IBOV. \n Atualizando o IBOV ...')
            self.get()
            return self.get_composition(min_part=min_part)
        current = files[-1]
        df = pd.read_csv(current, sep=";", encoding='latin1')
        if df.shape[0] == 0:
            self.error(f'Erro ao processar o arquivo de IBOV. - > {df.shape}.')
            return []
        df['part'] = df['part'].astype('float')
        if min_part > 0:
            df = df.loc[df.part >= min_part]
        df.sort_values(by=['part'], axis=0, inplace=True, ascending=ascending)
        comp = []
        for row in range(df.shape[0]):
            comp.append(Symbol(ticker=df.iloc[row, 0],
                               asset=df.iloc[row, 1],
                               type=df.iloc[row, 2],
                               part=df.iloc[row, 3]))
        return comp


def update_ibov(verbose: bool = False):
    Ibov(verbose=verbose).get()


def get_ibov_composition(min_part: float = .0, verbose: bool = False) -> List[NamedTuple]:
    return Ibov(verbose=verbose).get_composition(min_part=min_part)
