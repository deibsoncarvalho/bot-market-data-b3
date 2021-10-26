import os
import time

from random import randint
from datetime import timedelta, datetime
from typing import Dict, List, Union

import requests
from dateutil.relativedelta import relativedelta
from threading import Thread

import pandas as pd
import numpy as np

from tqdm import tqdm

from general import (
    Logging,
    TEMP_PATH,
    DATA_PATH,
    HOLLIDAYS,
    TIME_FINISH_TRADING,
    ChainedAssignent,
    PANDAS_SEP,
    PANDAS_ENCODING
)
from b3.request import Request


DEFAULT_FROM_DATE_YEARLY: int = datetime.now().year
DEFAULT_TO_DATE_YEARLY: int = datetime.now().year


DEFAULT_FROM_DATE_MONTHLY: datetime = datetime.now() - relativedelta(months=2)
DEFAULT_TO_DATE_MONTHLY: datetime = datetime.now()-relativedelta(months=1)


DEFAULT_FROM_DATE_DAILY: datetime = datetime(datetime.now().year,
                                    datetime.now().month,
                                    datetime.now().day, 0, 0, 0) - timedelta(days=15)
DEFAULT_TO_DATE_DAILY: datetime = datetime(datetime.now().year,
                                  datetime.now().month,
                                  datetime.now().day, 0, 0, 0) - timedelta(days=1)


TABLE_CODBDI: Dict[str, str] = {
            '02': 'LOTE PADRAO',
            '05': 'SANCIONADAS PELOS REGULAMENTOS BMFBOVESPA',
            '06': 'CONCORDATARIAS',
            '07': 'RECUPERACAO EXTRAJUDICIAL',
            '08': 'RECUPERAÇÃO JUDICIAL',
            '09': 'RAET - REGIME DE ADMINISTRACAO ESPECIAL TEMPORARIA',
            '10': 'DIREITOS E RECIBOS',
            '11': 'INTERVENCAO',
            '12': 'FUNDOS IMOBILIARIOS',
            '14': 'CERT.INVEST/TIT.DIV.PUBLICA',
            '18': 'OBRIGACÕES',
            '22': 'BÔNUS (PRIVADOS)',
            '26': 'APOLICES/BÔNUS/TITULOS PUBLICOS',
            '32': 'EXERCICIO DE OPCOES DE COMPRA DE INDICES',
            '33': 'EXERCICIO DE OPCOES DE VENDA DE INDICES',
            '38': 'EXERCICIO DE OPCOES DE COMPRA',
            '42': 'EXERCICIO DE OPCOES DE VENDA',
            '46': 'LEILAO DE NAO COTADOS',
            '48': 'LEILAO DE PRIVATIZACAO',
            '49': 'LEILAO DO FUNDO RECUPERACAO ECONOMICA ESPIRITO SANTO',
            '50': 'LEILAO',
            '51': 'LEILAO FINOR',
            '52': 'LEILAO FINAM',
            '53': 'LEILAO FISET',
            '54': 'LEILAO DE ACÕES EM MORA',
            '56': 'VENDAS POR ALVARA JUDICIAL',
            '58': 'OUTROS',
            '60': 'PERMUTA POR ACÕES',
            '61': 'META',
            '62': 'MERCADO A TERMO',
            '66': 'DEBENTURES COM DATA DE VENCIMENTO ATE 3 ANOS',
            '68': 'DEBENTURES COM DATA DE VENCIMENTO MAIOR QUE 3 ANOS',
            '70': 'FUTURO COM RETENCAO DE GANHOS',
            '71': 'MERCADO DE FUTURO',
            '74': 'OPCOES DE COMPRA DE INDICES',
            '75': 'OPCOES DE VENDA DE INDICES',
            '78': 'OPCOES DE COMPRA',
            '82': 'OPCOES DE VENDA',
            '83': 'BOVESPAFIX',
            '84': 'SOMA FIX',
            '90': 'TERMO VISTA REGISTRADO',
            '96': 'MERCADO FRACIONARIO',
            '99': 'TOTAL GERAL',
        }


TABLE_ESPECI: Dict[str, str] = {
            'BDR': 'BDR',
            'BNS': 'BÔNUS DE SUBSCRIÇÃO EM ACÕES MISCELÂNEA',
            'BNS B/A': 'BÔNUS DE SUBSCRIÇÃO EM ACÕES PREFERÊNCIA',
            'BNS ORD': 'BÔNUS DE SUBSCRIÇÃO EM ACÕES ORDINÁRIAS',
            'BNS P/A': 'BÔNUS DE SUBSCRIÇÃO EM ACÕES PREFERÊNCIA',
            'BNS P/B': 'BÔNUS DE SUBSCRIÇÃO EM ACÕES PREFERÊNCIA',
            'BNS P/C': 'BÔNUS DE SUBSCRIÇÃO EM ACÕES PREFERÊNCIA',
            'BNS P/D': 'BÔNUS DE SUBSCRIÇÃO EM ACÕES PREFERÊNCIA',
            'BNS P/E': 'BÔNUS DE SUBSCRIÇÃO EM ACÕES PREFERÊNCIA',
            'BNS P/F': 'BÔNUS DE SUBSCRIÇÃO EM ACÕES PREFERÊNCIA',
            'BNS P/G': 'BÔNUS DE SUBSCRIÇÃO EM ACÕES PREFERÊNCIA',
            'BNS P/H': 'BÔNUS DE SUBSCRIÇÃO EM ACÕES PREFERÊNCIA',
            'BNS PRE': 'BÔNUS DE SUBSCRIÇÃO EM ACÕES PREFERÊNCIA',
            'CDA': 'CERTIFICADO DE DEPÓSITO DE ACÕES ORDINÁRIAS',
            'CI': 'FUNDO DE INVESTIMENTO',
            'CI ATZ': 'Fundo de Investimento Atualização',
            'CI EA': 'Fundo de Investimento Ex-Atualização',
            'CI EBA': 'Fundo de Investimento Ex-Bonificação e Ex-Atualização',
            'CI ED': 'Fundo de Investimento Ex-dividendo',
            'CI ER': 'Fundo de Investimento Ex-Rendimento',
            'CI ERA': 'Fundo de Investimento Ex-rendimento e Ex-Atualização',
            'CI ERB': 'Fundo de Investimento Ex-rendimento e Ex-Bonificação',
            'CI ERS': 'Fundo de Investimento Ex-Rendimento e Ex-Subscrição',
            'CI ES': 'Fundo de Investimento Ex-Subscrição',
            'CPA': 'CERTIF. DE POTENCIAL ADIC. DE CONSTRUÇÃO',
            'DIR': 'DIREITOS DE SUBSCRIÇÃO MISCELÂNEA (BÔNUS)',
            'DIR DEB': 'Direito de Debênture',
            'DIR ORD': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES ORDINÁRIAS',
            'DIR P/A': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'DIR P/B': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'DIR P/C': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'DIR P/D': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'DIR P/E': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'DIR P/F': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'DIR P/G': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'DIR P/H': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'DIR PR': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES RESGATÁVEIS',
            'DIR PRA': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES RESGATÁVEIS',
            'DIR PRB': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES RESGATÁVEIS',
            'DIR PRC': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES RESGATÁVEIS',
            'DIR PRE': 'DIREITOS DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'FIDC': 'Fundo de Investimento em Direitos Creditórios',
            'LFT': 'LETRA FINANCEIRA DO TESOURO',
            'M1 REC': 'RECIBO DE SUBSCRIÇÃO DE MISCELÂNEAS',
            'ON': 'ACÕES ORDINÁRIAS NOMINATIVAS',
            'ON ATZ': 'Ações Ordinárias Atualização',
            'ON EB': 'Ações Ordinárias Ex-Bonificação',
            'ON ED': 'Ações Ordinárias Ex-Dividendo',
            'ON EDB': 'Ações Ordinárias Ex-Dividendo e Ex-Bonificação',
            'ON EDJ': 'Ações Ordinárias Ex-dividendo e Ex-Juros',
            'ON EDR': 'Ações Ordinárias Ex-Dividendo e Ex-Rendimento',
            'ON EG': 'Ações Ordinárias Ex-Grupamento',
            'ON EJ': 'Ações Ordinárias Ex-juros',
            'ON EJB': 'Ações Ordinárias Ex-juros e Ex-bonificação',
            'ON EJS': 'Ações Ordinárias Ex-Juros e Ex-Subscrição',
            'ON ER': 'Ações Ordinárias Ex-Rendimento',
            'ON ERJ': 'Ações Ordinárias Ex-Rendimento e Ex-Juros',
            'ON ES': 'Ações Ordinárias Ex-Subscrição',
            'ON P': 'ACÕES ORDINÁRIAS NOMINATIVAS COM DIREITO',
            'ON REC': 'RECIBO DE SUBSCRIÇÃO EM ACÕES ORDINÁRIAS',
            'OR': 'ACÕES ORDINÁRIAS NOMINATIVAS RESGATÁVEIS',
            'OR P': 'ACÕES ORDINÁRIAS NOMINATIVAS RESGATÁVEIS',
            'PCD': 'POSIÇÃO CONSOLIDADA DA DIVIDA',
            'PN': 'ACÕES PREFERÊNCIAIS NOMINATIVAS',
            'PN EB': 'Ações Preferenciais Ex-Bonificação',
            'PN ED': 'Ações Preferenciais Ex-Dividendo',
            'PN EDB': 'Ações Preferenciais Ex-Dividendo e Ex-Bonificação',
            'PN EDJ': 'Ações Preferenciais Ex-dividendo e Ex-Juros',
            'PN EDR': 'Ações Preferenciais Ex-Dividendo e Ex-Rendimento',
            'PN EJ': 'Ações Preferenciais Ex-Juros',
            'PN EJB': 'Ações Preferenciais Ex-juros e Ex-bonificação',
            'PN EJS': 'Ações Preferenciais Ex-Juros e Ex-Subscrição',
            'PN ES': 'Ações Preferenciais Ex-Subscrição',
            'PN P': 'ACÕES PREFERÊNCIAIS NOMINATIVAS COM DIREITO',
            'PN REC': 'RECIBO DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'PNA ACÕES': 'PREFERÊNCIAIS NOMINATIVAS CLASSE A',
            'PNA EB': 'Ações Preferenciais Classe A Ex-BonificaçãoPreferencial',
            'PNA EDR': 'Ações Preferenciais Classe A Ex-Dividendo e Ex-Rendimento',
            'PNA EJ': 'Ações Preferenciais Classe A Ex-Juros',
            'PNA ES': 'Ações Preferenciais Classe A Preferencial Ex-Subscrição',
            'PNA P': 'ACÕES PREFERÊNCIAIS NOMINATIVAS CLASSE A',
            'PNA REC': 'RECIBO DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'PNB': 'ACÕES PREFERÊNCIAIS NOMINATIVAS CLASSE B',
            'PNB EB': 'Ações Preferenciais Classe B Ex-Bonificação',
            'PNB ED': 'Ações Preferenciais Classe B Ex-Dividendo',
            'PNB EDR': 'Ações Preferenciais Classe B Ex-Dividendo e Ex-Rendimento',
            'PNB EJ': 'Ações Preferenciais Classe B Ex-Juros',
            'PNB P': 'ACÕES PREFERÊNCIAIS NOMINATIVAS CLASSE B',
            'PNB REC': 'RECIBO DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'PNC': 'ACÕES PREFERÊNCIAIS NOMINATIVAS CLASSE C',
            'PNC ED': 'Ações Preferenciais Classe C Preferencial Classe C Ex-Dividendo',
            'PNC P': 'ACÕES PREFERÊNCIAIS NOMINATIVAS CLASSE C',
            'PNC REC': 'RECIBO DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'PND': 'ACÕES PREFERÊNCIAIS NOMINATIVAS CLASSE D',
            'PND ED': 'Ações Preferenciais Classe D Ex-Dividendo',
            'PND P': 'ACÕES PREFERÊNCIAIS NOMINATIVAS CLASSE D',
            'PND REC': 'RECIBO DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
            'PNE': 'ACÕES PREFERÊNCIAIS NOMINATIVAS CLASSE E',
            'PNE ED': 'Ações Preferenciais Classe E Ex-Dividendo',
            'PNE P': 'ACÕES PREFERÊNCIAIS NOMINATIVAS CLASSE E',
            'PNE REC': 'RECIBO DE SUBSCRIÇÃO EM ACÕES PREFERENCIAIS',
        }


TABLE_INDOPC: Dict[str, str] = {
            '1': 'US$ CORREÇÃO PELA TAXA DO DÓLAR 2 TJLP CORREÇÃO PELA TJLP',
            '8': 'IGPM CORREÇÃO PELO IGP-M - OPÇÕES PROTEGIDAS 9',
            'URV': 'CORREÇÃO PELA URV'
        }


TABLE_TPMERC: Dict[str, str] = {
            '010': 'VISTA',
            '012': 'EXERCÍCIO DE OPÇÕES DE COMPRA',
            '013': 'EXERCÍCIO DE OPÇÕES DE VENDA',
            '017': 'LEILÃO',
            '020': 'FRACIONÁRIO',
            '030': 'TERMO',
            '050': 'FUTURO COM RETENÇÃO DE GANHO',
            '060': 'FUTURO COM MOVIMENTAÇÃO CONTÍNUA',
            '070': 'OPÇÕES DE COMPRA',
            '080': 'OPÇÕES DE VENDA',
        }


class HistoricalQuotes(Logging, Request):

    def __init__(self, verbose: bool = True):
        Logging.__init__(self, 'HistoricalQuotes', verbose)
        Request.__init__(self)
        self._path_data: str = DATA_PATH
        self._sub_dirs: Dict[str, List[str]] = {
            'year': ['COTAHIST_A', 'COTAHIST_A'],
            'month': ['COTAHIST_M', 'COTAHIST_M'],
            'day': ['COTAHIST_D', 'COTAHIST_D']
        }
        self._file_name: str = 'HistoricalQuotes'
        self._check_folder()

    def _check_temp_file(self,
                         sub_dir: str,
                         term: str) -> bool:
        files = self._get_temp_files(sub_dir)
        if not files:
            return False
        for file in files:
            if file.find(term) >= 0:
                return True
        return False

    def _check_data_file(self,
                         sub_dir: str,
                         term: str) -> bool:
        files = self._get_data_files(sub_dir)
        if not files:
            return False
        for file in files:
            if file.find(term) >= 0:
                return True
        return False

    def _get_temp_files(self, sub_dir: str):
        res_files = []
        try:
            for root, dirs, files in os.walk(TEMP_PATH):
                dirs[:] = []
                for file in files:
                    if file.find(self._sub_dirs[sub_dir][0]) >= 0:
                        res_files.append(os.path.join(root, file))
        except Exception as e:
            msg = f'Error in get files {sub_dir} in folder {TEMP_PATH} -> {e}'
            self.error(msg)
            return []
        return sorted(res_files)

    def _get_data_files(self, sub_dir: str):
        res_files = []
        _path = os.path.join(self._data_path, sub_dir)
        try:
            for root, dirs, files in os.walk(_path):
                dirs[:] = []
                for file in files:
                    if file.find(self._sub_dirs[sub_dir][1]) >= 0:
                        res_files.append(os.path.join(root, file))
        except Exception as e:
            msg = f'Error in get files {self._sub_dirs[sub_dir][1]} in folder {_path} -> {e}'
            self.error(msg)
            return []
        return sorted(res_files)

    def get_yearly_data(self, from_year: int = DEFAULT_FROM_DATE_YEARLY,
                        to_year: int = DEFAULT_TO_DATE_YEARLY):

        self.info(f'Iniciando a busca de cotação história para {from_year} até {to_year} ...')

        years = [y for y in range(from_year, to_year + 1)]

        for year in tqdm(years):

            # O arquivo anual do ano corrente é atualizado diariamente
            if datetime.now().year == year:
                file_name = f'COTAHIST_A{year}_{datetime.now().strftime("%Y%m%d")}.ZIP'
            else:
                file_name = f'COTAHIST_A{year}.ZIP'

            if datetime.now().weekday() > 4 or datetime(datetime.now().year, datetime.now().month, datetime.now().day) in HOLLIDAYS.get(datetime.now().year, []) or \
               self._check_temp_file(sub_dir='year', term=file_name.split(".")[0]) or \
               self._check_data_file(sub_dir='year', term=file_name.split(".")[0]):
                self.info(f'o arquivo para o ano {year} atual já foi baixado.')
                continue

            self.info(f'baixando o arquivo para o ano {year} ...')

            url = f'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A{year}.ZIP'

            try:
                req = self.download(url)
            except requests.exceptions.ConnectionError as e:
                self.error(f'Error in download file {file_name}. \n Connection error -> {e}. \n '
                           f'Waiting 300 seconds for new try.')
                time.sleep(300)
                self.info(f'Iniciando nova tentativa para download dos arquivos anuais ...')
                return self.get_yearly_data(from_year=from_year, to_year=to_year)

            if req.status_code == 200:

                self.info(f'Arquivo do ano {year} encontrado com sucesso. \n baixando ...')

                file = os.path.join(TEMP_PATH, file_name)

                if datetime.now().year == year:
                    # limpa os arquivos anteriores
                    files_data = self._get_temp_files("year")
                    self.info(f'excluíndo os arquivos do ano atual .. ')
                    for _f in files_data:
                        if _f.find(datetime.now().strftime("A%Y")) >= 0:
                            os.remove(_f)

                with open(file, 'wb') as f:
                    for chunk in req.iter_content(1024):
                        f.write(chunk)
                self.info(f'download do arquivo {file_name} concluído com sucesso.')

                Thread(target=self._process_df, args=[file, 'year'], name=file_name).start()
            else:
                self.error(f"a requisição '{url}' retornou {req.status_code} -> \n {req.text}")

            time.sleep(randint(1, 15))

        Thread(target=self.process_yearly_data, name='process_yearly_data').start()

    def get_monthly_data(self, from_month_year: datetime = DEFAULT_FROM_DATE_MONTHLY,
                         to_month_year: datetime = DEFAULT_TO_DATE_MONTHLY):

        #url = "https://bvmf.bmfbovespa.com.br/pt-br/cotacoes-historicas/FormConsultaValida.asp?arq=COTAHIST_M092020.ZIP" # noqa
        months = [from_month_year + relativedelta(months=m) for m in range(((to_month_year.year - from_month_year.year) * 12 + to_month_year.month - from_month_year.month) + 1)]

        for month in months:

            if self._check_temp_file(sub_dir='month', term=month.strftime("%m%Y")) or \
               self._check_data_file(sub_dir='month', term=month.strftime("%m%Y")) or \
               month.month == datetime.now().month:
                self.info(f'o arquivo para a data {month.strftime("%m/%Y")} indisponível.')
                continue

            filename = f'COTAHIST_M{month.strftime("%m%Y")}.ZIP'
            url = f'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/{filename}'

            try:
                req = self.download(url)
                self.info(f'iniciando o download do arquivo {filename} ...')
            except Exception as e:
                self.error(f'requisição do arquivo {filename} falhou -> {e}')

            if req.status_code == 200:
                file = os.path.join(TEMP_PATH, filename)
                with open(file, 'wb') as f:
                    for chunk in req.iter_content(1024):
                        f.write(chunk)
                self.info(f'download do arquivo {filename} foi concluído!')
            else:
                self.error(f"a requisição '{url}' retornou {req.status_code} -> \n {req.text}")
            time.sleep(randint(1, 15))

    def get_daily_data(self,
                       from_date: datetime = DEFAULT_FROM_DATE_DAILY,
                       to_date: datetime = DEFAULT_TO_DATE_DAILY):

        dates = [from_date + timedelta(days=dia) for dia in range((to_date - from_date).days + 1)]

        for date in dates:

            if date.weekday() > 4 or datetime(date.year, date.month, date.day) in HOLLIDAYS.get(date.year, []) or \
               self._check_temp_file(sub_dir='day', term=date.strftime("%d%m%Y")) or \
               self._check_data_file(sub_dir='day', term=date.strftime("%d%m%Y")) or \
               date >= datetime(datetime.now().year, datetime.now().month, datetime.now().day, 0, 0, 0):
                self.info(f'o arquivo para a data {date.strftime("%d/%m/%Y")} indisponível.')
                continue

            filename = f'COTAHIST_D{date.strftime("%d%m%Y")}.ZIP'
            url = f'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/{filename}'
            try:
                req = self.download(url)
                self.info(f'iniciando o download do arquivo {filename} ...')
            except Exception as e:
                self.error(f'requisição do arquivo {filename} falhou -> {e}')

            if req.status_code == 200:
                file = os.path.join(TEMP_PATH, filename)
                with open(file, 'wb') as f:
                    for chunk in req.iter_content(1024):
                        f.write(chunk)
                self.info(f'download do arquivo {filename} foi concluído!')
            else:
                self.error(f"a requisição '{url}' retornou {req.status_code} -> \n {req.text}")
            time.sleep(randint(1, 15))
        self.process_daily_data()

    def _check_folder(self):
        def create(path):
            if not os.path.isdir(path):
                os.mkdir(path)
        _path = os.path.join(DATA_PATH, self._file_name)
        create(_path)
        for sub in self._sub_dirs.keys():
            _sub_path = os.path.join(_path, sub)
            create(_sub_path)
        self._data_path = _path

    def _process_df(self, file: str, sub_dir: str) -> bool:
        path_data = os.path.join(self._data_path, sub_dir)
        columns = {
            'tipoRegistro': [0, 2],
            'dataPregao': [2, 10],
            'codigoBDI': [10, 12],
            'CodigoNegociacaoPapel': [12, 24],
            'tipoMercado': [24, 27],
            'nomeEmpresa': [27, 39],
            'especificacaoPapel': [39, 49],
            'prazoMercadoriaATermo': [49, 52],
            'moedaReferencia': [52, 56],
            'abertura': [56, 69],
            'maximo': [69, 82],
            'minimo': [82, 95],
            'precoMedio': [95, 108],
            'ultimo': [108, 121],
            'precoMelhorOfertaCompra': [121, 134],
            'precoMelhorOfertaVenda': [134, 147],
            'numeroNegociosEfetuados': [147, 152],
            'totalTitulosNegociados': [152, 170],
            'volumeTotalTitulosNegociados': [170, 188],
            'precoExercicioOuValorContrato': [188, 201],
            'indicadorCorrecaoFuturos': [201, 202],
            'vencimentoFuturos': [202, 210],
            'fatorCotacao': [210, 217],
            'precosExercicioFuturosEmDolar': [217, 230],
            'isin': [230, 242],
            'numeroDistribuicaoPapel': [242, 245]
        }

        self.info(f'processando os dados do arquivo {file} ...')
        with ChainedAssignent():
            df = pd.read_csv(file, skiprows=1, skipfooter=1, encoding='latin1', engine='python', index_col=False,
                             header=None)

        if df.shape[0] == 0:
            self.error(f'O dataset retornou vazio {file}')
            return False

        # Fatia a coluna e divide os dados conforme o layout da B3
        self.info(f'Dividindo os dados em pelas colunas do {file} ...')
        for col, pos in columns.items():
            df[col] = df[0].str[pos[0]: pos[1]].str.strip()

        df.drop([0], axis=1, inplace=True)

        exclude_cols = ['tipoRegistro', 'isin', 'numeroDistribuicaoPapel', 'indicadorCorrecaoFuturos',
                            'indicadorCorrecaoFuturos', ]

        # Exclui a primeira coluna
        self.info(f'excluindo as colunas {exclude_cols} do arquivo {file} ...')

        df.drop(exclude_cols, axis=1, inplace=True)

        # Converte as colunas de data
        date_cols = ['dataPregao', 'vencimentoFuturos', ]
        self.info(f'convertendo as colunas de data ({date_cols}) do arquivo {file} ...')
        for col in date_cols:
            df[col] = self._transform_date(df[col])

        # Preenche os campos vazios com 0
        df.fillna(0, axis=1, inplace=True)

        # Converte as colunas inteiras
        numeric_cols = ['numeroNegociosEfetuados', 'totalTitulosNegociados', 'volumeTotalTitulosNegociados',
                        'prazoMercadoriaATermo', ]
        self.info(f'convertendo as colunas de valores numéricos ({numeric_cols}) do arquivo {file} ...')
        for num in numeric_cols:
            df[num] = self._transform_int(df[num])

        # Converte as colunas de ponto flutuante
        float_cols = ['abertura', 'maximo', 'minimo', 'precoMedio', 'ultimo', 'precoMelhorOfertaCompra',
                      'precoMelhorOfertaVenda', 'precoExercicioOuValorContrato', 'precosExercicioFuturosEmDolar', ]
        self.info(f'convertendo as colunas de valores decimais ({float_cols}) do arquivo {file} ...')
        for fl in float_cols:
            df[fl] = df[fl].apply(lambda x: f'{x[:len(x)-2]}.{x[len(x)-2:]}' if x else 0).astype('float64')

        # Se o arquivo for anual
        if sub_dir == 'year':
            year = int(file.split("//")[-1].split("_")[1].split(".")[0].replace("A", ""))
            if datetime.now().year == year:
                filename = os.path.join(path_data, f'{self._sub_dirs[sub_dir][1]}{year}_{datetime.now().strftime("%Y%m%d")}.csv') # noqa
            else:
                filename = os.path.join(path_data, f'{self._sub_dirs[sub_dir][1]}{year}.csv')
        elif sub_dir == 'month':
            raise NotImplementedError()
        else:
            raise NotImplementedError()

        if not self._check_data_file(sub_dir=sub_dir, term=filename):
            if sub_dir == 'year':
                if datetime.now().year == int(year):
                    # limpa os arquivos anteriores
                    files_data = self._get_data_files("year")
                    self.info(f'excluíndo os arquivos do ano atual .. ')
                    for _f in files_data:
                        if _f.find(datetime.now().strftime("%Y")) >= 0:
                            os.remove(_f)
            self.info(f'salvando o arquivo {filename} na pasta de dados ...')
            df.to_csv(filename, sep=PANDAS_SEP, encoding=PANDAS_ENCODING, index=False)
            self.info(f'arquivo {filename} salvo com sucesso!')
        else:
            self.info(f'o arquivo {filename} foi salvo anteriormente no disco.')

        self.info(f'removendo o arquivo {file} dos temporários ...')
        os.remove(file)
        self.info(f'o arquivo {file} foi removido com sucesso!')
        return True

    @staticmethod
    def _transform_date(serie: pd.Series,
                        date_format: str = '%Y%m%d') -> pd.Series:
        """

        :param pd.Series serie: String Series for transform in datetime type
        :param string date_format: String datetime format. Default: '%Y-%m-%d'
        :return: pandas Series
        """
        serie = serie.apply(
            lambda x: datetime.strptime(x, date_format) if type(x) == str and x else pd.to_datetime('1970-01-01'))
        return serie

    @staticmethod
    def _transform_int(serie: pd.Series) -> pd.Series:
        """
        :param pd.Series serie: String Series for transform in int type
        :return: pandas Series
        """
        serie = serie.apply(
            lambda x:  np.int(x) if type(x) == str and x else np.int(0))
        return serie.astype('int64')

    def process_yearly_data(self, file: str = None):
        self.info('iniciando o processamento dos arquivos de dados anuais ...')
        if not file:
            files = self._get_temp_files("year")
            self.info(f'Há {len(files)} para processar. \n arquivos: \n {files}')
            for file in tqdm(files):
                Thread(target=self._process_df, args=[file, 'year']).start()
                time.sleep(.2)
        else:
            self._process_df(file, 'year')
        self.info('processamento dos arquivos de cotações anuais concluído com sucesso!')

    def process_daily_data(self):
        raise NotImplementedError()

    def process_monthly_data(self):
        raise NotImplementedError()

    def get_yearly_history_quote(self,
                                 instruments: List[str] = None,
                                 months: int = 0) -> pd.DataFrame:

        if not isinstance(instruments, list):
            raise ValueError('O parâmetro "instruments" deve ser uma lista.')

        _temp = self._get_data_files(sub_dir='year')
        if not _temp:
            raise ValueError(f'Não há dados históricos')

        today = datetime.today()
        if months > 0:
            from_date = today - relativedelta(month=months)
            files = []
            for f in _temp:
                if int(f.split("_")[1].split(".")[0].replace("A", "")) >= from_date.year:
                    files.append(f)
        else:
            files = _temp

        df = pd.DataFrame()

        for file in tqdm(files):
            _df = pd.read_csv(file, sep=PANDAS_SEP, encoding=PANDAS_ENCODING)
            if instruments:
                _df = _df[_df['CodigoNegociacaoPapel'].isin(instruments)]
            df = pd.DataFrame.append(df, _df, ignore_index=True)

        exclude = ['tipoRegistro',
                   'moedaReferencia',
                   'indicadorCorrecaoFuturos',
                   'fatorCotacao',
                   'precosExercicioFuturosEmDolar',
                   'isin',
                   'numeroDistribuicaoPapel']

        try:
            df.drop(exclude, axis=1, inplace=True)
        except Exception as e:
            self.error(f'Error in exclude columns {exclude} -> {e}.')

        return df


def update_historical_quotes_yearly(verbose: bool = False,
                                    from_year: int = datetime.now().year,
                                    to_year: int = datetime.now().year):
    HistoricalQuotes(verbose=verbose).get_yearly_data(from_year=from_year,
                                                      to_year=to_year)


def update_historical_quotes_monthly(verbose: bool = False,
                                     from_month: datetime = datetime.now(),
                                     to_month: datetime = datetime.now()):
    HistoricalQuotes(verbose=verbose).get_monthly_data(from_month_year=from_month,
                                                       to_month_year=to_month)


def update_historical_quotes_daily(verbose: bool = False,
                                   from_date: datetime = datetime.now(),
                                   to_date: datetime = datetime.now()):
    HistoricalQuotes(verbose=verbose).get_daily_data(from_date=from_date,
                                                     to_date=to_date)


def get_yearly_history_quote(instruments: List[str] = None,
                             months: int = 0,
                             verbose: bool = False) -> pd.DataFrame:
    return HistoricalQuotes(verbose=verbose).get_yearly_history_quote(instruments=instruments, months=months)
