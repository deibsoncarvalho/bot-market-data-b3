import requests
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Request:

    def __init__(self):
        self._session = requests.Session()
        self._header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
           'referrer': 'http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/mercado-a-vista/opcoes/posicoes-em-aberto/', # noqa
           'Connection': 'keep-alive',
           'Accept-Encoding': 'gzip, deflate, br',
           'Accept-Language': 'pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3',
           'Host': 'arquivos.b3.com.br',
           'Cache-Control': 'max-age=0',
           'DNT': '1',
           'Sec-GPC': '1',
           'Upgrade-Insecure-Requests': '1',
           'Sec-Fetch-Site': 'none',
           'Sec-Fetch-Mode': 'navigate',
           'Sec-Fetch-Dest': 'document',
           'Sec-Fetch-User': '?1',
           #'Cookie': 'visid_incap_2246223=hibHxHcESNWyvO2Llqu7Tmb4RGEAAAAAQUIPAAAAAAD+4438MxFwr5iDR64BD4iT; ai_user=YquqO|2021-09-17T20:19:52.456Z; dtCookie=v_4_srv_27_sn_27628384CD846D25FAC0ECD80A33BF95_perc_100000_ol_0_mul_1_app-3Afd69ce40c52bd20e_0_rcs-3Acss_0; TS0171d45d=011d592ce1212081f6850e2ff9696a1111780ec14334997c5dd1f8f66ae90f46f0c66312753baa6a64939118bab8aa5258ee78b1c18f4b5f72be14e43de79a1c733a43055b465a8fdb451e862af8592aeb66aea6da11a4f14bf717c51a263c331a9583aee9f270bde1ba82083616f85a5a58ae20523cbc7027aba9a1165738ca805f3828da282e795df979845ce544378f883d5b8f2055da970ee57094169d8c88070ecc5ac4e005cd6e0599694e38d9eb5196e6f4; rxVisitor=16321644623343CNOKO6EJ6GR4S6HC063GVT3GNGFB3HU; dtPC=27$174263421_503h-vQUBUCGDDOVQRGDUSGGVLWBFBUUGJCAMG-0e0; rxvt=1632176065136|1632171670869; dtSa=-; dtLatC=3; __trf.src=encoded_eyJmaXJzdF9zZXNzaW9uIjp7InZhbHVlIjoiKG5vbmUpIiwiZXh0cmFfcGFyYW1zIjp7fX0sImN1cnJlbnRfc2Vzc2lvbiI6eyJ2YWx1ZSI6Iihub25lKSIsImV4dHJhX3BhcmFtcyI6e319LCJjcmVhdGVkX2F0IjoxNjMyMTc0MjY0MDAxfQ==; _sp_id.8319=3a6b01a2-aed6-59e8-a338-5249d89f1e97.1632164463.2.1632174264.1632164463.7f549bfe-3925-5af4-8ab9-225d44cd49eb; OptanonConsent=isGpcEnabled=1&datestamp=Mon+Sep+20+2021+18%3A44%3A24+GMT-0300+(Hor%C3%A1rio+Padr%C3%A3o+de+Bras%C3%ADlia)&version=6.21.0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0003%3A1%2CC0001%3A1%2CC0004%3A1%2CC0002%3A1&geolocation=%3B&AwaitingReconsent=false; OptanonAlertBoxClosed=2021-09-20T19:01:05.856Z; auth0=; nlbi_2246223=+EuDS1TG6mDESjkZ9OkOmwAAAACiMxdFPVFg6nmPrzTyJHtP; incap_ses_685_2246223=9uN2fs0LQUoPaIj7ZZyBCbX6SGEAAAAAeR7bo1tn/xd2dzkmuuu8Ng==; _sp_ses.8319=*; _sp_first_session.8319=', # noqa
           }
        self._header_download = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0',
                        'Accept': 'application/json, text/plain, */*',
                        'referrer': 'http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/mercado-a-vista/opcoes/posicoes-em-aberto/', # noqa
                        'Connection': 'keep-alive',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3',
                        'Host': 'arquivos.b3.com.br',
                        'Cache-Control': 'max-age=0',
                        'DNT': '1',
                        'Sec-GPC': '1',
                        'Request-Context': 'appId=cid-v1:35129064-154f-4002-b824-e9ee555dbb52',
                        'Sec-Fetch-Site': 'same-origin',
                        'Sec-Fetch-Mode': 'cors',
                        'Sec-Fetch-Dest': 'empty',
                        'Request-Id': '7JJGN.s2wzp',
                        #'Cookie': 'visid_incap_2246223=hibHxHcESNWyvO2Llqu7Tmb4RGEAAAAAQUIPAAAAAAD+4438MxFwr5iDR64BD4iT; ai_user=YquqO|2021-09-17T20:19:52.456Z; dtCookie=v_4_srv_27_sn_27628384CD846D25FAC0ECD80A33BF95_perc_100000_ol_0_mul_1_app-3Afd69ce40c52bd20e_0_rcs-3Acss_0; TS0171d45d=011d592ce1212081f6850e2ff9696a1111780ec14334997c5dd1f8f66ae90f46f0c66312753baa6a64939118bab8aa5258ee78b1c18f4b5f72be14e43de79a1c733a43055b465a8fdb451e862af8592aeb66aea6da11a4f14bf717c51a263c331a9583aee9f270bde1ba82083616f85a5a58ae20523cbc7027aba9a1165738ca805f3828da282e795df979845ce544378f883d5b8f2055da970ee57094169d8c88070ecc5ac4e005cd6e0599694e38d9eb5196e6f4; rxVisitor=16321644623343CNOKO6EJ6GR4S6HC063GVT3GNGFB3HU; dtPC=27$174263421_503h-vQUBUCGDDOVQRGDUSGGVLWBFBUUGJCAMG-0e0; rxvt=1632176065136|1632171670869; dtSa=-; dtLatC=3; __trf.src=encoded_eyJmaXJzdF9zZXNzaW9uIjp7InZhbHVlIjoiKG5vbmUpIiwiZXh0cmFfcGFyYW1zIjp7fX0sImN1cnJlbnRfc2Vzc2lvbiI6eyJ2YWx1ZSI6Iihub25lKSIsImV4dHJhX3BhcmFtcyI6e319LCJjcmVhdGVkX2F0IjoxNjMyMTc0MjY0MDAxfQ==; _sp_id.8319=3a6b01a2-aed6-59e8-a338-5249d89f1e97.1632164463.2.1632174264.1632164463.7f549bfe-3925-5af4-8ab9-225d44cd49eb; OptanonConsent=isGpcEnabled=1&datestamp=Mon+Sep+20+2021+18%3A44%3A24+GMT-0300+(Hor%C3%A1rio+Padr%C3%A3o+de+Bras%C3%ADlia)&version=6.21.0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0003%3A1%2CC0001%3A1%2CC0004%3A1%2CC0002%3A1&geolocation=%3B&AwaitingReconsent=false; OptanonAlertBoxClosed=2021-09-20T19:01:05.856Z; auth0=; nlbi_2246223=+EuDS1TG6mDESjkZ9OkOmwAAAACiMxdFPVFg6nmPrzTyJHtP; incap_ses_685_2246223=9uN2fs0LQUoPaIj7ZZyBCbX6SGEAAAAAeR7bo1tn/xd2dzkmuuu8Ng==; _sp_ses.8319=*; _sp_first_session.8319=', # noqa
                        }
        self._cookies = None
        self._get_cookies()

    def _get_cookies(self):
        req = self.request(
            "http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/boletim-diario/dados-publicos-de-produtos-listados-e-de-balcao/")  # noqa
        if req.status_code == 200:
            self._cookies = req.cookies

    def download(self, url: str):
        return self._session.get(url, headers=self._header_download, stream=True, cookies=self._cookies, verify=False)

    def request(self, url: str):
        return self._session.get(url, headers=self._header, cookies=self._cookies, verify=False)

    def post(self, url: str, data: dict):
        return self._session.post(url, headers=self._header, cookies=self._cookies, verify=False, data=data)
