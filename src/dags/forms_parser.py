import csv
import json
import os
import random
import pandas as pd
import re
import time
from airflow.models import Variable

import requests
from bs4 import BeautifulSoup
from fake_headers import Headers

abbreviations = {
    'ag': 'aktiengesellschaft',
    'avrg': 'average',
    'bk': 'bank',
    'bkg': 'banking',
    'cap': 'capital',
    'chems': 'chemicals',
    'co': 'company',
    'constr': 'construction',
    'corp': 'corporation',
    'ctls': 'controls',
    'finl': 'financial',
    'fla': 'florida',
    'fltng': 'floating',
    'hldg': 'holding',
    'hldgs': 'holdings',
    'indl': 'industrial',
    'intl': 'international',
    'labs': 'laboratories',
    'machs': 'machines',
    'matls': 'materials',
    'mgmt': 'management',
    'natl': 'national',
    'pac': 'pacific',
    'pete': 'petroleum',
    'pptys': 'properties',
    'prods': 'products',
    'rlty': 'realty',
    'rte': 'rate',
    'ry': 'railway',
    'sr': 'senior',
    'svcs': 'services',
    'tr': 'trust'
}

base_url = 'https://query2.finance.yahoo.com/v1/finance/search?q='
header = Headers(headers=True)
active_proxy = {'HTTP': ' ', 'HTTPS': ''}
active_header = header.generate()
proxies = []
proxies_size = 0


def create_csv(filename):
    columns = (
        "CIK",
        "Name of fund",
        "Quater of report",
        "Signing date",
        "Name of Issuer",
        "Ticker of Issuer",
        "CUSIP",
        "Value (x$1000)",
        "Shares",
        "Shares type",
        "Investment Discretion",
        "Voting Sole",
        "Shared",
        "None"
    )
    with open(filename, 'w+') as csv_file:
        writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
        writer.writerow(columns)


def change_proxy():
    global proxies, proxies_size
    if proxies_size == 0:
        response = requests.get('https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/http.txt')
        proxies = response.text.split('\n')
        proxies_size = len(proxies)
    adress = proxies.pop(random.randint(0, proxies_size))
    active_proxy['HTTP'] = 'http://' + adress
    active_proxy['HTTPS'] = 'https://' + adress
    proxies_size -= 1


def ticker_by_name(name, name_to_ticker_mapping, is_existed_only_mapping):
    global active_header
    ticker = name_to_ticker_mapping.get(name)
    if ticker is not None:
        return ticker

    if is_existed_only_mapping:
        return 'None'

    url = base_url + "'" + name + "'"
    params = {'q': name, 'quotesCount': 1, 'newsCount': 0}

    time.sleep(random.randint(0, 4))
    try:
        r = requests.get(url, timeout=10, params=params, headers=active_header, proxies=active_proxy)
    except:
        return 'None'

    print(r.status_code)
    while r.status_code != 200:
        time.sleep(random.randint(1, 60))
        change_proxy()
        active_header = header.generate()
        r = requests.get(url, timeout=10, params=params, headers=active_header, proxies=active_proxy)
    try:
        ticker = json.loads(r.text)['quotes'][0]['symbol']
    except IndexError:
        ticker = 'None'
    name_to_ticker_mapping[name] = ticker
    print(name)
    print(ticker)

    return ticker


def replace_abbreviation(name):
    name = [abbreviations.get(word) if abbreviations.get(word) is not None else word for word in name.split()]
    name = ' '.join(name)
    return name


def add_to_csv(soup_xml, filename, mapping_filename, name_to_ticker_mapping, is_existed_only_mapping):
    try:
        quarter_of_report = soup_xml.body.find(re.compile('reportcalendarorquarter')).text
        signing_date = soup_xml.body.find(re.compile('signaturedate')).text
        cik = soup_xml.body.find(re.compile('cik')).text
        name_of_fund = soup_xml.body.find(re.compile('filingmanager')).find('name').text
        issuers = soup_xml.body.find_all(re.compile('nameofissuer'))
        cusips = soup_xml.body.find_all(re.compile('cusip'))
        values = soup_xml.body.find_all(re.compile('value'))
        sshprnamts = soup_xml.body.find_all(re.compile('sshprnamt'))[::2]
        sshprnamttypes = soup_xml.body.find_all(re.compile('sshprnamttype'))
        investmentdiscretions = soup_xml.body.find_all(re.compile('investmentdiscretion'))
        soles = soup_xml.body.find_all(re.compile('sole'))
        shareds = soup_xml.body.find_all(re.compile('shared'))
        nones = soup_xml.body.find_all(re.compile('none'))

        with open(filename, 'a') as f:
            w = csv.writer(f)
            for issuer, cusip, value, \
                sshprnamt, sshprnamttype, investmentdiscretion, \
                sole, shared, none in zip(issuers,
                                          cusips,
                                          values,
                                          sshprnamts,
                                          sshprnamttypes,
                                          investmentdiscretions,
                                          soles,
                                          shareds,
                                          nones):

                row = (
                    cik,
                    name_of_fund,
                    quarter_of_report,
                    signing_date,
                    replace_abbreviation(issuer.text),
                    ticker_by_name(replace_abbreviation(issuer.text.lower()), name_to_ticker_mapping, is_existed_only_mapping),
                    cusip.text,
                    value.text,
                    sshprnamt.text,
                    sshprnamttype.text,
                    investmentdiscretion.text,
                    sole.text,
                    shared.text,
                    none.text
                )
                if row[5] != 'None':
                    w.writerow(row)

        if is_existed_only_mapping:
            return

        mapping_df = pd.DataFrame.from_dict(name_to_ticker_mapping.items())
        mapping_df.columns = ["Name", "Tiker"]
        mapping_df.to_csv(mapping_filename, index=False)
    except:
        return


def parse_with_mapping():
    config = json.loads(Variable.get("forms_parser").replace("'", '"'))
    result_file = config.get("file_name")
    mapping_file = config.get("mapping")
    is_existed_only_mapping = bool(config.get("is_existed_only_mapping") == "true")
    data_path = Variable.get("data_path")

    print(f'Parsing forms from directory {data_path + "forms"}')
    if not os.path.exists(data_path + result_file):
        create_csv(data_path + result_file)

    name_to_ticker_mapping = {}

    if os.path.isfile(data_path + mapping_file):
        name_to_ticker_mapping = pd.read_csv(
            data_path + mapping_file, header=None, index_col=0, squeeze=True
        )[1:].to_dict()
        name_to_ticker_mapping = {v: k for k, v in name_to_ticker_mapping.items()}

    for form in os.listdir(data_path + "forms"):
        print(f'parse {form} file')
        file = open(data_path + f'forms/{form}')
        soup_xml = BeautifulSoup(file, "lxml")
        add_to_csv(soup_xml, data_path + result_file, data_path + mapping_file, name_to_ticker_mapping, is_existed_only_mapping)
