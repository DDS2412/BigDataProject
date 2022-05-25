import os

import requests
import json
from airflow.models import Variable

SEC_HOST = 'https://www.sec.gov/Archives/'
path_to_save = '/opt/airflow/data/forms'


def _save_form(path, form_data):
    if not os.path.exists(path_to_save):
        os.makedirs(path_to_save)
    hash_name = str(hash(path))
    path = f'{path_to_save}/{hash_name}.txt'
    with open(path, 'w+') as file:
        file.write(form_data)


def _load_forms(forms_info):
    for path in forms_info:
        url = f'{SEC_HOST}edgar{path}'
        headers = {'user-agent': 'my-app/0.0.1'}
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            _save_form(path, r.text)


def _handle_index_file(index_data):
    lines = index_data.splitlines()
    forms = list(filter(lambda line: line.startswith('13F-HR'), lines))
    forms_paths = []
    failed_forms = []
    for form in forms:
        try:
            path = form.split("edgar", 1)[1].strip()
        except:
            path = ""
            failed_forms.append(form)

        forms_paths.append(path)
    print(f'Got info about {len(forms_paths)} forms')

    ## Handle failed forms
    print(f'Lost info about {len(failed_forms)} forms')
    _load_forms(forms_paths)


def get_forms():
    config = json.loads(Variable.get("get_forms").replace("'", '"'))
    from_year = int(config.get("from_year"))
    to_year = int(config.get("to_year"))

    print(f'Will load forms from {from_year} to {to_year}')

    indixes_host = f'{SEC_HOST}edgar/full-index/'
    quaters = config.get("quaters") ## ['QTR1', 'QTR2', 'QTR3', 'QTR4']

    for year in range(from_year, to_year + 1):
        print(f'Loading index files for {year} year')
        for quater in quaters:
            url = f'{indixes_host}{year}/{quater}/form.idx'
            print(f'Performing request to {url}')
            headers = {'user-agent': 'my-app/0.0.1'}
            r = requests.get(url, headers=headers)
            print(f'{r.status_code}')
            if r.status_code == 200:
                index_data = r.text
                _handle_index_file(index_data)
