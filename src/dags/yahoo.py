import yfinance as yf
import pandas as pd
import datetime
import json


from airflow.models import Variable


def add_ticker(ticket, first_date, last_date, total_hist_df):
    hist_df = yf.download(ticket, first_date, last_date).reset_index()
    hist_df['Date'] = hist_df['Date'].astype(str)
    hist_df['Ticker'] = ticket
    total_hist_df = pd.concat([hist_df, total_hist_df])

    return total_hist_df


def get_data_from_yahoo():
    config = json.loads(Variable.get("yahoo_downloader").replace("'", '"'))

    parsed_forms_filename = config.get("parsed_forms_filename")
    result_filename = config.get("result_filename")
    data_path = Variable.get("data_path")

    report_df = pd.read_csv(data_path + parsed_forms_filename, parse_dates=[3, 4])
    report_df = report_df[['Quater of report', 'Signing date', 'Name of Issuer', 'Ticker of Issuer']]

    report_df['Quater of report'] = pd.to_datetime(report_df['Quater of report'])

    first_date = report_df.groupby('Ticker of Issuer')['Quater of report'] \
        .first() \
        .reset_index()

    last_date = report_df.groupby('Ticker of Issuer')['Signing date'] \
        .last() \
        .reset_index() \
        .rename(columns={'Signing date': 'Last date'})

    last_date['Last date'] = pd.to_datetime(last_date['Last date'])

    merged_df = pd.merge(first_date, last_date)

    merged_df['Quater of report'] = pd.PeriodIndex(merged_df['Quater of report'], freq='Q').to_timestamp()

    total_hist = pd.DataFrame()

    for ticker, q_of_report in zip(merged_df['Ticker of Issuer'], merged_df['Quater of report']):
        total_hist = add_ticker(ticker, q_of_report, datetime.date.today(), total_hist)

    total_hist.to_csv(data_path + result_filename, index=False)

