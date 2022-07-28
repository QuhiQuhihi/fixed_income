import os
import requests
import numpy as np
import pandas as pd
import pickle

### for detailed holdings data ###
# url = 'https://www.ishares.com/us/products/239452/ishares-13-year-treasury-bond-etf/1467271812596.ajax?fileType=csv&fileName=SHY_holdings&dataType=fund'

### for early holdings data ###
# url = 'https://www.ishares.com/us/literature/holdings/ishibdp-etf-early-holdings.csv'

### for cash flow data ###
# url = 'https://www.ishares.com/us/literature/cashflows/ishibdp-etf-cash-flows.csv'



class bond_etf_data_daily:

    def __init__(self, today ):

        self.today = today

        self.BASE_DIR = os.getcwd()
        self.DATA_DIR = os.path.join(self.BASE_DIR, 'data')
        self.RAW_DATA_DIR = os.path.join(self.DATA_DIR, 'raw_data')
        self.CLN_DATA_DIR = os.path.join(self.DATA_DIR, 'clean_data')

        if not os.path.exists(os.path.join(self.RAW_DATA_DIR,today)):
            os.makedirs(os.path.join(self.RAW_DATA_DIR,today))
            self.RAW_DATA_DIR = os.path.join(self.RAW_DATA_DIR, today)
        else:
            self.RAW_DATA_DIR = os.path.join(self.RAW_DATA_DIR, today)

        if not os.path.exists(os.path.join(self.CLN_DATA_DIR,today)):
            os.makedirs(os.path.join(self.CLN_DATA_DIR,today))
            self.CLN_DATA_DIR = os.path.join(self.CLN_DATA_DIR, today)
        else:
            self.CLN_DATA_DIR = os.path.join(self.CLN_DATA_DIR, today)


        self.ishares_links = pd.read_excel(
            os.path.join(self.DATA_DIR, 'ishares_product_screener.xlsx'),
            index_col='Ticker',
            usecols='A:B,G:K'
            )

        self.ishares_tickers = self.ishares_links.index.tolist()

        print("ishares_tickers are : ")
        print(self.ishares_tickers)

        print("ishares columns are : ")
        print(self.ishares_links.columns.tolist())


        # operation done in bond_etf_data_daily --> ishares_daily_operation
        # self.download_data_from_ishares(today, RAW_DATA_DIR)
        # self.clean_raw_data_from_ishares(today, RAW_DATA_DIR, CLN_DATA_DIR)

    def download_data_from_ishares(self, today, save_dir):

        print("today is {}".format(today))

        for ticker in self.ishares_tickers:
            print("  ")
            print("Ishares bond etf ticker is : ", ticker)

            # for url_file_name in ['cashflow_link', 'detail_holdings_link']:
            for url_file_name in ['cashflow_link', 'early_holdings_link', 'detail_holdings_link']:

                print("url name is : ", url_file_name)

                url = self.ishares_links.loc[ticker, url_file_name]
                file_name = '{0}_{1}_{2}.csv'.format(today, ticker, url_file_name)
                self.download_excel_from_web(url, save_dir, file_name)

            print("-------- {0} completed ---------".format(ticker))


    def clean_raw_data_from_ishares(self, today, save_dir_raw, save_dir_clean):
        error_file_list = []
        complete_file_list = []
        for ticker in self.ishares_tickers:

            print("IShares bond ticker is : ", ticker)

            # for url_file_name in ['cashflow_link', 'detail_holdings_link']:
            for url_file_name in ['cashflow_link', 'early_holdings_link', 'detail_holdings_link']:

                print(url_file_name, end=" ")

                try:
                    file_name = today + '_' + ticker + '_' + url_file_name + '.csv'

                    if url_file_name == 'cashflow_link':
                        original = pd.read_csv(
                            os.path.join(save_dir_raw, file_name)
                        )
                    elif url_file_name == 'early_holdings_link':
                        original = pd.read_csv(
                            os.path.join(save_dir_raw, file_name),
                            sep='|'
                        )
                    elif url_file_name == 'detail_holdings_link':
                        original = pd.read_csv(
                            os.path.join(save_dir_raw, file_name)
                            , skiprows=[1, 2, 3, 4, 5, 6, 7, 8, 9]
                        ).iloc[:-2, :]
                    complete_file_list.append(file_name)

                except:
                    error_file_list.append(file_name)

                # if file is well downloaded from IShares websites, clean version will be downloaded in clean dir
                original.to_csv(os.path.join(save_dir_clean, file_name))

        print("      ")
        print("error file list are below : ")
        print(error_file_list)

        pd.Series(error_file_list).to_csv(os.path.join(save_dir_clean, '0000_error_file_list.csv'))

        return error_file_list


    def download_excel_from_web(self, url, save_dir, save_file_name):

        r = requests.get(url, allow_redirects=True)
        open(os.path.join(save_dir, save_file_name), 'wb').write(r.content)
        # return_file = pd.read_csv(save_dir)
        return

    def ishares_daily_operation(self):

        self.download_data_from_ishares(self.today, self.RAW_DATA_DIR)
        self.clean_raw_data_from_ishares(self.today, self.RAW_DATA_DIR, self.CLN_DATA_DIR)


if __name__=='__main__':
    today = '2022-01-03'
    print("@2222222222")













