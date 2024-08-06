import pandas as pd 

# Program to remove futures and options from wb side, replaces HDSN broker to HRTF
# new files are wb_buy_orders_test_filtered.csv, wb_sell_orders_test_filtered.csv, trf_original_sell_filtered.csv, trf_original_buy_filtered.csv
# Read in the dataframes
wb_buy_orders_test = pd.read_csv('wb_buy_orders.csv')
wb_sell_orders_test = pd.read_csv('wb_sell_orders.csv')
trf_original_sell = pd.read_csv('trf_original_sell.csv')
trf_original_buy = pd.read_csv('trf_original_buy.csv')

# Filter out rows where tickertype is 'option' or 'future'
wb_buy_orders_test = wb_buy_orders_test[~wb_buy_orders_test['tickertype'].isin(['OPTION', 'FUTURES'])]
wb_sell_orders_test = wb_sell_orders_test[~wb_sell_orders_test['tickertype'].isin(['OPTION', 'FUTURES'])]
# Replace 'hdsn' with 'HRTF' in the 'execbroker' column
wb_buy_orders_test['execbroker'] = wb_buy_orders_test['execbroker'].replace('HDSN', 'HRTF')
wb_sell_orders_test['execbroker'] = wb_sell_orders_test['execbroker'].replace('HDSN', 'HRTF')

# Replace 'HDSN' with 'HRTF' in the 'ContraBroker' column
trf_original_sell['ContraBroker'] = trf_original_sell['ContraBroker'].replace('HDSN', 'HRTF')
trf_original_buy['ContraBroker'] = trf_original_buy['ContraBroker'].replace('HDSN', 'HRTF')

wb_buy_orders_test.to_csv('wb_buy_orders_filtered.csv', index=False)
wb_sell_orders_test.to_csv('wb_sell_orders_filtered.csv', index=False)
trf_original_sell.to_csv('trf_original_sell_filtered.csv', index=False)
trf_original_buy.to_csv('trf_original_buy_filtered.csv', index=False)
