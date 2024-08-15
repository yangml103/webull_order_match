import pandas as pd
from collections import defaultdict
import copy 
import os

#NOTE This version attempts to implement the changes suggested by Grayman - get TOTAL QTY 
# of each broker, symbol, and calculate the total price - price*qty, if wb price and trf price
# are within $5 of each other, then we consider it a match 

# Read in the dataframes 
wb_sell_not_matching = pd.read_csv('First Round CSV Results WB SELL TRF BUY/wb_first_round_not_match_wb_sell_trf_buy.csv', low_memory=False)
trf_sell_not_matching = pd.read_csv('First Round CSV Results WB SELL TRF BUY/trf_first_round_not_match_wb_sell_trf_buy.csv', low_memory=False)

# Sort the dataframes
wb_sell_not_matching = wb_sell_not_matching.sort_values(by=['execbroker', 'symbol', 'strikeprice'])
trf_sell_not_matching = trf_sell_not_matching.sort_values(by=['ContraBroker', 'Symbol', 'AvgPx'])


# Extract the symbols from both DataFrames
wb_symbols = wb_sell_not_matching['symbol'].tolist()
trf_symbols = trf_sell_not_matching['Symbol'].tolist()

wb_symbols = set(wb_symbols)
trf_symbols = set(trf_symbols)

# Find the Brokers that are common between both df 
common_brokers = set(wb_sell_not_matching['execbroker'].unique()) & set(trf_sell_not_matching['ContraBroker'].unique())
common_brokers = list(common_brokers)
common_brokers.sort()

# Initialize a Counter to count occurrences of each common symbol
wb_symbol_dict = dict()
trf_symbol_dict = dict()

for val in common_brokers:
    wb_symbol_dict[val] = dict()
    trf_symbol_dict[val] = dict()


idx_wb = 0 
num_rows_wb = wb_sell_not_matching.shape[0]
idx_trf = 0
num_rows_trf = trf_sell_not_matching.shape[0]

# Initialize dictionaries to count occurrences and store prices and quantities for each common symbol
wb_prices_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
trf_prices_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

# Iterate through wb_sell_not_matching and accumulate prices and quantities
while idx_wb < num_rows_wb:
    wb_row = wb_sell_not_matching.iloc[idx_wb]
    if wb_row['execbroker'] in common_brokers:
        wb_prices_dict[wb_row['execbroker']][wb_row['symbol']][wb_row['strikeprice']].append(wb_row['strikeqty'])  # Append quantity to list
    idx_wb += 1
#print(wb_prices_dict['CDRG']['ZVSA'])

# Iterate through trf_sell_not_matching and accumulate prices and quantities
while idx_trf < num_rows_trf:
    trf_row = trf_sell_not_matching.iloc[idx_trf]
    if trf_row['ContraBroker'] in common_brokers:
        trf_prices_dict[trf_row['ContraBroker']][trf_row['Symbol']][trf_row['AvgPx']].append(trf_row['CumQty'])  # Append quantity to list
    idx_trf += 1

# Create deep copies of the dictionaries
wb_prices_dict_copy = copy.deepcopy(wb_prices_dict)
trf_prices_dict_copy = copy.deepcopy(trf_prices_dict)
#print(trf_prices_dict['CDRG']['ZVSA'])

# To get cumulative sums of quantities, you can use the following:
for broker in wb_prices_dict:
    for symbol in wb_prices_dict[broker]:
        for strikeprice in wb_prices_dict[broker][symbol]:
            wb_prices_dict[broker][symbol][strikeprice] = sum(wb_prices_dict[broker][symbol][strikeprice])

for broker in trf_prices_dict:
    for symbol in trf_prices_dict[broker]:
        for avgpx in trf_prices_dict[broker][symbol]:
            trf_prices_dict[broker][symbol][avgpx] = sum(trf_prices_dict[broker][symbol][avgpx])
            
#print(wb_prices_dict['CDRG']['AEMD'][0.53], '\n')
#print(wb_prices_dict_copy['CDRG']['AEMD'])

wb_price_times_cum_qty = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))  
trf_price_times_cum_qty = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

# Calculate the price times cumulative quantity for each broker and symbol 
for broker in wb_prices_dict:
    for symbol in wb_prices_dict[broker]:
        temp = []
        for strikeprice in wb_prices_dict[broker][symbol]:
            temp.append(wb_prices_dict[broker][symbol][strikeprice] * strikeprice) 
        wb_price_times_cum_qty[broker][symbol] = int(sum(temp)) # SAVE AS INT TO REDUCE MEMORY CONSUMPTION
#print(wb_price_times_cum_qty['CDRG']['AEMD'])

# Calculate the price times cumulative quantity for each broker and symbol 
for broker in trf_prices_dict:
    for symbol in trf_prices_dict[broker]:
        temp = []
        for avgpx in trf_prices_dict[broker][symbol]:
            temp.append(trf_prices_dict[broker][symbol][avgpx] * avgpx) 
        trf_price_times_cum_qty[broker][symbol] = int(sum(temp)) # SAVE AS INT TO REDUCE MEMORY CONSUMPTION

matching_wb = []
matching_trf = []
not_matching_trf = []
not_matching_wb = []
num_rows_wb = wb_sell_not_matching.shape[0]
num_rows_trf = trf_sell_not_matching.shape[0]

wb_append_set = set()
trf_append_set = set()

maximum_difference = 0 # maximum acceptable difference between wb and trf price 

for idx_wb in range(num_rows_wb):
    wb_row = wb_sell_not_matching.iloc[idx_wb]
    broker = wb_row['execbroker']
    symbol = wb_row['symbol']
    avgpx = float(wb_row['strikeprice'])
    quantity = wb_row['strikeqty']
    #print(f'{broker}, {symbol}, {avgpx}, {quantity}, {past_broker}, {past_symbol}')
    wb_price_times_cum_qty_val = wb_price_times_cum_qty[broker][symbol]
    trf_price_times_cum_qty_val = trf_price_times_cum_qty[broker][symbol]
    
    
    if (broker, symbol) not in wb_append_set and isinstance(wb_price_times_cum_qty_val, int) and isinstance(trf_price_times_cum_qty_val, int) and abs(wb_price_times_cum_qty_val - trf_price_times_cum_qty_val) <= maximum_difference:
        wb_append_set.add((broker, symbol))
        trf_append_set.add((broker, symbol))

idx_wb = 0
idx_trf = 0

while idx_wb < num_rows_wb:
    wb_row = wb_sell_not_matching.iloc[idx_wb]
    broker = wb_row['execbroker']
    symbol = wb_row['symbol']
    if (broker, symbol) in wb_append_set:
        matching_wb.append(wb_row)
    else:
        not_matching_wb.append(wb_row)
    idx_wb += 1


while idx_trf < num_rows_trf:
    trf_row = trf_sell_not_matching.iloc[idx_trf]
    broker = trf_row['ContraBroker']
    symbol = trf_row['Symbol']
    if (broker, symbol) in trf_append_set:
        matching_trf.append(trf_row)
    else:
        not_matching_trf.append(trf_row)
    idx_trf += 1


# Convert lists to DataFrames
matching_wb_df = pd.DataFrame(matching_wb).drop_duplicates()
matching_trf_df = pd.DataFrame(matching_trf).drop_duplicates()
not_matching_wb_df = pd.DataFrame(not_matching_wb).drop_duplicates()
not_matching_trf_df = pd.DataFrame(not_matching_trf).drop_duplicates()

# Make sure no rows were lost 
print(f'Matching WB rows: {matching_wb_df.shape[0]}, Not matching WB rows: {not_matching_wb_df.shape[0]}, Sum = {matching_wb_df.shape[0] + not_matching_wb_df.shape[0]}, Total WB rows: {num_rows_wb}')
print(f'Matching TRF rows: {matching_trf_df.shape[0]}, Not matching TRF rows: {not_matching_trf_df.shape[0]}, Sum = {matching_trf_df.shape[0] + not_matching_trf_df.shape[0]}, Total TRF rows: {num_rows_trf}')


# Save the DataFrames to CSV files
output_dir = 'Second Round CSV Results WB BUY TRF SELL - TEST'
os.makedirs(output_dir, exist_ok=True)

matching_wb_df.to_csv(os.path.join(output_dir, 'wb_second_round_match_wb_buy_trf_sell.csv'), index=False)
matching_trf_df.to_csv(os.path.join(output_dir, 'trf_second_round_match_wb_buy_trf_sell.csv'), index=False)
not_matching_wb_df.to_csv(os.path.join(output_dir, 'wb_second_round_not_match_wb_buy_trf_sell.csv'), index=False)
not_matching_trf_df.to_csv(os.path.join(output_dir, 'trf_second_round_not_match_wb_buy_trf_sell.csv'), index=False)


# Calculate statistics
total_wb_rows = wb_sell_not_matching.shape[0]
total_trf_rows = trf_sell_not_matching.shape[0]
matching_wb_rows = matching_wb_df.shape[0]
matching_trf_rows = matching_trf_df.shape[0]

wb_matching_percentage = (matching_wb_rows / total_wb_rows) * 100
trf_matching_percentage = (matching_trf_rows / total_trf_rows) * 100

# Print statistics
print(f'Second Round Matching Statistics - WB BUY TRF SELL')
print(f"Total WB rows: {total_wb_rows}")
print(f"Total TRF rows: {total_trf_rows}")
print(f"Matching WB rows: {matching_wb_rows}")
print(f"Matching TRF rows: {matching_trf_rows}")
print(f"WB Matching Percentage: {wb_matching_percentage:.2f}%")
print(f"TRF Matching Percentage: {trf_matching_percentage:.2f}%")