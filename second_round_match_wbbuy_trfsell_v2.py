import pandas as pd
from collections import defaultdict
import copy 
import os
# Takes in nonmatching files after brute force program
# Matches the non matching values using price point and volume
# If a value in wb is found in the cumulative values of trf, append all the rows that make up the value
# vice versa for trf 

wb_sell_not_matching = pd.read_csv('First Round CSV Results WB BUY TRF SELL/wb_first_round_not_match_wb_buy_trf_sell.csv')
trf_sell_not_matching = pd.read_csv('First Round CSV Results WB BUY TRF SELL/trf_first_round_not_match_wb_buy_trf_sell.csv')

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
            

#NOTE trf and wb prices dict contain the correct nums
# e.g. trf_prices_dict['CDRG']['ZVSA'] prints out:
# defaultdict(<class 'list'>, {4.6: 200.0})
# original values: trf_prices_dict['CDRG']['ZVSA'] prints out:
# defaultdict(<class 'list'>, {4.6: [4.0, 35.0, 61.0, 100.0]})

# if trf_prices_dict_copy[broker][symbol][strikeprice] is in wb_prices_dict[broker][symbol][avgpx],
# append all rows in trf_prices_dict_copy[broker][symbol][strikeprice] to matching_trf
# else append all rows in trf_prices_dict_copy[broker][symbol][strikeprice] to not_matching_trf


matching_wb = []
matching_trf = []
not_matching_trf = []
not_matching_wb = []
num_rows_wb = wb_sell_not_matching.shape[0]
num_rows_trf = trf_sell_not_matching.shape[0]


# Pre-compute sets for faster lookups
seen_matching = set()
seen_not_matching = set()

# Use a dictionary for faster wb_prices_dict lookups
wb_prices_dict_flat = {(broker, symbol, avgpx): values 
                       for broker, symbols in wb_prices_dict.items()
                       for symbol, prices in symbols.items()
                       for avgpx, values in prices.items()}

# Iterate through trf_sell_not_matching once
for idx_trf, trf_row in trf_sell_not_matching.iterrows():
    broker = trf_row['ContraBroker']
    symbol = trf_row['Symbol']
    avgpx = float(trf_row['AvgPx'])
    quantity = trf_row['CumQty']
    
    key = (broker, symbol, avgpx)
    trf_individual_values = trf_prices_dict_copy[broker][symbol][avgpx]
    trf_cumulative_values = trf_prices_dict[broker][symbol][avgpx]

    if idx_trf in seen_matching:
        continue

    wb_values = wb_prices_dict_flat.get(key, [])
    if isinstance(wb_values, (int, float)):
        wb_values = [wb_values]


    if len(wb_values) != 0 and trf_cumulative_values in wb_values:
        #print(f'trf cum: {trf_cumulative_values}, \n trf ind: {trf_individual_values}, \n wb: {wb_values}')

        for count, value in enumerate(trf_individual_values):
            temp_idx = idx_trf + count
            if temp_idx >= len(trf_sell_not_matching):
                break
            temp_row = trf_sell_not_matching.iloc[temp_idx]

            matching_trf.append(temp_row)
            seen_matching.add(temp_idx)

    else:
        not_matching_trf.append(trf_row)
        seen_not_matching.add(idx_trf)

# Pre-compute sets for faster lookups
seen_matching = set()
seen_not_matching = set()

# Use a dictionary for faster trf_prices_dict lookups
trf_prices_dict_flat = {(broker, symbol, avgpx): values 
                        for broker, symbols in trf_prices_dict.items()
                        for symbol, prices in symbols.items()
                        for avgpx, values in prices.items()}
# Iterate through wb_sell_not_matching once
for idx_wb, wb_row in wb_sell_not_matching.iterrows():
    broker = wb_row['execbroker']
    symbol = wb_row['symbol']
    avgpx = float(wb_row['strikeprice'])
    quantity = wb_row['strikeqty']
    
    key = (broker, symbol, avgpx)
    wb_individual_values = wb_prices_dict_copy[broker][symbol][avgpx]
    wb_cumulative_values = wb_prices_dict[broker][symbol][avgpx]

    if idx_wb in seen_matching:
        continue

    trf_values = trf_prices_dict_flat.get(key, [])
    if isinstance(trf_values, (int, float)):
        trf_values = [trf_values]

    if len(trf_values) != 0 and wb_cumulative_values in trf_values:
        #print(f'trf cum: {trf_cumulative_values}, \n trf ind: {trf_individual_values}, \n wb: {wb_values}')

        for count, value in enumerate(wb_individual_values):
            temp_idx = idx_wb + count
            if temp_idx >= len(wb_sell_not_matching):
                break
            temp_row = wb_sell_not_matching.iloc[temp_idx]
            matching_wb.append(temp_row)
            seen_matching.add(temp_idx)

    else:
        not_matching_wb.append(wb_row)
        seen_not_matching.add(idx_wb)

# Make sure no rows were lost 
assert len(matching_wb) + len(not_matching_wb) == num_rows_wb
assert len(matching_trf) + len(not_matching_trf) == num_rows_trf 

# Convert lists to DataFrames
matching_wb_df = pd.DataFrame(matching_wb).drop_duplicates()
matching_trf_df = pd.DataFrame(matching_trf).drop_duplicates()
not_matching_wb_df = pd.DataFrame(not_matching_wb).drop_duplicates()
not_matching_trf_df = pd.DataFrame(not_matching_trf).drop_duplicates()

# Save the DataFrames to CSV files
output_dir = 'Second Round CSV Results WB BUY TRF SELL'
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
print(f'Second Round Matching Statistics')
print(f"Total WB rows: {total_wb_rows}")
print(f"Total TRF rows: {total_trf_rows}")
print(f"Matching WB rows: {matching_wb_rows}")
print(f"Matching TRF rows: {matching_trf_rows}")
print(f"WB Matching Percentage: {wb_matching_percentage:.2f}%")
print(f"TRF Matching Percentage: {trf_matching_percentage:.2f}%")