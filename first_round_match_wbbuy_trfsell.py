import pandas as pd
from collections import defaultdict
import copy 
import os
# New version of brute force program, intended to be used before
# price_point_match program 
# Takes in filtered df's from remove_and_replace program
#NOTE: V8 had issues with matching, this file attempts to fix issues

wb_buy_original = pd.read_csv('wb_buy_orders_filtered.csv')
trf_sell_original = pd.read_csv('trf_original_sell_filtered.csv')


# Sort the dataframes
wb_buy_original = wb_buy_original.sort_values(by=['execbroker', 'symbol', 'strikeprice'])
trf_sell_original = trf_sell_original.sort_values(by=['ContraBroker', 'Symbol', 'AvgPx'])


# Extract the symbols from both DataFrames
wb_symbols = wb_buy_original['symbol'].tolist()
trf_symbols = trf_sell_original['Symbol'].tolist()

wb_symbols = set(wb_symbols)
trf_symbols = set(trf_symbols)

# Find the Brokers that are common between both df 
common_brokers = set(wb_buy_original['execbroker'].unique()) & set(trf_sell_original['ContraBroker'].unique())
common_brokers = list(common_brokers)
common_brokers.sort()

# Initialize a Counter to count occurrences of each common symbol
wb_symbol_dict = dict()
trf_symbol_dict = dict()

for val in common_brokers:
    wb_symbol_dict[val] = dict()
    trf_symbol_dict[val] = dict()


idx_wb = 0 
num_rows_wb = wb_buy_original.shape[0]
idx_trf = 0
num_rows_trf = trf_sell_original.shape[0]

# Initialize dictionaries to count occurrences and store prices and quantities for each common symbol
wb_prices_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
trf_prices_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

# Iterate through wb_buy_original and accumulate prices and quantities
while idx_wb < num_rows_wb:
    wb_row = wb_buy_original.iloc[idx_wb]
    if wb_row['execbroker'] in common_brokers:
        wb_prices_dict[wb_row['execbroker']][wb_row['symbol']][wb_row['strikeprice']].append(wb_row['strikeqty'])  # Append quantity to list
    idx_wb += 1
#print(wb_prices_dict['CDRG']['ZVSA'])

# Iterate through trf_sell_original and accumulate prices and quantities
while idx_trf < num_rows_trf:
    trf_row = trf_sell_original.iloc[idx_trf]
    if trf_row['ContraBroker'] in common_brokers:
        trf_prices_dict[trf_row['ContraBroker']][trf_row['Symbol']][trf_row['AvgPx']].append(trf_row['CumQty'])  # Append quantity to list
    idx_trf += 1

# Create deep copies of the dictionaries
wb_prices_dict_copy = copy.deepcopy(wb_prices_dict)
trf_prices_dict_copy = copy.deepcopy(trf_prices_dict)



#NOTE trf and wb prices dict contain the correct nums
# e.g. trf_prices_dict['CDRG']['ZVSA'] prints out:
# defaultdict(<class 'list'>, {4.6: 200.0})
# original values: trf_prices_dict['CDRG']['ZVSA'] prints out:
# defaultdict(<class 'list'>, {4.6: [4.0, 35.0, 61.0, 100.0]})

matching_wb = []
matching_trf = []
not_matching_trf = []
not_matching_wb = []
num_rows_wb = wb_buy_original.shape[0]
num_rows_trf = trf_sell_original.shape[0]

# Iterate through trf, if quantity in wb, append to matching_trf
# else append to not_matching_trf, set wb value to 0
for idx_trf in range(num_rows_trf):
    trf_row = trf_sell_original.iloc[idx_trf]
    broker = trf_row['ContraBroker']
    symbol = trf_row['Symbol']
    avgpx = float(trf_row['AvgPx'])
    quantity = trf_row['CumQty']
    wb_individual_values = wb_prices_dict_copy[broker][symbol][avgpx] # stores the individual quantities
    
    if wb_individual_values is not None and quantity in wb_individual_values:
        # if symbol == 'AAAU':
        #     print(f'{symbol}, {avgpx}, {quantity}, {wb_individual_values}, appended: {quantity}')
        matching_trf.append(trf_row)
        wb_individual_values.remove(quantity)  # Remove the matched quantity
        if not wb_individual_values:  # If the list is empty, set it to None
            wb_prices_dict_copy[broker][symbol][avgpx] = None   
    else:
        not_matching_trf.append(trf_row)
    
print('\n')
for idx_wb in range(num_rows_wb):
    wb_row = wb_buy_original.iloc[idx_wb]
    broker = wb_row['execbroker']
    symbol = wb_row['symbol']
    avgpx = float(wb_row['strikeprice'])
    quantity = wb_row['strikeqty']
    trf_individual_values = trf_prices_dict_copy[broker][symbol][avgpx] # stores the individual quantities

    if trf_individual_values is not None and quantity in trf_individual_values:
        # if symbol == 'AAAU':
        #     print(f'{symbol}, {avgpx}, {quantity}, {trf_individual_values}, appended: {quantity}')
        matching_wb.append(wb_row)
        trf_individual_values.remove(quantity)  # Remove the matched quantity
        if not trf_individual_values:  # If the list is empty, set it to None
            trf_prices_dict_copy[broker][symbol][avgpx] = None
    else:
        not_matching_wb.append(wb_row)

# Convert lists to DataFrames
matching_wb_df = pd.DataFrame(matching_wb)
matching_trf_df = pd.DataFrame(matching_trf)
not_matching_wb_df = pd.DataFrame(not_matching_wb)
not_matching_trf_df = pd.DataFrame(not_matching_trf)

# Save the DataFrames to CSV files
output_dir = 'First Round CSV Results WB BUY TRF SELL'
os.makedirs(output_dir, exist_ok=True)
matching_wb_df.to_csv(os.path.join(output_dir, 'wb_first_round_match_wb_buy_trf_sell.csv'), index=False)
matching_trf_df.to_csv(os.path.join(output_dir, 'trf_first_round_match_wb_buy_trf_sell.csv'), index=False)
not_matching_wb_df.to_csv(os.path.join(output_dir, 'wb_first_round_not_match_wb_buy_trf_sell.csv'), index=False)
not_matching_trf_df.to_csv(os.path.join(output_dir, 'trf_first_round_not_match_wb_buy_trf_sell.csv'), index=False)

# Calculate statistics
total_wb_rows = wb_buy_original.shape[0]
total_trf_rows = trf_sell_original.shape[0]
matching_wb_rows = matching_wb_df.shape[0]
matching_trf_rows = matching_trf_df.shape[0]

wb_matching_percentage = (matching_wb_rows / total_wb_rows) * 100
trf_matching_percentage = (matching_trf_rows / total_trf_rows) * 100

# Print statistics
print(f'First Round Matching Statistics')
print(f"Total WB rows: {total_wb_rows}")
print(f"Total TRF rows: {total_trf_rows}")
print(f"Matching WB rows: {matching_wb_rows}")
print(f"Matching TRF rows: {matching_trf_rows}")
print(f"WB Matching Percentage: {wb_matching_percentage:.2f}%")
print(f"TRF Matching Percentage: {trf_matching_percentage:.2f}%")