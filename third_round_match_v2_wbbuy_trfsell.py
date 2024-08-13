import pandas as pd
from collections import defaultdict
import copy 
import os
# Takes in nonmatching files after price point match program 'v2' wb buy trf sell 
# Matches the non matching values using price point and volume
# wb contains the sums, trf contains individual orders
# keep running count of total and average price for trf, if running total and count == values in wb,
# append all the values 

#NOTE THERES AN ISSUE WITH MATCHING THE CORRECT PRICE TO ORDERS, RIGHT NOW THE CODE ADDS 
# THE QUANTITY WITHOUT CHECKING THE PRICE 

# Read in the dataframes 
wb_sell_not_matching = pd.read_csv('Second Round CSV Results WB BUY TRF SELL/wb_second_round_not_match_wb_buy_trf_sell.csv')
trf_sell_not_matching = pd.read_csv('Second Round CSV Results WB BUY TRF SELL/trf_second_round_not_match_wb_buy_trf_sell.csv')

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
            
#print(wb_prices_dict['CDRG']['ACON'], '\n')
#print(trf_prices_dict_copy['CDRG']['ACON'])


matching_wb = []
matching_trf = []
not_matching_trf = []
not_matching_wb = []
num_rows_wb = wb_sell_not_matching.shape[0]
num_rows_trf = trf_sell_not_matching.shape[0]
first_idx = 0 

temp_idx = 0 

trf_append_list = [] # keeps track of trf rows that need to be appended to trf match
# Iterate through wb_buy_trf_sell_not_matching_wb_merge_new_filtered to classify rows
past_broker, past_symbol = None, None
trf_qty_list = []
remaining_trf_qty_list = []

for idx_wb in range(num_rows_wb):
    wb_row = wb_sell_not_matching.iloc[idx_wb]
    broker = wb_row['execbroker']
    symbol = wb_row['symbol']
    avgpx = float(wb_row['strikeprice'])
    quantity = wb_row['strikeqty']
    #print(f'{broker}, {symbol}, {avgpx}, {quantity}, {past_broker}, {past_symbol}')
    
    if past_broker is None and past_symbol is None:
        past_broker = broker
        past_symbol = symbol
        trf_individual_values = trf_prices_dict_copy[broker][symbol]
        trf_qty_list = [(price, qty) for price, qtys in trf_individual_values.items() for qty in qtys]
        remaining_trf_qty_list = trf_qty_list.copy()
    elif past_broker != broker or past_symbol != symbol:
        past_broker = broker
        past_symbol = symbol
        trf_individual_values = trf_prices_dict_copy[broker][symbol]
        trf_qty_list = [(price, qty) for price, qtys in trf_individual_values.items() for qty in qtys]
        remaining_trf_qty_list = trf_qty_list.copy()

    # Iterate through remaining_trf_qty_list, if a sub combination adds up to quantity, add row to matching
    # and remove rows from remaining_trf_qty_list and continue     
    running_count = 0 
    temp_idx = 0
    temp_list = [] 
    
    wb_check = False
    while temp_idx < len(remaining_trf_qty_list):
        running_count += remaining_trf_qty_list[temp_idx][1]
        #price = remaining_trf_qty_list[temp_idx][0]
        temp_list.append((broker, symbol, remaining_trf_qty_list[temp_idx][0], remaining_trf_qty_list[temp_idx][1]))
        if running_count == quantity: #and price == avgpx
            matching_wb.append(wb_row)
            temp_idx += 1 
            trf_append_list.append(temp_list)
            #print(f'trf_append_list: {trf_append_list}')
            #print(f'Appended: {wb_row}')
            wb_check = True
            remaining_trf_qty_list = remaining_trf_qty_list[temp_idx:]
            break
        temp_idx += 1
    #print(f'remaining_trf_qty_list: {remaining_trf_qty_list}')
    if not wb_check:
        not_matching_wb.append(wb_row)
        
#print(trf_append_list)
# Flatten trf_append_list to make it easier to check for membership
flattened_trf_append_list = [item for sublist in trf_append_list for item in sublist]

# Iterate through trf_sell_not_matching and add rows to matching_trf if they are in flattened_trf_append_list
for idx_trf in range(num_rows_trf):
    trf_row = trf_sell_not_matching.iloc[idx_trf]
    broker = trf_row['ContraBroker']
    symbol = trf_row['Symbol']
    price = trf_row['AvgPx']
    quantity = trf_row['CumQty']
    
    if (broker, symbol, price, quantity) in flattened_trf_append_list:
        matching_trf.append(trf_row)
        flattened_trf_append_list.remove((broker, symbol, price, quantity))

    else:
        not_matching_trf.append(trf_row)

# Convert lists to DataFrames
matching_wb_df = pd.DataFrame(matching_wb).drop_duplicates()
matching_trf_df = pd.DataFrame(matching_trf).drop_duplicates()
not_matching_wb_df = pd.DataFrame(not_matching_wb).drop_duplicates()
not_matching_trf_df = pd.DataFrame(not_matching_trf).drop_duplicates()

# Save the DataFrames to CSV files
output_dir = 'Third Round CSV Results WB BUY TRF SELL'
os.makedirs(output_dir, exist_ok=True)

matching_wb_df.to_csv(os.path.join(output_dir, 'wb_third_round_match_wb_buy_trf_sell.csv'), index=False)
matching_trf_df.to_csv(os.path.join(output_dir, 'trf_third_round_match_wb_buy_trf_sell.csv'), index=False)
not_matching_wb_df.to_csv(os.path.join(output_dir, 'wb_third_round_not_match_wb_buy_trf_sell.csv'), index=False)
not_matching_trf_df.to_csv(os.path.join(output_dir, 'trf_third_round_not_match_wb_buy_trf_sell.csv'), index=False)


# Calculate statistics
total_wb_rows = wb_sell_not_matching.shape[0]
total_trf_rows = trf_sell_not_matching.shape[0]
matching_wb_rows = matching_wb_df.shape[0]
matching_trf_rows = matching_trf_df.shape[0]

wb_matching_percentage = (matching_wb_rows / total_wb_rows) * 100
trf_matching_percentage = (matching_trf_rows / total_trf_rows) * 100

# Print statistics
print(f'Third Round Matching Statistics')
print(f"Total WB rows: {total_wb_rows}")
print(f"Total TRF rows: {total_trf_rows}")
print(f"Matching WB rows: {matching_wb_rows}")
print(f"Matching TRF rows: {matching_trf_rows}")
print(f"WB Matching Percentage: {wb_matching_percentage:.2f}%")
print(f"TRF Matching Percentage: {trf_matching_percentage:.2f}%")