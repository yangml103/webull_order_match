import pandas as pd
from collections import defaultdict
import copy 
import numpy as np
import pandas as pd
import os 


# This file is intended to be used to contain all the rounds of program code
# for wb buy and trf sell

#NOTE DOESNT WORK YET 

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
print('\n')


#NOTE ROUND 2 

# Takes in nonmatching files after brute force program
# Matches the non matching values using price point and volume
# If a value in wb is found in the cumulative values of trf, append all the rows that make up the value
# vice versa for trf 

wb_sell_not_matching = not_matching_wb_df
trf_sell_not_matching = not_matching_trf_df



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

        
# Convert lists to DataFrames
matching_wb_df = pd.DataFrame(matching_wb).drop_duplicates()
matching_trf_df = pd.DataFrame(matching_trf).drop_duplicates()
not_matching_wb_df = pd.DataFrame(not_matching_wb).drop_duplicates()
not_matching_trf_df = pd.DataFrame(not_matching_trf).drop_duplicates()

# Calculate statistics

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
print('\n')


#NOTE ROUND 3 

# Takes in nonmatching files after price point match program 'v2' wb buy trf sell 
# Matches the non matching values using price point and volume
# wb contains the sums, trf contains individual orders
# keep running count of total and average price for trf, if running total and count == values in wb,
# append all the values 


# Read in the dataframes 
wb_sell_not_matching = not_matching_wb_df
trf_sell_not_matching = not_matching_trf_df

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
        temp_list.append((broker, symbol, remaining_trf_qty_list[temp_idx][0], remaining_trf_qty_list[temp_idx][1]))
        if running_count == quantity: 
            matching_wb.append(wb_row)
            temp_idx += 1 
            trf_append_list.append(temp_list)

            wb_check = True
            remaining_trf_qty_list = remaining_trf_qty_list[temp_idx:]
            break
        temp_idx += 1
    if not wb_check:
        not_matching_wb.append(wb_row)
        
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

# Calculate statistics

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
print('\n')


#NOTE ROUND 4  

# Takes in nonmatching files afterthird round match program 'v3' wb buy trf sell 
# Matches the non matching values using cumulative volume
# wb contains the sums, trf contains individual orders
# If a value in wb is found in the cumulative values of trf, append all the rows that make up the value
# vice versa for trf 

# Read in the dataframes 
wb_sell_not_matching = not_matching_wb_df
trf_sell_not_matching = not_matching_trf_df

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
            
            
# Sum up the values of all AvgPx for each broker and symbol in trf_prices_dict
trf_avgpx_sums = defaultdict(lambda: defaultdict(float))

for broker in trf_prices_dict:
    for symbol in trf_prices_dict[broker]:
        for avgpx in trf_prices_dict[broker][symbol]:
            trf_avgpx_sums[broker][symbol] += trf_prices_dict[broker][symbol][avgpx]
            
# Sum up the values of all AvgPx for each broker and symbol in wb_prices_dict
wb_avgpx_sums = defaultdict(lambda: defaultdict(float))

for broker in wb_prices_dict:
    for symbol in wb_prices_dict[broker]:
        for avgpx in wb_prices_dict[broker][symbol]:
            wb_avgpx_sums[broker][symbol] += wb_prices_dict[broker][symbol][avgpx]
                       
not_matching_trf = []
not_matching_wb = []
num_rows_wb = wb_sell_not_matching.shape[0]
num_rows_trf = trf_sell_not_matching.shape[0]

idx_wb, idx_trf = 0,0
# Iterate through wb_buy_trf_sell_not_matching_wb_merge_new_filtered to classify rows
while idx_wb < num_rows_wb:
    wb_row = wb_sell_not_matching.iloc[idx_wb]
    broker = wb_row['execbroker']
    symbol = wb_row['symbol']
    avgpx = float(wb_row['strikeprice'])
    quantity = wb_row['strikeqty']

    trf_avgpx_sum = trf_avgpx_sums[broker][symbol]
    wb_avgpx_sum = wb_avgpx_sums[broker][symbol]
    
       
    if trf_avgpx_sum == wb_avgpx_sum and trf_avgpx_sum != 0:
        

        num_orders_wb = sum(len(lst) for lst in wb_prices_dict_copy[broker][symbol].values())

        # Append all values in not matching trf and not matching wb 
        for val in range(idx_wb, idx_wb+num_orders_wb,1):
            wb_row = wb_sell_not_matching.iloc[val]
            matching_wb.append(wb_row)
        idx_wb += num_orders_wb
            
    else:
        num_orders_wb = sum(len(lst) for lst in wb_prices_dict_copy[broker][symbol].values())
        
        # Edge case to prevent infinite loop if num_orders_wb == 0 
        if num_orders_wb == 0:
            idx_wb += 1
            continue
        for val in range(idx_wb, idx_wb+num_orders_wb):
            wb_row = wb_sell_not_matching.iloc[val]
            not_matching_wb.append(wb_row)
        idx_wb += num_orders_wb
        
        
while idx_trf < num_rows_trf:
    trf_row = trf_sell_not_matching.iloc[idx_trf]
    broker = trf_row['ContraBroker']
    symbol = trf_row['Symbol']
    avgpx = float(trf_row['AvgPx'])
    quantity = trf_row['CumQty']
    
    trf_avgpx_sum = trf_avgpx_sums[broker][symbol]
    wb_avgpx_sum = wb_avgpx_sums[broker][symbol]
    
    if trf_avgpx_sum == wb_avgpx_sum and wb_avgpx_sum != 0:
        num_orders_trf = sum(len(lst) for lst in trf_prices_dict_copy[broker][symbol].values())
        # Append all values in not matching trf and not matching wb 
        
        for val in range(idx_trf, idx_trf+num_orders_trf):
            trf_row = trf_sell_not_matching.iloc[val]
            matching_trf.append(trf_row)
        idx_trf += num_orders_trf
    else:
        num_orders_trf = sum(len(lst) for lst in trf_prices_dict_copy[broker][symbol].values())
        
        if num_orders_trf == 0:
            idx_trf += 1
            continue
        for val in range(idx_trf, idx_trf+num_orders_trf):
            trf_row = trf_sell_not_matching.iloc[val]
            not_matching_trf.append(trf_row)
        idx_trf += num_orders_trf


# Convert lists to DataFrames
matching_wb_df = pd.DataFrame(matching_wb).drop_duplicates()
matching_trf_df = pd.DataFrame(matching_trf).drop_duplicates()
not_matching_wb_df = pd.DataFrame(not_matching_wb).drop_duplicates()
not_matching_trf_df = pd.DataFrame(not_matching_trf).drop_duplicates()

# Calculate statistics

matching_wb_rows = matching_wb_df.shape[0]
matching_trf_rows = matching_trf_df.shape[0]

wb_matching_percentage = (matching_wb_rows / total_wb_rows) * 100
trf_matching_percentage = (matching_trf_rows / total_trf_rows) * 100

# Print statistics
print(f'Fourth Round Matching Statistics')
print(f"Total WB rows: {total_wb_rows}")
print(f"Total TRF rows: {total_trf_rows}")
print(f"Matching WB rows: {matching_wb_rows}")
print(f"Matching TRF rows: {matching_trf_rows}")
print(f"WB Matching Percentage: {wb_matching_percentage:.2f}%")
print(f"TRF Matching Percentage: {trf_matching_percentage:.2f}%")
print('\n')


#NOTE ROUND 5

# Takes in nonmatching files after price point match program 'v4' wb buy trf sell 
# If a list of quantities in TRF can sum up to a value in wb, append the combinations 
# to the matching list, and the related order in WB. 

#NOTE THE COMBINATIONS ARE RANDOM AND MIGHT MISS SOME COMBINATIONS SINCE IT IS CAPPED AT 10,000 COMBOS 

#NOTE TAKES LIKE 5 MINUTES TO RUN 

def can_sum(nums, target):
    possible_sums = set([0])

    for num in nums:
        if not isinstance(num, (int, float)) or not isinstance(num, np.float64):  # Ensure num is not a numpy float64
            continue
        new_sums = set()
        for s in possible_sums:
            new_sum = s + num
            if new_sum == target:
                return True
            if new_sum < target:
                new_sums.add(new_sum)
        possible_sums.update(new_sums)
    
    return target in possible_sums

def combinationSum3(candidates, target):
    # Stack
    candidates.sort()
    result = []
    stack = [(0, 0, [])]  # (current_sum, start_index, current_path)
    found = False
    
    count , max_count = 0, 10000 # prevent infinite loop, idk how infinite loops happen tbh maybe issue with the continue in line 760
    while stack:
        current_sum, start, path = stack.pop()

        if current_sum == target:
            result.append(path)
            found = True
            return result 
        count += 1 
        for i in range(start, len(candidates)):
            if found:
                return result
            if count > max_count:
                return [] 
            if i > start and candidates[i] == candidates[i - 1]:
                continue

            new_sum = current_sum + candidates[i]
            if new_sum > target:
                break

            stack.append((new_sum, i + 1, path + [candidates[i]]))

    return result



# Read in the dataframes with specified dtypes
wb_not_matching =  not_matching_wb_df
trf_not_matching = not_matching_trf_df


# Sort the dataframes
wb_not_matching = wb_not_matching.sort_values(by=['execbroker', 'symbol', 'strikeprice'])
trf_not_matching = trf_not_matching.sort_values(by=['ContraBroker', 'Symbol', 'AvgPx'])

# Extract the symbols from both DataFrames
wb_symbols = wb_not_matching['symbol'].tolist()
trf_symbols = trf_not_matching['Symbol'].tolist()

wb_symbols = set(wb_symbols)
trf_symbols = set(trf_symbols)

# Find the Brokers that are common between both df 
common_brokers = set(wb_not_matching['execbroker'].unique()) & set(trf_not_matching['ContraBroker'].unique())
common_brokers = list(common_brokers)
common_brokers.sort()

# Initialize a Counter to count occurrences of each common symbol
wb_symbol_dict = dict()
trf_symbol_dict = dict()

for val in common_brokers:
    wb_symbol_dict[val] = dict()
    trf_symbol_dict[val] = dict()

idx_wb = 0 
num_rows_wb = wb_not_matching.shape[0]
idx_trf = 0
num_rows_trf = trf_not_matching.shape[0]

# Initialize dictionaries to count occurrences and store prices and quantities for each common symbol
wb_prices_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
trf_prices_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

# Iterate through wb_not_matching and accumulate prices and quantities
while idx_wb < num_rows_wb:
    wb_row = wb_not_matching.iloc[idx_wb]
    if wb_row['execbroker'] in common_brokers:
        wb_prices_dict[wb_row['execbroker']][wb_row['symbol']][wb_row['strikeprice']].append(wb_row['strikeqty'])  # Append quantity to list
    idx_wb += 1

# Iterate through trf_not_matching and accumulate prices and quantities
while idx_trf < num_rows_trf:
    trf_row = trf_not_matching.iloc[idx_trf]
    if trf_row['ContraBroker'] in common_brokers:
        trf_prices_dict[trf_row['ContraBroker']][trf_row['Symbol']][trf_row['AvgPx']].append(trf_row['CumQty'])  # Append quantity to list
    idx_trf += 1


trf_prices_dict_copy = trf_prices_dict # hack for the moment, could be cleaner 

not_matching_trf = []
not_matching_wb = []
num_rows_wb = wb_not_matching.shape[0]
num_rows_trf = trf_not_matching.shape[0]


trf_append_list = [] # keeps track of trf rows that need to be appended to trf match

# Iterate through wb_buy_trf_not_matching_wb_merge_new_filtered to classify rows
trf_qty_list = []
seen = set()
prevBroker, prevSymbol = None, None

# WB CONTAINS THE SUMS 
for idx_wb in range(num_rows_wb):
    wb_row = wb_not_matching.iloc[idx_wb]
    broker = wb_row['execbroker']
    symbol = wb_row['symbol']
    quantity = wb_row['strikeqty']
    
    if prevBroker != broker or prevSymbol != symbol:
        seen = set() 
        prevBroker, prevSymbol = broker, symbol
    
    curr_wb_quantity = quantity
    trf_individual_values = trf_prices_dict_copy[broker][symbol]
    trf_qty_list = (qty for qtys in trf_individual_values.values() for qty in qtys)
    trf_qty_list = [qty for qty in trf_qty_list if qty not in seen] # remove the combinations that have already been seen 
    
    if can_sum(trf_qty_list, curr_wb_quantity): # check if it is possible for the values in the list to sum up to the current wb value 
        combinations = combinationSum3(trf_qty_list, curr_wb_quantity) # get a combination that sum up to the current wb value 
        #print(f'combinations: {combinations}, len: {len(combinations)}')
        
        if len(combinations) > 0:
            matching_wb.append(wb_row)
            curr = combinations[0]
            for combination in curr:
                trf_append_list.append([broker, symbol, combination]) # keep track of the combinations from TRF to append later
                seen.add(combination) # keep track of the combinations that have already been seen to prevent duplicates 
    else:

        not_matching_wb.append(wb_row)
    

# Append the combinations from TRF to the matching list 
for idx_trf1 in range(num_rows_trf):
    trf_row = trf_not_matching.iloc[idx_trf1]
    broker = trf_row['ContraBroker']
    symbol = trf_row['Symbol']
    quantity = trf_row['CumQty']
    found = False 
    for val in trf_append_list:
        br, sy, qu = val 
        if br == broker and sy == symbol and qu == quantity:
            matching_trf.append(trf_row)
            trf_append_list.remove(val) # remove the combination from the list so it doesn't get appended to the matching list again 
            found = True 
            break 
    if not found:
        not_matching_trf.append(trf_row)


# Convert lists to DataFrames
matching_wb_df = pd.DataFrame(matching_wb).drop_duplicates()
matching_trf_df = pd.DataFrame(matching_trf).drop_duplicates()
not_matching_wb_df = pd.DataFrame(not_matching_wb).drop_duplicates()
not_matching_trf_df = pd.DataFrame(not_matching_trf).drop_duplicates()


# Save the DataFrames to a folder as CSV files
output_dir = 'Cumulative CSV Results WB BUY TRF SELL'
os.makedirs(output_dir, exist_ok=True)
matching_wb_df.to_csv(os.path.join(output_dir, 'wb_buy_trf_sell_matching_wb_CUMULATIVE.csv'), index=False)
matching_trf_df.to_csv(os.path.join(output_dir, 'wb_buy_trf_sell_matching_trf_CUMULATIVE.csv'), index=False)
not_matching_wb_df.to_csv(os.path.join(output_dir, 'wb_buy_trf_sell_not_matching_wb_CUMULATIVE.csv'), index=False)
not_matching_trf_df.to_csv(os.path.join(output_dir, 'wb_buy_trf_sell_not_matching_trf_CUMULATIVE.csv'), index=False)

# Calculate statistics

matching_wb_rows = matching_wb_df.shape[0]
matching_trf_rows = matching_trf_df.shape[0]
not_matching_wb_rows = not_matching_wb_df.shape[0]
not_matching_trf_rows = not_matching_trf_df.shape[0]

wb_matching_percentage = (matching_wb_rows / total_wb_rows) * 100
trf_matching_percentage = (matching_trf_rows / total_trf_rows) * 100

# Print statistics
print(f'Fifth Round Matching Statistics')
print(f"Total WB rows: {total_wb_rows}")
print(f"Total TRF rows: {total_trf_rows}")
print(f"Matching WB rows: {matching_wb_rows}")
print(f"Matching TRF rows: {matching_trf_rows}")
print(f"WB Matching Percentage: {wb_matching_percentage:.2f}%")
print(f"TRF Matching Percentage: {trf_matching_percentage:.2f}%")
print(f'Remaining WB rows: {not_matching_wb_rows}')
print(f'Remaining TRF rows: {not_matching_trf_rows}')
print('\n')
