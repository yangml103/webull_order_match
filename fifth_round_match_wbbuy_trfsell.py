import pandas as pd
from collections import defaultdict
import numpy as np
# combinationSum3 doesn't use recursion to try to address maximum recursion depth issue 

# Takes in nonmatching files after price point match program 'v4' wb buy trf sell 
# Matches the non matching values using price point and volume
# wb contains the sums, trf contains individual orders
# keep running count of total and average price for trf, if running total and count == values in wb,
# append all the values 
# Repeat of third round match 

#NOTE WORKS!!!!!!!!!!!!!! BUT TAKES LIKE 5 MINUTES TO RUN 

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
    
    count , max_count = 0, 10000 # prevent infinite loop, idk how infinite loops happen tbh 
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
wb_not_matching = pd.read_csv('wb_buy_trf_sell_not_matching_wb_merge_new_filtered_v4.csv', low_memory=False)
trf_not_matching = pd.read_csv('wb_buy_trf_sell_not_matching_trf_merge_new_filtered_v4.csv', low_memory=False)


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


trf_prices_dict_copy = trf_prices_dict

matching_wb = []
matching_trf = []
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
    #avgpx = float(wb_row['strikeprice'])
    quantity = wb_row['strikeqty']
    
    if prevBroker != broker or prevSymbol != symbol:
        seen = set() 
        prevBroker, prevSymbol = broker, symbol
    
    curr_wb_quantity = quantity
    trf_individual_values = trf_prices_dict_copy[broker][symbol]
    trf_qty_list = (qty for qtys in trf_individual_values.values() for qty in qtys)
    trf_qty_list = [qty for qty in trf_qty_list if qty not in seen] # remove the combinations that have already been seen 
    #print(f'trf_qty_list: {trf_qty_list}, \n, curr_wb_quantity: {curr_wb_quantity}, \n, seen: {seen}, \n, prevBroker: {prevBroker}, \n, prevSymbol: {prevSymbol}')
    #print(f'\n, {can_sum(trf_qty_list, curr_wb_quantity)}, \n, {combinationSum3(trf_qty_list, curr_wb_quantity)}')
    
    if can_sum(trf_qty_list, curr_wb_quantity):
        combinations = combinationSum3(trf_qty_list, curr_wb_quantity)
        #print(f'combinations: {combinations}, len: {len(combinations)}')
        
        if len(combinations) > 0:
            matching_wb.append(wb_row)
            curr = combinations[0]
            for combination in curr:
                trf_append_list.append([broker, symbol, combination])
                seen.add(combination)
    else:

        not_matching_wb.append(wb_row)
    


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
            trf_append_list.remove(val)
            found = True 
            break 
    if not found:
        not_matching_trf.append(trf_row)


# Convert lists to DataFrames
matching_wb_df = pd.DataFrame(matching_wb).drop_duplicates()
matching_trf_df = pd.DataFrame(matching_trf).drop_duplicates()
not_matching_wb_df = pd.DataFrame(not_matching_wb).drop_duplicates()
not_matching_trf_df = pd.DataFrame(not_matching_trf).drop_duplicates()

# Save the DataFrames to CSV files
matching_wb_df.to_csv('wb_buy_trf_sell_matching_wb_merge_new_filtered_v5.csv', index=False)
matching_trf_df.to_csv('wb_buy_trf_sell_matching_trf_merge_new_filtered_v5.csv', index=False)
not_matching_wb_df.to_csv('wb_buy_trf_sell_not_matching_wb_merge_new_filtered_v5.csv', index=False)
not_matching_trf_df.to_csv('wb_buy_trf_sell_not_matching_trf_merge_new_filtered_v5.csv', index=False)

# Calculate statistics
total_wb_rows = wb_not_matching.shape[0]
total_trf_rows = trf_not_matching.shape[0]
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