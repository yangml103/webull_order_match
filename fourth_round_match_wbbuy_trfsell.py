import pandas as pd
from collections import defaultdict
import copy 

# Takes in nonmatching files afterthird round match program 'v3' wb buy trf sell 
# Matches the non matching values using cumulative volume
# wb contains the sums, trf contains individual orders
# If a value in wb is found in the cumulative values of trf, append all the rows that make up the value
# vice versa for trf 

# Read in the dataframes 
wb_sell_not_matching = pd.read_csv('wb_buy_trf_sell_not_matching_wb_merge_new_filtered_v3.csv')
trf_sell_not_matching = pd.read_csv('wb_buy_trf_sell_not_matching_trf_merge_new_filtered_v3.csv')

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
                       
            
# Print the summed AvgPx values for verification
# print(trf_avgpx_sums['CDRG']['AEI'], '\n')
# print(trf_prices_dict['CDRG']['AEI'], '\n')
# print(trf_prices_dict_copy['CDRG']['AEI'])
# print(len(trf_prices_dict['CDRG']['AEI']))
# print(len(trf_prices_dict_copy['CDRG']['AEI']))
# # Calculate the number of values
# num_values = sum(len(lst) for lst in trf_prices_dict_copy['CDRG']['AEI'].values())

# print(num_values)  # Output: 8


matching_wb = []
matching_trf = []
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

# Save the DataFrames to CSV files
matching_wb_df.to_csv('wb_buy_trf_sell_matching_wb_merge_new_filtered_v4.csv', index=False)
matching_trf_df.to_csv('wb_buy_trf_sell_matching_trf_merge_new_filtered_v4.csv', index=False)
not_matching_wb_df.to_csv('wb_buy_trf_sell_not_matching_wb_merge_new_filtered_v4.csv', index=False)
not_matching_trf_df.to_csv('wb_buy_trf_sell_not_matching_trf_merge_new_filtered_v4.csv', index=False)

# Calculate statistics
total_wb_rows = wb_sell_not_matching.shape[0]
total_trf_rows = trf_sell_not_matching.shape[0]
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