import pandas as pd
from collections import defaultdict
import copy 
import os


# Altered version of first round one for one match
# If quantity is found for a broker and symbol, it is matched together

wb_not_matching = pd.read_csv('Fifth Round CSV Results WB BUY TRF SELL/wb_fifth_round_not_match_wb_buy_trf_sell.csv', low_memory=False)
trf_not_matching = pd.read_csv('Fifth Round CSV Results WB BUY TRF SELL/trf_fifth_round_not_match_wb_buy_trf_sell.csv', low_memory=False)


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

# Create deep copies of the dictionaries
wb_prices_dict_copy = copy.deepcopy(wb_prices_dict)
trf_prices_dict_copy = copy.deepcopy(trf_prices_dict)

# Get all quantities for each broker and symbol for each avgpx
all_quantities_wb = defaultdict(lambda: defaultdict(list))

for broker, symbols in wb_prices_dict_copy.items():
    for symbol, prices in symbols.items():
        for avgpx, quantities in prices.items():
            if quantities is not None:
                all_quantities_wb[broker][symbol].extend(quantities)
                
all_quantities_trf = defaultdict(lambda: defaultdict(list))

for broker, symbols in trf_prices_dict_copy.items():
    for symbol, prices in symbols.items():
        for avgpx, quantities in prices.items():
            if quantities is not None:
                all_quantities_trf[broker][symbol].extend(quantities)

#NOTE trf and wb prices dict contain the correct nums
# e.g. trf_prices_dict['CDRG']['ZVSA'] prints out:
# defaultdict(<class 'list'>, {4.6: 200.0})
# original values: trf_prices_dict['CDRG']['ZVSA'] prints out:
# defaultdict(<class 'list'>, {4.6: [4.0, 35.0, 61.0, 100.0]})

matching_wb = []
matching_trf = []
not_matching_trf = []
not_matching_wb = []
num_rows_wb = wb_not_matching.shape[0]
num_rows_trf = trf_not_matching.shape[0]

# Iterate through trf, if quantity in wb, append to matching_trf
# else append to not_matching_trf, set wb value to 0
for idx_trf in range(num_rows_trf):
    trf_row = trf_not_matching.iloc[idx_trf]
    broker = trf_row['ContraBroker']
    symbol = trf_row['Symbol']
    avgpx = float(trf_row['AvgPx'])
    quantity = trf_row['CumQty']
    all_wb_qty = all_quantities_wb[broker][symbol]
    
    if quantity in all_wb_qty:
        # if symbol == 'AAAU':
        #     print(f'{symbol}, {avgpx}, {quantity}, {wb_individual_values}, appended: {quantity}')
        matching_trf.append(trf_row)
        all_wb_qty.remove(quantity)  # Remove the matched quantity
 
    else:
        not_matching_trf.append(trf_row)

for idx_wb in range(num_rows_wb):
    wb_row = wb_not_matching.iloc[idx_wb]
    broker = wb_row['execbroker']
    symbol = wb_row['symbol']
    avgpx = float(wb_row['strikeprice'])
    quantity = wb_row['strikeqty']

    all_trf_qty = all_quantities_trf[broker][symbol]
    if quantity in all_trf_qty:
        # if symbol == 'AAAU':
        #     print(f'{symbol}, {avgpx}, {quantity}, {trf_individual_values}, appended: {quantity}')
        matching_wb.append(wb_row)
        all_trf_qty.remove(quantity)  # Remove the matched quantity

    else:
        not_matching_wb.append(wb_row)


# Convert lists to DataFrames
matching_wb_df = pd.DataFrame(matching_wb)
matching_trf_df = pd.DataFrame(matching_trf)
not_matching_wb_df = pd.DataFrame(not_matching_wb)
not_matching_trf_df = pd.DataFrame(not_matching_trf)

# Make sure no rows were lost 
print(f'Matching WB rows: {matching_wb_df.shape[0]}, Not matching WB rows: {not_matching_wb_df.shape[0]}, Sum = {matching_wb_df.shape[0] + not_matching_wb_df.shape[0]}, Total WB rows: {num_rows_wb}')
print(f'Matching TRF rows: {matching_trf_df.shape[0]}, Not matching TRF rows: {not_matching_trf_df.shape[0]}, Sum = {matching_trf_df.shape[0] + not_matching_trf_df.shape[0]}, Total TRF rows: {num_rows_trf}')


# Save the DataFrames to CSV files  
output_dir = 'Sixth Round CSV Results WB BUY TRF SELL'
os.makedirs(output_dir, exist_ok=True)
matching_wb_df.to_csv(os.path.join(output_dir, 'wb_sixth_round_match_wb_buy_trf_sell.csv'), index=False)
matching_trf_df.to_csv(os.path.join(output_dir, 'trf_sixth_round_match_wb_buy_trf_sell.csv'), index=False)
not_matching_wb_df.to_csv(os.path.join(output_dir, 'wb_sixth_round_not_match_wb_buy_trf_sell.csv'), index=False)
not_matching_trf_df.to_csv(os.path.join(output_dir, 'trf_sixth_round_not_match_wb_buy_trf_sell.csv'), index=False)

# Calculate statistics
total_wb_rows = wb_not_matching.shape[0]
total_trf_rows = trf_not_matching.shape[0]
matching_wb_rows = matching_wb_df.shape[0]
matching_trf_rows = matching_trf_df.shape[0]

wb_matching_percentage = (matching_wb_rows / total_wb_rows) * 100
trf_matching_percentage = (matching_trf_rows / total_trf_rows) * 100

# Print statistics
print(f'Sixth Round Matching Statistics')
print(f"Total WB rows: {total_wb_rows}")
print(f"Total TRF rows: {total_trf_rows}")
print(f"Matching WB rows: {matching_wb_rows}")
print(f"Matching TRF rows: {matching_trf_rows}")
print(f"WB Matching Percentage: {wb_matching_percentage:.2f}%")
print(f"TRF Matching Percentage: {trf_matching_percentage:.2f}%")