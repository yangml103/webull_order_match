import pandas as pd

# Sorts the parsed messages by buy/sell

# Read the Excel file
df = pd.read_csv('parsed_messages_with_names.csv', low_memory=False)

# Read another CSV file and remove its first row
df_additional = pd.read_csv('NYSE TRF_WBUL_WEBL_Contra Firm_07JUN2024.csv', low_memory=False)
df_additional = df_additional.iloc[1:]

# Concatenate the additional DataFrame to the original DataFrame
df = pd.concat([df, df_additional], ignore_index=True)

# Sort the DataFrame by buy/sell. Side - 1 is buy, Side - 2 is sell
df_buy = df[df['Side'] == 1]
df_sell = df[df['Side'] == 2]

# Save the sorted DataFrame back to an Excel file
df_buy.to_csv('trf_original_buy.csv', index=False)
df_sell.to_csv('trf_original_sell.csv', index=False)