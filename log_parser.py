import re
import pandas as pd 

# Parses the TRF log files and converts them to a pandas dataframe

d = {"6": "AvgPx",
    "8": "BeginString",
    "9": "BodyLength",
    "10": "CheckSum",
    "14": "CumQty",
    "17": "ExecID",
    "20": "ExecTransType",
    "34": "MsgSeqNum",
    "35": "MsgType",
    "37": "OrderID",
    "39": "OrdStatus",
    "49": "SenderCompID",
    "50": "SenderSubID",
    "52": "SendingTime",
    "54": "Side",
    "55": "Symbol",
    "56": "TargetCompID",
    "57": "TargetSubID",
    "58": "Text",
    "60": "TransactTime",
    "65": "SymbolSfx",
    "75": "TradeDate",
    "98": "EncryptMethod",
    "107": "SecurityDesc",
    "108": "HeartBtInt",
    "128": "DeliverToCompID",
    "129": "DeliverToSubID",
    "150": "ExecType",
    "151": "LeavesQty",
    "275": "MDMkt",
    "277": "TradeCondition",
    "423": "PriceType",
    "440": "ClearingAccount",
    "375": "ContraBroker",
    '852':'PublishTrdIndicator',
    '9860':'ContraBranchSeqNbr',
    '855':'SecondaryTrdType',
    '9862':'ContraTradePA',
    '856':'TradeReportType',
    '9861':'BranchSeqNbr',
    '571':'TradeReportID',
    '22024':'ShortSaleIndicator',
    '577':'ClearingInstruction',
    '22033':'TradeModifier2Time',
    '9807':'RegFeeFlag',
    '880':'TrdMatchID ',
    '9826':'NonReportingGUID ',
    '939':'TrdRptStatus',
    '9863':'ContraClearingAcct',
    '5080':'AsOfIndicator',
    '829':'TrdSubType',
    '9854':'OverrideFlag'
        }

def parse_fix_message(message):
    # Split the message by the delimiter (ASCII 1)
    fields = message.split('\x01')
    # Create a dictionary to store the key-value pairs
    fix_dict = {}
    for field in fields:
        if '=' in field:
            key, value = field.split('=', 1)
            # Replace the key with the corresponding name from dictionary d
            
            key = d.get(key, key)
            fix_dict[key] = value
    return fix_dict

def parse_fix_log(file_path):
    with open(file_path, 'r') as file:
        log_data = file.read()
    
    # Use regex to find all FIX messages
    messages = re.findall(r'FIX\.\d\.\d:[^:]+->[^:]+: (8=.*?10=\d+)', log_data)
    
    parsed_messages = [parse_fix_message(message) for message in messages]
    return parsed_messages

def save_parsed_messages(parsed_messages, output_file_path):
    # Convert parsed messages to a DataFrame
    df = pd.DataFrame(parsed_messages)
    # Save DataFrame to an Excel file
    df.to_csv(output_file_path, index=False)

# Example usage
file_path = 'Data/2024-06-07 files/nasdaq_trf_log_2024-06-07.log'
parsed_messages = parse_fix_log(file_path)

# Save the parsed messages to an Excel file
output_file_path = 'parsed_messages_with_names3.csv'
save_parsed_messages(parsed_messages, output_file_path)

#485954
# parsed - 487867
# discrepancy comes from orders without a Side label - i.e. not buy or sell order

# results - 2102 missing from final sell
# 1185 missing from final buy
# 3287 missing in total
# 0.676% missing in total 