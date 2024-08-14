import subprocess
import time 

# List of scripts to run sequentially
scripts = [
    'first_round_match_wbbuy_trfsell.py',
    'first_round_match_wbsell_trfbuy.py',
    'second_round_match_wbbuy_trfsell_v2.py',
    'second_round_match_wbsell_trfbuy_v2.py', 
    'third_round_match_v2_wbbuy_trfsell.py',
    'third_round_match_v2_wbsell_trfbuy.py',
    'fourth_round_match_wbbuy_trfsell.py',
    'fourth_round_match_wbsell_trfbuy.py'
]

for script in scripts:
    print(f"Running {script}...")
    result = subprocess.run(['python', script], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Output of {script}:\n{result.stdout}")

    else:
        print(f"Error running {script}: {result.stderr}")
        break
    time.sleep(5)