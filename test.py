import  pandas as pd

file="/mnt/c/users/tarun.kumar/downloads/SALT Dental Directory 2.15.xlsx"

df = pd.read_excel(file, dtype=str,sheet_name="PRACTICE", header=0).fillna('')
pd.set_option('display.max_columns', None)
print(df)