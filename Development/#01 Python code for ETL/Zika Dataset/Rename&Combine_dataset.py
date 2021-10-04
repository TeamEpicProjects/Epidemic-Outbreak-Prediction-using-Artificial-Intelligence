
try:
    import pandas as pd
    from glob import glob
    import os
    print("All modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

# Rename all csv file name
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
#combine all files in the list
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
#export to csv
combined_csv.to_csv( "combined_csv.csv", index=False, encoding='utf-8-sig')

# combine all files in one csv file
stock_files = sorted(glob(f'./All_dataset/*.csv'))
data = pd.concat((pd.read_csv(file, encoding='cp1252')
                      for file in stock_files), ignore_index=True)
data.to_csv("./All_dataset/dataset/zikadataset.csv",index=False)
print("Success...")




