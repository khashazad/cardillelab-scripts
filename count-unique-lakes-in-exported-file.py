import pandas as pd
import os

data = []
file_path = os.path.abspath("E:/landsat8-50cc.csv")

df = pd.read_csv(file_path)
data.extend(df["hylak_id"].tolist())

# Fetch unique values with Set
data_unique = list(set(data))

print(f"{len(data_unique)} lakes")
# data_unique_string = map(str, data_unique)
# new_list = open("outputfile.csv", "w")
# new_list.write("\n".join(data_unique_string))
# new_list.close()
