import sys
import pandas as pd

print("helo pipeline; args: ", sys.argv)

# pipelines usually use parameters: e.g. which month to process
month = int(sys.argv[1])
print(f"Running pipeline for month {month}")

df = pd.DataFrame({"day": [1, 2], "passengers": [3, 4]})
df["month"] = month
print(df.head())

# parquet is binary file optimized for data 
# (will not run bcs I dont have pyarrow and fastparquet --> docker bcs i dont want it in my local machine, for demenstration purposes)
df.to_parquet(f"output_{month}.parquet")