import pandas as pd
import os

folder_path = "init_data_copy"

for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)
    df = pd.read_csv(file_path)
    df = df.drop(df.columns[0], axis=1)
    df.to_csv(file_path, index=False)