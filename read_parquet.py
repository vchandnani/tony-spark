import pyarrow.parquet as pq
import pandas as pd

# Access metadata only
parquet_file = pq.ParquetFile('sample.parquet')
schema = parquet_file.schema
print(schema)

df = pd.read_parquet('sample.parquet')
print(df.dtypes)