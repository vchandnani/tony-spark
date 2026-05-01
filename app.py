import pandas as pd
from faker import Faker
import random
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import datetime
import os
import s3fs

from dotenv import load_dotenv
load_dotenv();

fake = Faker()
s3 = s3fs.S3FileSystem()

# generate synthetic trades
def generate_trades(n=1000):
    data = []
    for _ in range(n):
        data.append({
            'trade_id': fake.uuid4(),
            'account_id': random.randint(100, 110),
            'symbol': random.choice(['AAPL', 'AMZN', 'BA','GOOG', 'NFLX']),
            'quantity': random.randint(1, 1000),
            'price': random.uniform(100, 1500),
            'date': (datetime.date.today() - datetime.timedelta(days=random.randint(0, 5))).isoformat()
        })
    return pd.DataFrame(data)

# write to S3 in partitioned Parquet
df = generate_trades()
table = pa.Table.from_pandas(df)

# set up S3 fileystem
fs = s3fs.S3FileSystem(
    anon=False,
    use_ssl=True,
    client_kwargs={
        "region_name": os.environ['S3_REGION'],
        "endpoint_url": os.environ['S3_ENDPOINT'],
        "aws_access_key_id": os.environ['S3_ACCESS_KEY'],
        "aws_secret_access_key": os.environ['S3_SECRET_KEY'],
        "verify": True,
    }
)

s3_filepath = 'my-data-lake-raw-026090555251-us-east-1-an/trades'

pq.write_to_dataset(
    table,
    s3_filepath,
    filesystem=fs,
    use_dictionary=True,
    compression="snappy",
    version="2.4",
)