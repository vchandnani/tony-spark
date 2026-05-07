# Tony Spark

Technology Playground: Python, PySpark, S3, and Parquet Files.

## High-Level Design

1. Install prerequisites.
2. Configure AWS.
3. Clone the repository.
4. Generate synthetic trades data using Faker in Python, structure it as Pandas DataFrames, and write to S3 in partitioned Parquet format using pyarrow.
5. Process the raw data using PySpark by creating a SparkSession connected to S3, apply transformations, and save the results to S3. 

## Prerequisites

Required:
- `Python` (validated with Python `3.14.0`)
- `Java` (validated with Java `17`)

Recommended:
- `git` latest stable

## AWS Configuration

1. Root User
https://docs.aws.amazon.com/IAM/latest/UserGuide/id_root-user.html
2. IAM User
https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html
3. S3 Buckets
https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html

## Clone Repository

```bash
git clone https://github.com/vchandnani/tony-spark.git
cd tony-spark
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install pyspark faker pandas pyarrow boto3 s3fs
brew install openjdk@17
```

## Project Structure
```text
.
├── create_raw_data.py
├── read_parquet.py
├── README.md
├── sample.parquet
└── transform_raw_data.py
```

## 1. Create Raw Data

```bash
$ python create_raw_data.py
```

## 2. Transform Raw Data

```bash
$ python transform_raw_data.py
```