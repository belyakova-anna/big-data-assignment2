# big-data-assignment2

# How to run
## Step 1: Install prerequisites
- Docker
- Docker compose
## Step 2: Download the dataset file

Download `a.parquet` from Kaggle:

- [Wikipedia 20230701 (a.parquet)](https://www.kaggle.com/datasets/jjinho/wikipedia-20230701?select=a.parquet)

Place the file at:

- `app/a.parquet`

## Step 3: Run the command
```bash
docker compose up 
```
This will create 3 containers, a master node and a worker node for Hadoop, and Cassandra server. The master node will run the script `app/app.sh` as an entrypoint.
