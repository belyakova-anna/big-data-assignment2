## app folder
This folder contains the data folder and all scripts and source code that are required to run your simple search engine. 

### data
This folder stores plain-text documents to index (`<doc_id>_<doc_title>.txt`). They are produced by `prepare_data.py` from the Kaggle parquet sample (typically up to 1000 files), or you can add files manually for testing.

### a.parquet (optional input)
Place the Kaggle Wikipedia parquet file here as `a.parquet` before running `prepare_data.sh` (see root `README.md`). If missing, `prepare_data.py` falls back to existing `.txt` files under `data/`.

### mapreduce
This folder stores the mapper `mapperx.py` and reducer `reducerx.py` scripts for the MapReduce pipelines.

### app.py
This is a Python file to write code to store index data in Cassandra.

### app.sh
The entrypoint for the executables in your repository and includes all commands that will run your programs in this folder.

### create_index.sh
A script to create index data using MapReduce pipelines and store them in HDFS.

### index.sh
A script to run the MapReduce pipelines and the programs to store data in Cassandra/ScyllaDB.

### prepare_data.py
The script that will create documents from parquet file. You can run it in the driver.

### prepare_data.sh
Runs `prepare_data.py` and copies generated documents to HDFS (`/data`, `/input/data`). Uploads `a.parquet` to HDFS when present locally.

### incremental_index.py
Python script used by `add_to_index.sh`: updates Cassandra tables (`postings`, `vocabulary`, `document_stats`, `corpus_stats`) for a single new or replaced document without rebuilding MapReduce outputs under `/indexer`.

### add_to_index.sh
Optional workflow: takes a local `<doc_id>_<doc_title>.txt`, updates HDFS `/data` and merged `/input/data`, then calls `incremental_index.py` to refresh the Cassandra index in place.

### query.py
A Python file to write PySpark app that will process a user's query and retrieves a list of top 10 relevant documents ranked using BM25.

### requirements.txt
This file contains all Python depenedencies that are needed for running the programs in this repository. This file is read by pip when installing the dependencies in `app.sh` script.

### search.sh
This script will be responsible for running the `query.py` PySpark app on Hadoop YARN cluster.


### start-services.sh
This script will initiate the services required to run Hadoop components. This script is called in `app.sh` file.


### store_index.sh
This script will create Cassandra/ScyllaDB tables and load the index data from HDFS to them.
