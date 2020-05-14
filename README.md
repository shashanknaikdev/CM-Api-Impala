# CM-Api-Impala
Pre-requisites: Install the CM api python client bindings on the node selected to run the scripts, details here: https://cloudera.github.io/cm_api/docs/python-client/

Steps:

1) Add configuration details to `impala_config.py`
2) Run json collection: `python impala_json.py` The json files will be automatically copied to the HDFS location specified in the config 
3) Run the table insert using Spark 2.x: `spark-submit impala_spark.py`. Optionally, make use of the spark tuning parameters in `impala_config.py`. The data will be inserted into the table specified into the config, or one will be created if it does not already exist.
4) The table created could have some duplicate queries (<1%), create a distinct view on top of the table to analyze only the distinct queries.


### get_details
`get_details` is an experimental feature that fetches details from the query profile from each query. It must be used in conjunction with a very small query collection window, and it can potentially place a very high degree of load on Impala coordinators. Not recommended for production use without extensive testing.

