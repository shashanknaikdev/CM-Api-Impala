###############################################################################
# Authors: Shashank Naik & Eric Maynard                                       #
# Description: Config file for CM API Impala queries                          #
###############################################################################

import datetime

# Hostname of the Cloudera Manager server
cm_host = ""

# CM Port (7180 for unsecure clusters, 7183 for secure)
cm_port = "7180"

# Cloudera Manager username
cm_user = ""

# Cloudera Manager password
cm_password = ""

# Number of threads to use to get detailed query information from CM
detail_threads = 8

# Cluster Name
cluster_name = "Cluster 1"

# CM api version number, backwards compatible. Not recommended to increment.
version_number = 18

# Use SSL? 
ssl = False

# Get individual query details:
get_details = True

# Folder to output logs to
log_folder = "log"

# The end time (To) of the query period
#end_time = datetime.datetime.now()
end_time = datetime.datetime.strptime("2018-04-23T03:59:59.000Z",'%Y-%m-%dT%H:%M:%S.%fZ')

# The number of hours to go back from end time
delta_period = 2

# The Start Time (From) of the query period [Calculated based on end time and delta]
start_time = end_time + datetime.timedelta(hours=-delta_period)
#start_time = datetime.datetime.strptime("2018-03-28T23:59:59.000Z",'%Y-%m-%dT%H:%M:%S.%fZ')

# The base hdfs path where the json folders for each run are copied to (make sure to end in / )
hdfs_path = "/tmp/operational_metadata_impala/"

# The Database Name where the Queries table will be created
database_name = "operational_metadata"

# The Table Name where the Queries will be stored
table_name = "impala_queries"

# The batch size in hours
batch_size = 1

# Filter string for the tsquery:
filter_string = ""

# Spark config: The YARN queue for the Spark job
queue = "root.default"

# Spark config: The number of executors for the Spark job, adjust accordingly
num_executors = "4"

# Spark config: number of cores per executor:
executor_cores = 2

# Spark config: The driver memory
driver_memory = "4g"

# Spark config: The executor memory
executor_memory = "4g"
