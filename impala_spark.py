# Authors: Shashank Naik & Eric Maynard

# ********************************DESCRIPTION************************************
# This script is part of a 2 step process to gather details on the impala queries
# This is step 2 which is a separate pySpark job that will process the JSON
# and create a table
# *******************************************************************************

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, substring, current_date, regexp_replace, lit, udf, array
from pyspark.sql.types import StringType, MapType
from cm_api.api_client import ApiResource
import datetime
import impala_config as cfg
import urllib2
import urllib
from json import loads
import base64
import re

# ********************************CONFIGURATION**********************************

end = cfg.end_time
start = cfg.start_time

# HDFS path to the impalaQueries_<date>.json file created in step 1
hdfsPath = cfg.hdfs_path + "jsons_" + str(start.month) + "-" + str(start.day) + "_to_" + str(end.month) + "-" + str(end.day) + "/"

# *******************************************************************************

# Read in the JSON data:
spark = SparkSession.builder.getOrCreate()
df = spark.read.json(hdfsPath).withColumn("queries", explode("queries")).select("queries.*")

# Add any missing top-level columns and then select them:
raw_columns = ['coordinator', 'database', 'detailsAvailable', 'durationMillis', 'endTime', 'queryId', 'querystate', 'queryType', 'rowsProduced', 'startTime', 'statement', 'user']
for i in range(len(raw_columns)):
    if raw_columns[i] not in df.columns:
        df = df.withColumn(raw_columns[i], lit(None).cast(StringType()))
df = df.selectExpr('attributes.*', *raw_columns)

# Add any missing attribute-level columns and then select them:
attribute_columns = ['admission_result','admission_wait','bytes_streamed','client_fetch_wait_time','client_fetch_wait_time_percentage','connected_user','ddl_type','delegated_user','estimated_per_node_peak_memory','file_formats','hdfs_average_scan_range','hdfs_bytes_read','hdfs_bytes_read_from_cache','hdfs_bytes_read_from_cache_percentage','hdfs_bytes_read_local','hdfs_bytes_read_local_percentage','hdfs_bytes_read_remote','hdfs_bytes_read_remote_percentage','hdfs_bytes_read_short_circuit','hdfs_bytes_read_short_circuit_percentage','hdfs_bytes_skipped','hdfs_bytes_written','hdfs_scanner_average_bytes_read_per_second','impala_version','memory_accrual','memory_aggregate_peak','memory_per_node_peak','memory_per_node_peak_node','memory_spilled','network_address','oom','original_user','planning_wait_time','planning_wait_time_percentage','pool','query_status','rows_inserted','session_id','session_type','stats_corrupt','stats_missing','thread_cpu_time','thread_cpu_time_percentage','thread_network_receive_wait_time','thread_network_receive_wait_time_percentage','thread_network_send_wait_time','thread_network_send_wait_time_percentage','thread_storage_wait_time','thread_storage_wait_time_percentage','thread_total_time']
for i in range(len(attribute_columns)):
    if attribute_columns[i] not in df.columns:
        df = df.withColumn(attribute_columns[i], lit(None).cast(StringType()))
df = df.withColumn("statement", regexp_replace("statement", "\\s+"," "))
df = df.selectExpr("admission_result", "cast(admission_wait as int)", "cast(bytes_streamed as bigint)", "cast(client_fetch_wait_time as int)", "cast(client_fetch_wait_time_percentage as tinyint)", "connected_user", "ddl_type", "delegated_user", "cast(estimated_per_node_peak_memory as bigint)", "file_formats", "cast(hdfs_average_scan_range as float)", "cast(hdfs_bytes_read as bigint)", "cast(hdfs_bytes_read_from_cache as tinyint)", "cast(hdfs_bytes_read_from_cache_percentage as tinyint)", "cast(hdfs_bytes_read_local as bigint)", "cast(hdfs_bytes_read_local_percentage as tinyint)", "cast(hdfs_bytes_read_remote as bigint)", "cast(hdfs_bytes_read_remote_percentage as tinyint)", "cast(hdfs_bytes_read_short_circuit as bigint)", "cast(hdfs_bytes_read_short_circuit_percentage as tinyint)", "cast(hdfs_bytes_skipped as int)", "cast(hdfs_bytes_written as int)", "cast(hdfs_scanner_average_bytes_read_per_second as float)", "cast(memory_accrual as float)", "cast(memory_aggregate_peak as float)", "cast(memory_per_node_peak as float)", "memory_per_node_peak_node", "cast(memory_spilled as bigint)", "network_address", "cast(oom as boolean)", "original_user", "cast(planning_wait_time as smallint)", "cast(planning_wait_time_percentage as smallint)", "pool", "query_status", "cast(rows_inserted as int)", "session_id ", "session_type", "cast(stats_corrupt as boolean)", "cast(stats_missing as boolean)", "cast(thread_cpu_time as int)", "cast(thread_cpu_time_percentage as tinyint)", "cast(thread_network_receive_wait_time as int)", "cast(thread_network_receive_wait_time_percentage as tinyint)", "cast(thread_network_send_wait_time as int)", "cast(thread_network_send_wait_time_percentage as tinyint)", "cast(thread_storage_wait_time as int)", "cast(thread_storage_wait_time_percentage as tinyint)", "cast(thread_total_time as int)", "coordinator.hostid as hostid", "database", "cast(durationmillis as bigint)", "endtime", "queryid ", "querystate", "querytype", "cast(rowsproduced as bigint)", "starttime", "statement", "user")

# Remove duplicates:
df = df.distinct()

# Adding partition columns
df = df.withColumn("query_date", substring(df["starttime"].cast(StringType()), 0, 10))

######################
#    Experimental    #
######################

if cfg.get_details:

    # Get cluster details for use in collecting query details:
    api = ApiResource(cfg.cm_host, cfg.cm_port, cfg.cm_user, cfg.cm_password, use_tls=cfg.ssl, version=cfg.version_number)
    clustersArray = api.get_all_clusters()
    cluster = None
    impala = None

    for clusterInstance in clustersArray:
            if clusterInstance.displayName == cfg.cluster_name:
                    cluster = clusterInstance
                    break

    if not cluster:
            print "Cluster " + cfg.cluster_name + " not found!"
            quit(1)

    for service in cluster.get_all_services():
            if service.type == 'IMPALA':
                    impala = service
                    break

    if not impala:
            print "Could not find IMPALA service"
            quit(1)

    # A function used to parse the details for a query:
    def _parse_details(details):
            ret = dict()
            ret["full_details"] = re.sub('\\s+', ' ', details)
            if '\n' in details:
                # key: val units (val) | key: val
                for x in details.split('\n'):
                    if 'Query Options' in x:
                        vals = x.split(':')[1].split(',')
                        vals_dict = {}
                        for key_val in vals:
                            if '=' in vals:
                                key, val = key_val.split('=')
                                vals_dict[key] = val
                        if 'planner' in x:
                            ret['query_options'] = vals_dict
                        else:
                            ret['planner_query_options'] = vals_dict
                    if 'Estimated Per-Host Mem' in x:
                        ret['estimated_mem_per_host'] = x.split(':')[1].strip()
                        break
            return ret

    # A function used to get the details for a query:
    base64string = base64.b64encode('%s:%s' % (cfg.cm_user, cfg.cm_password))
    cm_url = "http://%s:%s/api/v%s/clusters/%s/services/%s/impalaQueries/" % (cfg.cm_host, cfg.cm_port, cfg.version_number, urllib2.quote(cluster.name), urllib2.quote(impala.name))
    def get_details(id):
        request = urllib2.Request(cm_url + str(id))
        request.add_header("Authorization", "Basic %s" % base64string)
        req = urllib2.urlopen(request)
        return _parse_details(loads(req.read())["details"])
    get_details_udf = udf(get_details, MapType(StringType(), StringType()))

    df = df.repartition(cfg.detail_threads)
    df = df.withColumn("query_details", get_details_udf("queryid"))

else:
    df = df.withColumn("query_details", lit(None).cast(MapType(StringType(), StringType()))

######################
#   /Experimental    #
######################

df = df.repartition('query_date')

spark.sql('SET hive.exec.dynamic.partition=true')
spark.sql('SET hive.exec.dynamic.partition.mode=nonstrict')

spark.sql("USE " + cfg.database_name)
if cfg.table_name in [r[0] for r in spark.sql("SHOW TABLES").select("tableName").collect()]:
        spark.sql("insert into " + cfg.database_name + "." + cfg.table_name + " partition(query_date) select * from tempQueriesTable distribute by query_date")
else:
        df.write.partitionBy('query_date').mode('append').format('parquet').saveAsTable(cfg.database_name + "." + cfg.table_name, compression='snappy')
