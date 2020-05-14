# Authors: Shashank Naik & Eric Maynard

# ********************************DESCRIPTION************************************

# This script is part of a 2 step process to gather details on the impala queries
# This is step 1 which will use the CM api libraries to fetch the Impala Queries
# and save them as jsons in the local folder and in hdfs

# *******************************************************************************

from cm_api.api_client import ApiResource
import datetime
import getpass
import json
import logging
import os
import impala_config as cfg
import subprocess

end = cfg.end_time
start = cfg.start_time

timestamp = str(start.month) + "-" + str(start.day) + "_to_" + str(end.month) + "-" + str(end.day)
baseFileName = "impalaQueries_" + timestamp
outputDir = "jsons/jsons_" + timestamp

runLogFolder = cfg.log_folder + "/logs_" + timestamp
logging.basicConfig(level=logging.DEBUG)
if not os.path.exists(runLogFolder):
        os.makedirs(runLogFolder)

logger = logging.getLogger(__name__)

logger.info("The jsons for this run will locally written to " + str(outputDir))

handler = logging.FileHandler(runLogFolder + "/" + __file__ + ".log")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

if not os.path.exists(outputDir):
        os.makedirs(outputDir)

api = ApiResource(cfg.cm_host, cfg.cm_port, cfg.cm_user, cfg.cm_password, use_tls=cfg.ssl, version=cfg.version_number)
clustersArray = api.get_all_clusters()
cluster = None
impala = None

for clusterInstance in clustersArray:
        if clusterInstance.displayName == cfg.cluster_name:
                cluster = clusterInstance
                logger.info("Found cluster " + cfg.cluster_name)
                break

if not cluster:
        logger.error("Cluster " + cfg.cluster_name + " not found!")
        quit(1)

for service in cluster.get_all_services():
        if service.type == 'IMPALA':
                impala = service
                break

if not impala:
        logger.error("Could not find IMPALA service")
        quit(1)

def run_cmd(args_list):
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return = proc.returncode
        return s_return, s_output, s_err

# ---------------------------------------------------
logger.info("The Master Start Time is: " + str(start))
logger.info("The Master End Time is: " + str(end))

batchNum = 1
totalCount = 0
batchesDone = False
foundWarning = False
tempBatchSize = cfg.batch_size
fileCount = 0

################### BATCH LOOP ######################

while (not batchesDone):
        batchQueryCount=0
        response_list=[]
        callNum=0
        if (foundWarning==False):
                startForBatch = end + datetime.timedelta(hours=-cfg.batch_size)
        if(startForBatch<=start):
                startForBatch=start
                batchesDone = True
                logger.debug("Reached the end of the window " + str(startForBatch) + " " + str(start))
        limit = 1000
        offset = 0
        logger.info("Batch number: " + str(batchNum) + " started. Intended StartTime: " + str(startForBatch) + " and EndTime: " + str(end))
        callsComplete=False


################### CALL LOOP #######################

        while (not callsComplete):
                dummyts = end + datetime.timedelta(milliseconds=-1)
                if (startForBatch < dummyts):
                        response = impala.get_impala_queries(startForBatch, end, cfg.filter_string, limit, offset)
                else:
                        logger.info("Loop exited because SMON does not have any data for this period, moving on...")
                        callsComplete = True
                        continue
                logger.debug(response.warnings)
                if len(response.warnings) > 0:
                        tempBatchSize = tempBatchSize - (0.2 * tempBatchSize)
                        startForBatch = end + datetime.timedelta(hours=-tempBatchSize)
                        logger.debug("Adjusting start time: " + str(startForBatch))
                        batchesDone = False
                        continue
                callCount = len(response.queries)
                batchQueryCount = batchQueryCount + callCount
                callNum += 1
                logger.debug("THIS IS BATCH " + str(batchNum) + " and CALL NUMBER " + str(callNum) + " got query count as " + str(callCount))
                if callCount == 0:
                        tempBatchSize = cfg.batch_size
                        foundWarning = False
                        callsComplete = True
                else:
                        response_list.append(response.to_json_dict(True))
                offset += callCount
                totalCount += callCount
                logger.debug("Number of queries so far: " + str(totalCount))
        logger.info("Batch number: "+ str(batchNum) + " completed. \nEventual StartTime: " + str(startForBatch) + " and EndTime: " + str(end))
        logger.info("The number of queries extracted so far is: " + str(totalCount))
        end = startForBatch
        batchNum += 1
        fileCount += 1
        with open(outputDir + "/" + baseFileName + "_" + str(fileCount) + "_" + str(batchQueryCount) + ".json", "a") as outfile:
                json.dump(response_list,outfile,indent=None)

logger.info("Total impala queries are " + str(totalCount))
logger.info ("Moving folder: " + outputDir + " to HDFS location " + cfg.hdfs_path)
(ret, out, err) = run_cmd(['hdfs', 'dfs', '-put', '-f', outputDir, cfg.hdfs_path])

if len(err) > 1:
        logger.error("Error moving folder to hdfs: " + err + "\nQuitting program! Please fix and rerun")
        quit(1)

logger.info("Removing local json folder: " + outputDir)
(ret, out, err) = run_cmd(['rm', '-r', outputDir])

if len(err) > 1:
        logger.warn("Error removing local folder: " + err + "\nBut continuing with program")

logger.info("Folder removed.")
logger.info("Finished.")
