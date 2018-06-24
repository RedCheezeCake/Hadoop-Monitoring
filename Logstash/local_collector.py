import os
import sys
import psutil
import pymongo

print "========== MONGODB CONFIGURATION =========="
db_ip = raw_input("Database IP : ")
db_port = raw_input("Database Port : ")
db_name = raw_input("Database Name : ")
db_user = raw_input("Database User : ")
db_pass = raw_input("Database Password : ")

# It is only Testing Configuration ##############################################################
if db_ip=="" :
    db_ip = "10.41.4.230"
if db_port=="" :
    db_port = "27017"
if db_name=="" :
    db_name = "hadoopmon"
if db_user=="" :
    db_user = "hmUser"
if db_pass=="" :
    db_pass = "nbp123"

cluster_id   = "asdasd"
cluster_name = "asdasd"

# read db password argv
# db_ip   = sys.argv[1]
# db_port = sys.argv[2]
# db_name = sys.argv[3]
# db_user = sys.argv[4]
# db_pass = sys.argv[5]
#
# cluster_id   = sys.argv[6]
# cluster_name = sys.argv[7]

# mongodb connection
client = pymongo.MongoClient("mongodb://"+db_user+":"+db_pass+"@"+db_ip+":"+db_port+"/"+db_name)
db = client[db_name]
col = db['ServerResource']
# col='??'
#db[col].query

# data collect and insert
old_value = 0
def convert_to_mbit(value):
    return value/1024./1024.*8
#
# def send_stat(value):
#     return "%0.3f" % convert_to_mbit(value)

while True:
    new_value = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv

    if old_value:
        cpu_percent = psutil.cpu_percent(interval=3)
        memory_total = psutil.virtual_memory().total
        memory_used = psutil.virtual_memory().used
        memory_percent = psutil.virtual_memory().percent
        network_bandwidth = float(format(convert_to_mbit(new_value - old_value), '.2f'))

        # db insert
        col.insert({"clusterId":cluster_id, "clusterName":cluster_name, "cpu":cpu_percent, "memory":{"total":memory_total, "used":memory_used, "percent":memory_percent}, "network":network_bandwidth})
        print "insert!"
    old_value = new_value
