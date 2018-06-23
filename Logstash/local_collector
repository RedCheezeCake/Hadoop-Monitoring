import os
import sys
import psutil
import pymongo
# read db password argv
db_password=sys.argv[1]
print db_password

# read cluster meta
cluster_meta = open("./cluster_meta", "r")
lines = cluster_meta.readlines()
cluster_id=lines[0].split()[1]
cluster_name=lines[1].split()[1]
cluster_meta.close()

# mongodb connection
connection = pymongo.MongoClient("mongodb://hmUser:"+db_password+"@10.41.4.230/hadoopmon")
db = connection.hadoopmon
col = 'serverResource'
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
        db[col].insert({"cpu":cpu_percent, "memory":{"total":memory_total, "used":memory_used, "percent":memory_percent}, "network":network_bandwidth})

    old_value = new_value
