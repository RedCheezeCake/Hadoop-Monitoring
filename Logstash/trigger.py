import os
import sys

BASE_DIR = "/home/hm_data_collector/"

##############################
## DATA COLLECTOR SET
##############################
# Data Collector : logstash and localResource.py
# [Logstash] collects data for hadoop state
# [localResource.py] collects data for server's local resource state
import urllib
import tarfile
print "BANG!!!!!!!!!!"
logstash_url = "https://artifacts.elastic.co/downloads/logstash/logstash-6.2.4.tar.gz"
ls_conf_url = "https://raw.github.com/RedCheezeCake/Hadoop-Monitoring/blob/master/Logstash/logstash.conf"
lc_py_url = "https://raw.github.com/RedCheezeCake/Hadoop-Monitoring/blob/master/Logstash/local_collector"

# LOGSTASH PART
# Download logstash
urllib.urlretrieve(logstash_url, BASE_DIR+"logstash.tar.gz")

# print Extract logstash.tar.gz
tar = tarfile.open(BASE_DIR+"logstash.tar.gz")
tar.extractall(path=BASE_DIR, members=None)
tar.close()

# dir rename logstashxxx -> logstash
os.remove(BASE_DIR+"logstash.tar.gz")
cur_ls_name = os.popen('ls ' + BASE_DIR + ' | grep logstash ').readline().rstrip('\n')
os.rename(BASE_DIR+cur_ls_name, BASE_DIR+'logstash')

# download logstash-output-mongodb plugin
os.popen(BASE_DIR+"logstash/bin/logstash-plugin install logstash-output-install")

# Download conf and local collector
urllib.urlretrieve(ls_conf_url, BASE_DIR+"logstash/logstash.conf")
urllib.urlretrieve(lc_py_url, BASE_DIR+"logstash/local_collector.py")


##############################
## DATA COllECTOR TRIGGER
##############################
# set db and cluster info by argv
db_ip   = sys.argv[1]
db_port = sys.argv[2]
db_name = sys.argv[3]
db_user = sys.argv[4]
db_pass = sys.argv[5]

cluster_id   = sys.argv[6]
cluster_name = sys.argv[7]

# get hostname
hostname = os.popen('uname -n').readline().rstrip('\n')

# Modify logstash.conf
ls_conf_template = open(BASE_DIR + "logstash.conf", 'r').readlines()
components = {"$DB_IP":db_ip, "$DB_PORT":db_port, "$DB_NAME":db_name, "$DB_USER":db_user,
            "$DB_PASS":db_pass, "$CLUSTER_ID":cluster_id, "$CLUSTER_NAME":cluster_name, "$HOSTNAME":hostname}
ls_conf = ""

for line in ls_conf_template :
    temp_line = line
    for comp in components.keys() :
        if comp in line :
            temp_line = line.replace(comp,components[comp])
    ls_conf += temp_line

# write modified logstash.conf
ls_conf_file = open(BASE_DIR + "logstash.conf", 'w')
ls_conf_file.write(ls_conf)
ls_conf_file.close()

# launch logstash


# launch local_collector.py
