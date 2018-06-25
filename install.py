##############################
## PIP INSTALL
##############################
import os
# pip install
try :
    import pip
except :
    print "installing pip"
    cmd = "yum install epel-release -y"
    os.system(cmd)
    cmd = "yum install python-pip"
    os.system(cmd)
    import pip
else :
    print "pip is already installed"

#  package install
os.system('pip install paramiko')
import paramiko
os.system('pip install pymongo')
from pymongo import MongoClient

##############################
## DATABASE CONNECTION CHECK
##############################
# Mongodb server for data collecting
db_ip = ""
db_port = ""
db_name = ""
db_pass = ""

while 1 :
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
    try :
        conn = MongoClient("mongodb://"+db_user+":"+db_pass+"@"+db_ip+":"+db_port+"/"+db_name)
         # The ismaster command is cheap and does not require auth.
        conn.admin.command('ismaster')
    except  :
        print "This Mongodb Server is not available !\n"
    else :
        print "Mongodb Server is available !"
        ############
        # conn[db_name].command("createUser","hm_was",pwd="was123",roles=["read"])
        conn.close()
        break


##############################
## HOSTNAME SET
##############################
# base directory
BASE_DIR = "/root/hm_workspace/"
os.system("mkdir -p "+BASE_DIR)

# hostnames variable setting for deploying
hadoop_conf_dir="/etc/hadoop/conf/" # This may change depending on your hadoop settings.
hostnames=[]

cmd_namehost="cat "+hadoop_conf_dir+"/hdfs-site.xml | grep -A 1 'dfs.namenode.http-address.' | grep '<value>'" + \
             "| sed -e 's/<value>//g' | sed -e 's/<\/value>//g' | sed -e 's/:/ /g' |  awk '{print $1}'"
hostnames=os.popen(cmd_namehost).readlines()

cmd_datahost="hdfs dfsadmin -report | grep 'Hostname: ' | awk '{print $2}'"
hostnames+=os.popen(cmd_datahost).readlines()

# readlines append '\n'
# rstrip '\n'
for idx in range(0,len(hostnames)) :
    hostnames[idx] = hostnames[idx].rstrip('\n') #remove strip


##############################
## META DATA SET
##############################
# get cluster id into Namenode config
namenode_conf_dir = "/ambari/hadoop/hdfs/namenode/current/VERSION"
cmd_cluster_id = "cat "+namenode_conf_dir+ "| grep 'clusterID' "
cluster_id = os.popen(cmd_cluster_id).readline().rstrip('\n').split('=')[1] # format : clusterID=xxxxxxxx

# get cluster name by user input
cluster_name = raw_input("Insert your cluster name : ")

# write file
cluster_meta_file_path = BASE_DIR+"cluster_"+cluster_id
cluster_meta_file = open(cluster_meta_file_path, 'w')
cluster_meta_file.write("cluster ID   = "+cluster_id)
cluster_meta_file.write("\ncluster Name = "+cluster_name)
cluster_meta_file.write("\nmongodb ip   = "+db_ip)
cluster_meta_file.write("\nmongodb port = "+db_port)
cluster_meta_file.write("\nmongodb user = hm_was")
cluster_meta_file.write("\nmongodb pwd  = was123")
cluster_meta_file.close()

# show info
print "========== CLUSTER INFO =========="
print "cluster id   : " + cluster_id
print "cluster name : " + cluster_name
print "hostnames    : "
for idx in range(0, len(hostnames)) :
    print "["+str(idx)+"] : " + hostnames[idx]

# send meta data file to WAS by ssh
while 1 :
    print "Send Meta Data File to WAS... "
    was_ip = raw_input("WAS IP : ")
    was_user = raw_input("WAS User : ")
    was_pass = raw_input("WAS Password : ")

    # It is only Testing Configuration ##############################################################
    if was_ip=="" :
        was_ip = "10.41.0.208"
    if was_user=="" :
        was_user = "root"
    if was_pass=="" :
        was_pass = "kdh1200#@!"

    msg = os.system("scp " +cluster_meta_file_path+" "+was_user+"@"+was_ip+":/root/webserver/cluster_list/")
    # 0 is good
    if msg != 0 :
        print "This Web Server is not available !\n"
    else :
        print "Cluster Meta Data Send [ok]"
        break

##############################
## DEPLOY TRIGGER
##############################
# download trigger.py
import urllib

print "Download trigger"
trigger_url = "https://github.com/RedCheezeCake/Hadoop-Monitoring/raw/master/Logstash/trigger.py"
urllib.urlretrieve(trigger_url, BASE_DIR+"trigger.py")
target_dir='/root/hm_data_collector/'

# deploy and launch
print "Start deploying..."
for target_host in hostnames :
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print "["+target_host+"]"
    try :
        ssh.connect(target_host, username = 'root', password = '')
    except :
        while 1 :
            target_pwd =  raw_input("plz " + target_host + "'s Password : ")
            try :
                ssh.connect(target_host, username = 'root', password = target_pwd)
            except :
                print "Password is not correct!\n[TRY AGAIN]"
            else :
                break

    ssh.exec_command('mkdir -p '+target_dir)
    print "create directory"

    # deploy trigger
    sftp = ssh.open_sftp()
    sftp.put(BASE_DIR+"trigger.py", target_dir+"trigger.py")

    # excute trigger
    # argv[db_ip, db_port, db_name, db_user, db_pass, cluster_id, cluster_name]
    ssh.exec_command('nohup python '+target_dir+'trigger.py '+db_ip+' '+db_port+' '+db_name+' '+db_user+' '+db_pass+' '+cluster_id+' '+cluster_name+' &', timeout=3)

    # ssh.exec_command('nohup python '+BASE_DIR+'trigger.py /dev/null 2>&1 &')
    ssh.close()
