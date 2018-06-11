#! /bin/sh
#echo "Logstash install batch..."

## Download Logstash 
#wget -P /opt https://artifacts.elastic.co/downloads/logstash/logstash-6.2.4.tar.gz
#tar -xzf /opt/logstash-6.2.4.tar.gz
#rm /opt/logstash-6.2.4.tar.gz

## Download logstash.conf
wget -O /opt/logstash-6.2.4/logstash.conf https://github.com/RedCheezeCake/Hadoop-Monitoring/blob/master/Logstash/logstash.conf?raw=true

## Set Logstash env
export LS_HOME="/opt/logstash-6.2.4"
export LS_BIN="/opt/logstash-6.2.4/bin"

echo $LS_HOME
echo $LS_BIN

## run logstash 
$LS_BIN/logstash -f $LS_HOME/logstash.conf

