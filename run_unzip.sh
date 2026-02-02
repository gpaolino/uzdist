#!/bin/bash

log_file=$(date +"%Y%m%d%H%M%S_unzip")
workspace_path=/path_to_my_workspace...

echo Kerberos Authentication
$workspace_path/renew_tgt.sh >> $workspace_path/logs/$log_file.log 2>&1 &
PID_RENEW=$!
echo "TGT renewal process started in background with PID $RENEW_PID"

export HADOOP_HOME=/opt/cloudera/parcels/CDH
export HADOOP_CONF_DIR=/etc/hadoop/conf
export LD_LIBRARY_PATH=/opt/cloudera/parcels/CDH/lib64:$JAVA_HOME/lib/server:$LD_LIBRARY_PATH
export CLASSPATH="$($HADOOP_HOME/bin/hadoop classpath)"

spark3-submit \
  $workspace_path/jobs/unzip_split.py 2>&1 | tee $workspace_path/logs/$log_file.log

exit_code=${PIPESTATUS[0]}

if [ $exit_code -eq 0 ]; then
    echo "Success."
else
    echo "Error!"
    exit 1
fi
