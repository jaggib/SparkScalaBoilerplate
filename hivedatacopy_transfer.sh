id=$1
db=$2
table=$3
if [ -z $id ] || [ -z $db ] || [ -z $table ]; then
echo "Provide your userid, database and table name in the same order"
echo "USAGE : hiveimport.sh <userid> <database> <tablename>"
else
echo "Please provide password for $1 to login to EDPP Dev Environment. This would not be stored."
read -s pass
if [[ -z $pass ]]; then
echo "Empty password provided !"
else
sequence=`shuf -i 10000-34543 -n 1`
dir=/tmp/$1/$sequence/$2_export/$3/
tmpdir=/tmp/$1/$sequence
sshpass -p $pass ssh $1_D2-SERVERNAME@49.32.41.20 << EOF
echo "Trying to import $3 from $2"
#hadoop fs -test -d $dir
#echo $flag
#if [ $flag !=0 ] then echo "Directory is already present" else "Delete Dir" fi
echo "Loading table from Hive to HDFS"
hive -e "EXPORT TABLE $2.$3 to '$dir'"
echo "Copying table from HDFS to Edge"
mkdir -p $dir
hadoop fs -copyToLocal $tmpdir hdfs://$dir
echo "Compressing the files"
tar -cvjf $tmpdir/$sequence.tar.gz $dir
echo "Transferring the contents to Sandbox"
sshpass -p "cloudera" scp -r  $tmpdir/$sequence.tar.gz root@49.19.64.161:/tmp/
EOF
echo "Decompressing the files"
tar -xvf /tmp/$sequence.tar.gz -C /tmp/
hadoop fs -rmdir --ignore-fail-on-non-empty /export/$3
hadoop fs -put /tmp/$dir /export/
#hadoop fs -ls /export/
hive -e "USE $2; DROP TABLE IF EXISTS $3;"
hive -e "USE $2; IMPORT TABLE $3 FROM '/export/$3/$sequence/$2_export/$3/';"
hadoop fs -rmdir --ignore-fail-on-non-empty /export/$3
fi
fi
