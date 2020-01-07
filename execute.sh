#!/bin/sh

CONFIG_FILE=properties.cfg
getProperty() {
    KEY=$1
    PROP_VALUE=`cat $CONFIG_FILE | grep "$KEY" | cut -d'=' -f2`
    echo $PROP_VALUE
}

aws configure set aws_access_key_id $(getProperty "AWS_ACCESS_KEY_ID")
aws configure set aws_secret_access_key $(getProperty "AWS_SECRET_ACCESS_KEY")
aws configure set default.region $(getProperty "REGION")

SOURCE_CODE_S3=$(getProperty "SOURCE_CODE_S3")
aws s3 rm $SOURCE_CODE_S3 --recursive 
aws s3 cp code/ $SOURCE_CODE_S3 --recursive
aws s3 cp properties.cfg $SOURCE_CODE_S3


if [ "$(getProperty "partition")" == "" ]
then
   cluster_name="IMDB Data Processing - `date +%F`"
else
   cluster_name="IMDB Data Processing - "$(getProperty "partition")
fi
region=$(getProperty "REGION")

aws emr create-cluster --name "$cluster_name" --release-label emr-5.28.0 --applications Name=Spark --use-default-roles --ec2-attributes KeyName=spark-emr --instance-type m5.xlarge --instance-count 2 --log-uri s3://udacity-dend-capstone-logs/ --auto-terminate --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=TERMINATE_CLUSTER,Jar=s3://$region.elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3://udacity-dend-capstone-code-ab/aws_script.sh"]


#aws emr create-cluster --name "$cluster_name" --release-label emr-5.28.0 --applications Name=Spark --use-default-roles --ec2-attributes KeyName=spark-emr --instance-type m5.xlarge --instance-count 2 --log-uri s3://udacity-dend-capstone-logs/ --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CANCEL_AND_WAIT,Jar=s3://$region.elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3://udacity-dend-capstone-code-ab/aws_script.sh"]


## log dir in EMR - /mnt/var/log/hadoop/steps/


