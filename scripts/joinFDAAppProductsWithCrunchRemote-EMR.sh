#! /bin/bash

#
# Shell script that executes the Hadoop Job.
#

# The argument to the shell is the IP address of the running
# Hadoop Virtual Machine.

arg_count=1
echo "IP ARG: $1"
ip_address=""
if [ "$#" -ne "${arg_count}" ]; then
	echo "USAGE: joinFDAAppProductsWithCrunchRemote.sh <HADOOP VM IP ADDRESS>"
	exit 1
else
	ip_address="$1"
	echo "IP ADDRESS: ${ip_address}"
fi

# VM_USER is your user name on the HADOOP_VM
# LOCAL_USER is your user name on your local development environment.

VM_USER="bfrench"
LOCAL_USER="bernie"

project_name="joinFDADataWithCrunch"
mainclass="com.superh.awsemr.spike.fdajoin.FDAJoinCrunchJob"

# Create the path to the install directory out on the Hadoop VM
# this is the directory where the job jar will be copied from
# your local machine to the Hadoop VM

install_dir="/tmp/hadoop-${VM_USER}/$project_name"
echo "INSTALL DIRECTORY: ${install_dir}"

# Create the path to the local Hadoop job jar.

target_location="/Users/${LOCAL_USER}/work/data-analytics/aws-emr-test/target"

# Create the full path to the Job Jar. We will rename the
# jar file to a simple jar name that reflects the actual
# main class that we're executing. The Job Jar may have
# more than one executable and the name of the compiled
# Job Jar may not reflect what you are running.

simple_jar_name="joinFDADataWithCrunch.jar"
jar="${install_dir}/${simple_jar_name}"

# Create the paths where you will read and write the data.
# These paths are HDFS file paths out on the Hadoop VM

fda_application_csv_input="-Dfda.application.csv.input=s3://sprh-data/hadoop-test/FDAApplications.test.txt"
fda_product_csv_input="-Dfda.product.csv.input=s3://sprh-data/hadoop-test/FDAProducts.test.txt"
fda_application_avro_output="-Dfda.application.avro.output=s3://sprh-data/hadoop-test/fda-joined"

echo "FDA_APPLICATION_CSV_INPUT_PATH=${fda_application_csv_input}"
echo "FDA_PRODUCT_CSV_INPUT_PATH=${fda_product_csv_input}"
echo "FDA_JOIN_OUTPUT_PATH=${fda_application_avro_output}"

# Create the directory where the jar will be copied out
# to the Hadoop VM and then copy the jar file to the
# Hadoop VM

echo "Creating Install directory"

ssh ${VM_USER}@${ip_address} "mkdir -p ${install_dir}"

echo "Copying Job Jar over to Hadoop VM"

scp ${target_location}/aws-emr-test-1.0-asm.jar ${VM_USER}@${ip_address}:${jar}

# Execute the hadoop command out on the Hadoop VM

echo "Executing Hadoop Job..."
ssh ${VM_USER}@${ip_address} "hadoop jar ${jar} ${mainclass} ${fda_application_csv_input} ${fda_product_csv_input} ${fda_application_avro_output}"

