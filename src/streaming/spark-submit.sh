if [ $# -lt 1 ]
then
	echo "Usage: $0 <pyspark-job.py> [ executor-memory ]"
	echo "(specify memory in string format such as \"512M\" or \"2G\")"
	exit 1
fi
PYTHON_JOB=$1

if [ -z $2 ]
then
	EXEC_MEM="1G"
else
	EXEC_MEM=$2
fi

spark-submit --master spark://localhost:7077 --num-executors 2 \
	           --executor-memory $EXEC_MEM --executor-cores 1 \
             --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.1 \
            --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions\
			--conf spark.driver.bindAddress=0.0.0.0 \
            --conf spark.executorEnv.SPARK_HOME=/bin/spark-3.3.1-bin-hadoop3 \
            $PYTHON_JOB