#!/usr/bin/env bash
type="weblogs"
weblogs="weblogs"
TEST_QUERIES="s1 s2 s3 s4"
SPARK_SUBMIT=/usr/local/spark/spark-2.3.2-bin-hadoop2.7/bin/spark-submit
class_test=/home/sabha/IdeaProjects/Thesis/src/main/scala/active/Consumer.scala
JAR_File=/home/sabha/IdeaProjects/Thesis/target/Thesis-1.0-SNAPSHOT-jar-with-dependencies.jar

for i in ${type}
do
    echo "type: ${i}"

    if [ ${type} == ${weblogs} ]
    then
        for j in ${TEST_QUERIES}
        do
            echo "${i} Spark Structured Streaming query: ${j}"
		    ${SPARK_SUBMIT} --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --class active.Consumer ${JAR_File} ${type} ${TEST_QUERIES}
        done
    else

            echo "${i} Spark Structured Streaming query: s5"
		    ${SPARK_SUBMIT} --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --class active.Consumer ${JAR_File} ${type} s5
    fi
done





