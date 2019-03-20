#!/usr/bin/env bash

TEST_QUERIES="s1 s2 s3 s4 "
type="weblogs websales"
weblogs="weblogs"

SPARK_SUBMIT=/usr/local/spark/spark-2.3.2-bin-hadoop2.7/bin/spark-submit
class_test=/home/sabha/IdeaProjects/Thesis/src/main/scala/active/Consumer.scala
JAR_File=/home/sabha/IdeaProjects/Thesis/target/Thesis-1.0-SNAPSHOT-jar-with-dependencies.jar


# Initialize log file for data loading times
LOG_FILE_EXEC_TIMES="query_times_consumer.csv"
if [ ! -e "$LOG_FILE_EXEC_TIMES" ]
  then
    touch "$LOG_FILE_EXEC_TIMES"
    echo "STARTDATE_EPOCH|STOPDATE_EPOCH|DURATION_MS|STARTDATE|STOPDATE|TYPE|QUERY" >> "${LOG_FILE_EXEC_TIMES}"
fi

if [ ! -w "$LOG_FILE_EXEC_TIMES" ]
  then
    echo "ERROR: cannot write to: $LOG_FILE_EXEC_TIMES, no permission"
    return 1
fi


   for i in ${type}
   do
        echo "type: ${i}"
                if [ ${i} == ${weblogs} ]
                then
                    for j in ${TEST_QUERIES}
                    do
	                # Measure time for query execution time
	                # Start timer to measure data loading for the file formats
	                STARTDATE="`date +%Y/%m/%d:%H:%M:%S`"
	                STARTDATE_EPOCH="`date +%s`" # seconds since epochstart

                        echo "${i} Spark Structured Streaming query: ${j}"
		                ${SPARK_SUBMIT} --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --class active.Consumer ${JAR_File} ${i} ${j} > consumer_${i}_query_${j}_log.txt 2>&1

                        # Calculate the time
	                    STOPDATE="`date +%Y/%m/%d:%H:%M:%S`"
	                    STOPDATE_EPOCH="`date +%s`" # seconds since epoch
	                    DIFF_s="$(($STOPDATE_EPOCH - $STARTDATE_EPOCH))"
	                    DIFF_ms="$(($DIFF_s * 1000))"

	                    # log the times in load_time.csv file
	                    echo "${STARTDATE_EPOCH}|${STOPDATE_EPOCH}|${DIFF_ms}|${STARTDATE}|${STOPDATE}|${j}|Query ${i}" >> ${LOG_FILE_EXEC_TIMES}


                    done
                else

                    echo "${i} Spark Structured Streaming query: s5"
		            ${SPARK_SUBMIT} --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --class active.Consumer ${JAR_File} ${type} s5 > consumer_${i}_query_s5_log.txt 2>&1

                    # Calculate the time
	                STOPDATE="`date +%Y/%m/%d:%H:%M:%S`"
	                STOPDATE_EPOCH="`date +%s`" # seconds since epoch
	                DIFF_s="$(($STOPDATE_EPOCH - $STARTDATE_EPOCH))"
	                DIFF_ms="$(($DIFF_s * 1000))"

	                # log the times in load_time.csv file
	                echo "${STARTDATE_EPOCH}|${STOPDATE_EPOCH}|${DIFF_ms}|${STARTDATE}|${STOPDATE}|${j}|Query ${i}" >> ${LOG_FILE_EXEC_TIMES}


                fi

    done