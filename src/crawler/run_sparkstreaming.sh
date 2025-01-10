while true
do
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 ./sparkstreaming.py
	sleep 3
done
