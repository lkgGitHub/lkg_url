#!/bash/bin
nohup /data/app_sca/spark-2.3.1-bin-hadoop2.6/bin/spark-submit \
--jars jars/spark-streaming_2.11-2.3.1.jar,jars/spark-streaming-kafka-0-10_2.11-2.3.1.jar,jars/kafka-clients-1.1.0.jar,jars/fastjson-1.2.71.jar \
--conf spark.streaming.backpressure.enabled=true \
--conf spark.streaming.kafka.maxRatePerPartition=100000 \
--num-executors 30 \
--executor-memory 30g \
--executor-cores 20 \
--driver-memory 10g \
--class com.lkg.Predict  \
--master yarn \
./sca-1.0.jar  \
yarn \
5 \
sream01:9092,stream02:9092,stream03:9092 \
logtopic \
scaGroup \
latest \
25 \
hdfs://nameservice1/sca/url_ads/model/PipLine_Model_Dir/pipModelGbdt \
hdfs://nameservice1/sca/url_ads/model/PipLine_Model_bayes \
hdfs://nameservice1/sca/hj/result \
2>&1 &