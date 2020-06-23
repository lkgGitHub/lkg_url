#!/bash/bin
/data/app_sca/spark-2.3.1-bin-hadoop2.6/bin/spark-submit \
--conf spark.streaming.kafka.consumer.cache.enabled=false \
--num-executors 30 \
--executor-memory 10g \
--executor-cores 10 \
--driver-memory 2g \
--jars jars/config-1.3.0.jar,jars/spark-streaming-kafka-0-10_2.11-2.3.1.jar,jars/spark-streaming_2.11-2.3.1.jar,jars/kafka-clients-0.10.2.1.jar \
--class com.lkg.Predict  \
--master local[*] \
./sca-1.0.jar  \
local[*] \
sream01:9092,stream02:9092,stream03:9092 \
logtopic \
scaGroup \
latest \
47 \
hdfs://nameservice1/sca/url_ads/model/PipLine_Model_Dir/pipModelGbdt \
hdfs://nameservice1/sca/url_ads/model/PipLine_Model_bayes \
hdfs://nameservice1/sca/hj/result