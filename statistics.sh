#!/bash/bin
/data/app_sca/spark-2.3.1-bin-hadoop2.6/bin/spark-submit \
--jars jars/spark-streaming_2.11-2.3.1.jar,jars/spark-streaming-kafka-0-10_2.11-2.3.1.jar,jars/kafka-clients-1.1.0.jar,jars/fastjson-1.2.71.jar \
--class com.lkg.Statistics \
--master local[*] \
./sca-1.0.jar  \
local[*] \
hdfs://nameservice1 \
/sca/hj/result/20200629 \
/sca/hj/statistics \
/sca/url_ads/white_list/white.txt \
/sca/url_ads/black_list/black.txt \
/data/prj_sca/hj
#2>&1 &