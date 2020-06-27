package com.lkg

import java.util.{Calendar, Date}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Predict {
  /*
   1. 读取数据：input count
   2. 过滤数据：filter count
   3. gbdt: gbdt count
   4. bayes: bayes count
   4. 存入hdfs数据: output count
    */
  def main(args: Array[String]): Unit = {

    val master = "local[3]"
    val duration = 10
    val kafkaServers ="localhost:9092"
    val kafkaTopic = "url"
    val kafkaGroupId = "scaGroup"
    val kafkaReset = "latest" //"latest" "earliest"
    val kafkaUrlIndex = 0
    val gbdtModelPath = "D:\\sca\\PipLine_Model_Dir\\pipModelGbdt"
    val bayesModelPath = "D:\\sca\\PipLine_Model_bayes"
    val dataOutPath = "hdfs://localhost:9000/sca/result"

//    if (args.length < 9) {
//      System.err.println("Usage: Predict <master> <kafkaServers> <kafkaTopic> <kafkaGroupId> <kafkaReset> " +
//        "<kafkaUrlIndex> <gbdtModelPath> <bayesModelPath> <dataOutPath>")
//      System.exit(1)
//    }
//    val master = args(0) // "local[*]"
//    val duration = args(1).toInt //10
//    val kafkaServers = args(2) //"localhost:9092"
//    val kafkaTopic = args(3) // "url"
//    val kafkaGroupId = args(4) //scaGroup
//    val kafkaReset = args(5) //
//    val kafkaUrlIndex = args(6).toInt // 76
//    val gbdtModelPath = args(7) // "D:\\sca\\PipLine_Model_Dir\\pipModelGbdt"
//    val bayesModelPath = args(8) // "D:\\sca\\PipLine_Model_bayes"
//    val dataOutPath = args(9) // "hdfs://localhost:9000/sca/result"

    //1. 初始化Spark
    val sparkConf = new SparkConf().setAppName("sca_predict").setMaster(master)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.backpressure.enabled","true")//启用反压
      .set("spark.streaming.backpressure.pid.minRate","1")//最小摄入条数控制
      .set("spark.streaming.kafka.maxRatePerPartition","10000")//最大摄入条数控制

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("INFO")
    val ssc = new StreamingContext(sc, Seconds(duration)) //StreamingContext 是所有流功能的主要入口点。
    //屏蔽日志
//    Logger.getLogger("org.apache").setLevel(Level.ERROR)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)

    //1. 读取kafka数据
    var urls = readKafka(ssc, kafkaServers, kafkaTopic, kafkaGroupId, kafkaReset, kafkaUrlIndex)
    urls.count().print()
    println(s"1 kafka input count:")
    //    urls.print(5)  //测试：OK

    // 2. 处理数据 todo 过滤后缀、过滤白名单
    urls = urls.filter(
      u=>u.endsWith("apk") ||u.endsWith("sis") ||u.endsWith("sisx")||u.endsWith("jar") ||u.endsWith("exe")
        || u.endsWith("cab") ||u.endsWith("ipa") ||u.endsWith("elf") ||u.endsWith("cod") ||u.endsWith("alx")
        ||u.endsWith("prc") || u.endsWith("zip") ||u.endsWith("rar")
    )
//    println(s"2. suffix filter count: ${urls.count().print()}")
//    urls.print(5)  //测试：OK

    val pipModelGbdt = PipelineModel.load(gbdtModelPath)
    val pipModelBayes = PipelineModel.load(bayesModelPath)
    urls.foreachRDD { rdd =>
      //todo: 数据监控与统计，过程按天简单统计
      val spark = SparkSession.builder.master(master).getOrCreate()
      import spark.implicits._
      //将当前批数据转换成dataframe
      val df = rdd.toDF("url")
      //      df.show(5) //测试：OK

      //选出上面过滤后的dataframe中的url列，形成新的dataframe：fileFX
      val fileFX = df.select("url")
//      println(s"urls count: ${fileFX.count()}")

      // 构建测试数据【通用】 构建特征：url,port端口,domainNameLevels域名级数长度,isIP是否含有IP,urllen URL长度,
      // longTermCount敏感词的数目,isDing是否顶级域名,is_Prefix_sensitive是否含有敏感词后缀,ishaschn是否含有中文,
      // execfile_len（val url_part=url.split("\\/") val execfile=url_part(url_part.length-1））,
      // Numricinlength 下载文件含有的数字长度,label
      val dataSeq = new UrlDataExtractForPredict(fileFX).init_feature()
//      println(s"3.1 构建特征: ${dataSeq} \n")
      //构建测试数据集：转换列数据类型，转存至dataframe并返回
      val dataTest = new LoadUrlFeaturePredict(dataSeq, sc, spark).vLoad_data() // ref to proj1: classifynew/TrainV.scala
      println(s"3.2 构建测试数据集: ${dataTest.show(5)} \n")

      //进行预测【通用】 gbdt
      val save_info_hdfs_gbdt = pipModelGbdt.transform(dataTest)
        .selectExpr("  Url as url_GBDT ", "0.58 as prob_GBDT")
        .where("predictions=0")
      save_info_hdfs_gbdt.selectExpr("prob_GBDT")
      println(s"3 gbdt output count: ${save_info_hdfs_gbdt.count()}")


      //构建测试数据【贝叶斯】构建特征，返回：Seq(url,和处理后的url（去除了协议，将[.=&?-@:(]均替换成了[/]）)
      val dataSeqBayes = new UrlDataExtractForBayesPredict(fileFX).__INIT_Feature__()
      //构建训练集：每一行是一个对象 UrlFeature_Bayes_predict（Url: String, Word:String）
      val dataBayesTest = new LoadUrlFeatureBayesPredict(dataSeqBayes, sc, spark).vLoad_data()
//      println(s"4.1 构建特征: ${dataBayesTest} \n")

      //进行预测【贝叶斯】
      val save_info_bayes_hdfs = pipModelBayes.transform(dataBayesTest)
        .selectExpr("  Url as url_Bayes ", "myProbability as prob_Bayes")
        .where("predictions=0")
      println(s"4 bayes output count: ${save_info_bayes_hdfs.count()}") // \n ${save_info_bayes_hdfs.show(5)}

      //模型融合，对各算法识别出的结果进行连接操作，将同时被检测出的疑似恶意URL作为检测结果
      var save_info_hdfs=save_info_hdfs_gbdt.join(save_info_bayes_hdfs, $"url_GBDT" === $"url_Bayes")
        .selectExpr("url_GBDT as url","prob_GBDT","prob_Bayes")
      val label_class = udf((x:Vector) => x(0))
      save_info_hdfs = save_info_hdfs.withColumn("prob_Bayes", label_class(col("prob_Bayes")))

      println( s"5 save hdfs count: ${save_info_hdfs.count()}")// \n ${save_info_hdfs.show(5)}
      //将结果写入hdfs
      if(save_info_hdfs.count()!=0){
        save_info_hdfs.map(f=>f(0)+"|"+"0.8"  )//.show()
        saveAsFileAbsPath(save_info_hdfs, dataOutPath, "|", SaveMode.Append)
      }
    }

    ssc.start(); //sparkStream启动程序,开始计算
    ssc.awaitTermination(); //sparkStream阻塞等待, 等待计算被中断
  }

  // 读取kafka数据
  def readKafka(ssc: StreamingContext, kafkaServers: String, topic: String,kafkaGroupId: String, reset: String,
                urlIndex: Int ): DStream[String] ={

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafkaGroupId,
      "auto.offset.reset" -> reset, // "latest".earliest:无提交的offset时，从头开始消费; latest: 无提交的offset时，消费新产生的该分区下的数据
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array(topic) //我们需要消费的kafka数据的topic列表
    //kafka消费者进行消费
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
//    stream.print(1)
    //    stream.map(record=>record.value)
    stream.map(record=>record.value.split("\\|")(urlIndex))
  }

  /*** 将 DataFrame 保存为 hdfs 文件 同时指定保存绝对路径 与 分隔符
   *
   * @param dataFrame  需要保存的 DataFrame
   * @param absSaveDir 保存保存的路径 （据对路径）
   * @param splitRex   指定分割分隔符
   * @param saveMode   保存的模式：Append、Overwrite、ErrorIfExists、Ignore
   */
  def saveAsFileAbsPath(dataFrame: DataFrame, absSaveDir: String, splitRex: String, saveMode: SaveMode): Unit = {
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    val day: String = cal.get(Calendar.DAY_OF_MONTH).toString
    val month: String = (cal.get(Calendar.MONTH) + 1).toString
    val year: String = cal.get(Calendar.YEAR).toString
    val outpath = s"$absSaveDir/$year-$month-$day"
    dataFrame.sqlContext.sparkContext.hadoopConfiguration.set("mapred.output.compress", "false")
    //为了方便观看结果去掉压缩格式
    val allColumnName: String = dataFrame.columns.mkString(",")
    val result: DataFrame = dataFrame.selectExpr(s"concat_ws('$splitRex',$allColumnName) as allclumn")
    result.write.mode(saveMode).text(outpath)
  }

}
