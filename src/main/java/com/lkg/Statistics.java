package com.lkg;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class Statistics {

    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length < 4) {
            System.err.println("Usage: Statistics <master> <hdfsPath> <inputPath> <outputPath> ");
            System.exit(1);
        }
        String master = args[0];
        String hdfsPath = args[1];
        String inputPath = args[2];
        String outputPath = args[3];
        if(!hdfsPath.endsWith("/")){
            hdfsPath +="/";
        }

        mergeFiles(hdfsPath, inputPath, outputPath);
        SparkSession spark = SparkSession.builder().appName("sca_statistics").master(master).getOrCreate();
        JavaRDD<String> text = spark.read().textFile(hdfsPath + outputPath).javaRDD();// "hdfs://localhost:9000/aaa"
        JavaPairRDD<String, String> rowRDD = text.mapToPair(s -> new Tuple2<>(s.split("\\|")[0], s));
        JavaRDD<String> resultRDD = rowRDD.reduceByKey((x, y)->x).map(Tuple2::_2);
        resultRDD.saveAsTextFile(hdfsPath +  outputPath + "-statistics");//"hdfs://localhost:9000/bbb"
        List<String> result = resultRDD.collect();
        for (String s: result){
            System.out.println(s);
        }
        spark.stop();
    }

    private static void mergeFiles(String hdfsPath, String inputPath, String outputPath) throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI(hdfsPath), conf);
        FSDataOutputStream outputStream = hdfs.create(new Path(outputPath));
        FileStatus[] fileStatuses = hdfs.listStatus(new Path(inputPath));
        for (FileStatus fileStatus : fileStatuses) {
            if (!fileStatus.isDirectory()) { //过滤掉文件夹，只操作文件。
                Path tmpPath = fileStatus.getPath();
                FSDataInputStream inputStream = hdfs.open(tmpPath);
                IOUtils.copyBytes(inputStream, outputStream, 4096, false);
                IOUtils.closeStream(inputStream);//关闭临时的输入流
                hdfs.delete(tmpPath, true);
            }
        }
        outputStream.close();
        hdfs.close();
    }
}

