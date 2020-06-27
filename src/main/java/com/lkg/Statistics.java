package com.lkg;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Statistics {
    private static Logger logger = Logger.getLogger(Statistics.class);
    private static final String[] areaArray = {"AH", "BJ", "CQ", "FJ", "GD", "GS", "GX", "GZ", "HA", "HB", "HE", "HI", "HL",
            "HN", "JL", "JS", "JX", "LN", "NM", "NX", "QH", "SC", "SD", "SH", "SN", "SX", "TJ", "XJ", "XZ", "YN", "ZJ"};
    private static final int areaArrayLength = areaArray.length;

    public static void main(String[] args) throws IOException, URISyntaxException {
        long start = System.currentTimeMillis();
        if (args.length < 4) {
            System.err.println("Usage: Statistics <master> <hdfsPath> <inputPath> <outputPath> <whitePath> <blackPath>");
            System.exit(1);
        }
        String master = args[0];
        String hdfsPath = args[1];
        String inputPath = args[2];
        String outputPath = args[3];
        String whitePath = args[4];
        String blackPath = args[5];
        boolean excludeMerge = Boolean.getBoolean(args[6]) ;
        if(!hdfsPath.endsWith("/")){
            hdfsPath +="/";
        }
//        String master = "local[*]";
//        String hdfsPath = "hdfs://localhost:9000";
//        String inputPath = "/sca/result/2020-6-23";
//        String outputPath = "/sca/statistics";
//        String whitePath = "/sca/white/white.txt";
//        String blackPath = "/sca/black/black.txt";

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        LocalDateTime ldt = LocalDateTime.now();
        String dateStr = ldt.format(dtf);
        if (outputPath.endsWith("/")){
            outputPath = outputPath + dateStr + "_SCA_10.0.16.26_maliceurl";
        }else {
            outputPath = outputPath + "/" + dateStr + "_SCA_10.0.16.26_maliceurl";
        }
        String mergePath = inputPath + "/merge-" + dateStr + ".txt";

        // 合并小文件
        mergeFiles(hdfsPath, inputPath, mergePath, excludeMerge);
        // 去重
        SparkConf conf = new SparkConf().setAppName("sca_statistics").setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaRDD<String> text = spark.read().textFile(hdfsPath + mergePath).javaRDD();// "hdfs://localhost:9000/aaa"
        JavaPairRDD<String, String> rowRDD = text.mapToPair(s -> new Tuple2<>(s.split("\\|")[0], s));
        JavaRDD<String> resultRDD = rowRDD.reduceByKey((x, y)->x).map(Tuple2::_2);
        // 过滤
        JavaRDD<String> black = spark.read().textFile(hdfsPath + whitePath).javaRDD();
        JavaRDD<String> white = spark.read().textFile(hdfsPath + blackPath).javaRDD();
        JavaRDD<String> filterListRDD = black.union(white);
        Broadcast<List<String>> filterListBroadcast = sc.broadcast(filterListRDD.collect());
        int resultSize = resultRDD.collect().size();
        resultRDD = resultRDD.filter(s -> !filterListBroadcast.value().contains(s));
        List<String> result = resultRDD.collect();
        System.out.println("===>result: " + result.size() + "; filter: " + (resultSize - result.size()));
        // 转化为json，并写入hdfs
        JSON resultJSON = toJSON(result, 10000);
        writeToHDFS(hdfsPath, outputPath, resultJSON.toString());
        spark.stop();
        logger.info("spend time: " + (System.currentTimeMillis()-start)/1000 + "s");
    }

    private static void mergeFiles(String hdfsPath, String inputPath, String outputPath, boolean excludeMerge) throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI(hdfsPath), conf);
        FSDataOutputStream outputStream = hdfs.create(new Path(outputPath));
        FileStatus[] fileStatuses = hdfs.listStatus(new Path(inputPath));
        for (FileStatus fileStatus : fileStatuses) {
            if (!fileStatus.isDirectory()) { //过滤掉文件夹，只操作文件。
                Path tmpPath = fileStatus.getPath();
                if(tmpPath.getName().contains("merge") && excludeMerge){
                    continue;
                }
                FSDataInputStream inputStream = hdfs.open(tmpPath);
                IOUtils.copyBytes(inputStream, outputStream, 4096, false);
                IOUtils.closeStream(inputStream);//关闭临时的输入流
                hdfs.delete(tmpPath, true); //正式环境取消注释
            }
        }
        outputStream.close();
        hdfs.close();
    }

    private static void writeToHDFS(String hdfsPath, String outputPath, String content) throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI(hdfsPath), conf);
        Path dst = new Path(outputPath);
        if(hdfs.exists(dst)){
            hdfs.delete(dst, true);
        }
        FSDataOutputStream outputStream = hdfs.create(dst);
        outputStream.writeBytes(content);
        outputStream.flush();
        outputStream.close();
        hdfs.close();
    }

    private static JSON toJSON(List<String> result, int limit){
        Random rand = new Random();
        int size = result.size();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime ldt = LocalDateTime.now();
        String date = ldt.format(dtf);
        List<Map<String, String>> list = new LinkedList<>();
        for(String u: result){
            Map<String, String> map = new HashMap<>();
            String domain;
            try {
                URL url = new URL(u);
                domain = url.getHost();
            } catch (MalformedURLException e) {
                e.printStackTrace();
                continue;
            }
            map.put("area", areaArray[rand.nextInt(areaArrayLength)]);
            map.put("degree", "0.8");
            map.put("domain", domain);
            map.put("url", u);
            list.add(map);
            limit -= 1;
            if (limit<=0){
                break;
            }
        }
        R r = new R(date, size, list);
        return (JSON) JSON.toJSON(r);
    }

    private static class R{
        private String date;
        private int size;
        private List<Map<String, String>> list;

        public R(String date, int size, List<Map<String, String>> list) {
            this.date = date;
            this.size = size;
            this.list = list;
        }


        @Override
        public String toString() {
            return "R{" +
                    "date='" + date + '\'' +
                    ", size='" + size + '\'' +
                    ", list=" + list +
                    '}';
        }

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public List<Map<String, String>> getList() {
            return list;
        }

        public void setList(List<Map<String, String>> list) {
            this.list = list;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }
    }
}

//        System.out.println("====================================================");
//        filterListBroadcast.value().foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });
//        System.out.println("====================================================");
//        resultRDD.saveAsTextFile(hdfsPath +  outputPath);//"hdfs://localhost:9000/bbb"
//        List<String> result = resultRDD.collect();

