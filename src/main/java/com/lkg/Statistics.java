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

import java.io.File;
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
        if (args.length < 6) {
            System.err.println("Usage: Statistics <master> <hdfsPath> <inputPath> <outputPath> " +
                    "<whitePath> <blackPath> <localPath>");
            System.exit(1);
        }
        String master = args[0];
        String hdfsPath = args[1];
        String inputPath = args[2];
        String outputPath = args[3];
        String whitePath = args[4];
        String blackPath = args[5];
        String localDst = args[6];
        int limitResult = Integer.parseInt(args[7]);
//        String master = "local[*]";
//        String hdfsPath = "hdfs://localhost:9000";
//        String inputPath = "/sca/result/2020-6-23";
//        String outputPath = "/sca/statistics";
//        String whitePath = "/sca/white/white.txt";
//        String blackPath = "/sca/black/black.txt";
//        boolean excludeMerge = Boolean.getBoolean("false") ;

        if(!hdfsPath.endsWith("/")){
            hdfsPath +="/";
        }
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        LocalDateTime ldt = LocalDateTime.now();
        String dateStr = ldt.format(dtf);
        String mergePath = inputPath + "/merge-" + dateStr + ".txt";
        if (outputPath.endsWith("/")){
            outputPath = outputPath + dateStr.substring(0, 8) + "_SCA_10.0.16.26_maliceurl";
        }else {
            outputPath = outputPath + "/" + dateStr.substring(0, 8) + "_SCA_10.0.16.26_maliceurl";
        }

        // 合并小文件
        mergeFiles(hdfsPath, inputPath, mergePath);
        // 去重
        SparkConf conf = new SparkConf().setAppName("sca_statistics").setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaRDD<String> text = spark.read().textFile(hdfsPath + mergePath).javaRDD();// "hdfs://localhost:9000/aaa"
        long inputCount = text.count();
        JavaPairRDD<String, String> rowRDD = text.mapToPair(s -> new Tuple2<>(s.split("\\|")[0], s));
        JavaRDD<String> resultRDD = rowRDD.reduceByKey((x, y)->x).map(Tuple2::_1);
        long deleteRepetitionCount = resultRDD.count();
        logger.info("=====>inputCount: " + inputCount + "\nfilter: " + (inputCount - deleteRepetitionCount)
                + "\ndelete repetition: " + deleteRepetitionCount);
        // 黑白名单过滤
        JavaRDD<String> black = spark.read().textFile(hdfsPath + whitePath).javaRDD();
        JavaRDD<String> white = spark.read().textFile(hdfsPath + blackPath).javaRDD();
        JavaRDD<String> filterListRDD = black.union(white);
        Broadcast<List<String>> filterListBroadcast = sc.broadcast(filterListRDD.collect());
//        int resultSize = resultRDD.collect().size();
        resultRDD = resultRDD.filter(s -> !filterListBroadcast.value().contains(s));
        resultRDD = resultRDD.filter(s -> s.endsWith(".apk"));
        List<String> result = resultRDD.collect();
        int resultSize = result.size();
        logger.info("=====>result: " + resultSize + "; blacklist and whitelist filter: " + (deleteRepetitionCount - resultSize));
        spark.stop();
        // 转化为json，并写入hdfs
        JSON resultJSON = toJSON(result, limitResult);
        writeToHDFS(hdfsPath, outputPath, resultJSON.toString());
        hdfsDownloadLocal(outputPath, localDst);
        logger.info("=====>spend time: " + (System.currentTimeMillis()-start)/1000 + "s"
                + "\ninputCount:                     " + inputCount
                + "\ndelete repetition filter:       " + (inputCount - deleteRepetitionCount)
                + "\nafter delete repetition:        " + deleteRepetitionCount
                + "\nblacklist and whitelist filter: " + (deleteRepetitionCount - resultSize)
                + "\nresult: " + resultSize
        );
    }

    private static void mergeFiles(String hdfsPath, String inputPath, String mergePathName) {
        Configuration conf = new Configuration();
        conf.set("dfs.datanode.max.xcievers", "4096");
        Path mergePath = new Path(mergePathName);
        try(
            FileSystem hdfs = FileSystem.get(new URI(hdfsPath), conf);
            FSDataOutputStream mergeOutputStream = hdfs.create(mergePath)
        ) {
            FileStatus[] fileStatuses = hdfs.listStatus(new Path(inputPath));
            int total = fileStatuses.length;
            int i = 1;
            for (FileStatus fileStatus : fileStatuses) {
                Path tmpPath = fileStatus.getPath();
                logger.info(i++ + "/" + total + ": " + tmpPath.getName());
                if(tmpPath.getName().startsWith("_") || mergePath.getName().equals(tmpPath.getName())){
                    continue;
                }
                if (!fileStatus.isDirectory()) { //过滤掉文件夹，只操作文件。
                    FSDataInputStream inputStream = hdfs.open(tmpPath);
                    IOUtils.copyBytes(inputStream, mergeOutputStream, 4096, false);
                    IOUtils.closeStream(inputStream);//关闭临时的输入流
                    mergeOutputStream.flush();
                    hdfs.delete(tmpPath, true); //正式环境取消注释
                }else{
                    FileStatus[] files2 = hdfs.listStatus(fileStatus.getPath());
                    for (FileStatus f: files2){
                        if(f.getPath().getName().startsWith("_") || mergePath.getName().equals(f.getPath().getName())){
                            continue;
                        }
                        FSDataInputStream inputStream = hdfs.open(f.getPath());
                        IOUtils.copyBytes(inputStream, mergeOutputStream, 4096, false);
                        IOUtils.closeStream(inputStream);//关闭临时的输入流
                        mergeOutputStream.flush();
//                        hdfs.delete(f.getPath(), true); //正式环境取消注释
                    }
                    hdfs.delete(fileStatus.getPath(), true);
                }
            }
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private static void writeToHDFS(String hdfsPath, String outputPath, String content) throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI(hdfsPath), conf);
        Path dst = new Path(outputPath);
        if(hdfs.exists(dst)){
            hdfs.delete(dst, true);
        }
        FSDataOutputStream outputStream = hdfs.create(dst);
        outputStream.write(content.getBytes());
        outputStream.flush();
        outputStream.close();
        hdfs.close();
    }

    private static void hdfsDownloadLocal(String src, String dst){
        Configuration conf = new Configuration();
        try (FileSystem fs = FileSystem.get(URI.create(src), conf)) {
            Path srcPath = new Path(src);
            File file = new File(dst+"/" + srcPath.getName());
            if (file.exists()) {
                file.delete();
            }
            Path dstPath = new Path(dst+"/" + srcPath.getName());
            fs.copyToLocalFile(srcPath, dstPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
            limit = limit - 1;
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

