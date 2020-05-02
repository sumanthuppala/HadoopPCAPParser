package com.spark.pcap;

import net.ripe.hadoop.pcap.packet.Packet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.spark.SparkConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import net.ripe.hadoop.pcap.io.PcapInputFormat;

import scala.Tuple2;

public class PCAPParser {

    private static void parse(String inputFile, String outputFile) {
        String base = "hdfs://hdp3-m1.dl:8020/user/suppala01/";

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("PCAPParser");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Configuration hadoopConf = sc.hadoopConfiguration();
        hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        JavaPairRDD<LongWritable, ObjectWritable> data = sc.hadoopFile(base+inputFile, PcapInputFormat.class,
                LongWritable.class, ObjectWritable.class);

        JavaPairRDD<String, String> lines = data.mapToPair(line -> {
            Object obj = line._2.get();
            Packet p = new Packet();
            if (obj instanceof Packet) {
                p = (Packet) obj;
            }
            return new Tuple2<>(p.get(Packet.TIMESTAMP).toString(), PacketUtils.packetToString(p));
        }).sortByKey();

        lines.saveAsTextFile(base+outputFile);
    }

    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFile = args[1];
        parse(inputFile, outputFile);
    }

}

