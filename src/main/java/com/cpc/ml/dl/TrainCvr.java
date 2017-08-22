package com.cpc.ml.dl;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Created by roydong on 22/08/2017.
 */
public class TrainCvr {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .config("spark.driver.maxResultSize", "20G")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", "org.nd4j.Nd4jRegistrator")
                .config("spark.kryoserializer.buffer.max", "2047MB")
                .config("spark.rpc.message.maxSize", "400")
                .config("spark.network.timeout", "240s")
                //.config("spark.speculation", "true")
                .config("spark.storage.blockManagerHeartBeatMs", "300000")
                .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "100")
                .config("spark.core.connection.auth.wait.timeout", "100")
                .appName("dl train cvr")
                .getOrCreate();

    }

}
