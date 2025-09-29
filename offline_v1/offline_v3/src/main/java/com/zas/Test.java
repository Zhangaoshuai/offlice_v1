package com.zas;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test {
    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP", "hdfs");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("cdh01", 10265);
        dataStreamSource.print();
        env.execute();
    }
}
