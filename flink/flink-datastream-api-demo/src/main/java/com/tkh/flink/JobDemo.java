package com.tkh.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class JobDemo {
    public static void main(String[] args) throws Exception{
        Map<String, String> config = new HashMap<>();
        config.put("rest.port", "8081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(Configuration.fromMap(config));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(6);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

//        env.readTextFile("pom.xml")
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//                    @Override
//                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        for (String word : value.split(" ")) {
//                            if (!word.trim().isEmpty()) {
//                                out.collect(Tuple2.of(word.trim(), 1));
//                            }
//                        }
//                    }
//                })
//                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
//                    @Override
//                    public String getKey(Tuple2<String, Integer> value) throws Exception {
//                        return value.f0;
//                    }
//                })
//                .sum(1)
//                .print();

        tEnv.executeSql("CREATE TABLE datagen (\n" +
                " f_random INT,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='100000000',\n" +
                " 'fields.f_random.min'='1',\n" +
                " 'fields.f_random.max'='1000',\n" +
                " 'fields.f_random_str.length'='10'\n" +
                ")");
        tEnv.executeSql("CREATE TABLE blackhole_table (\n" +
                "  f0 INT,\n" +
                "  f1 INT\n" +
                ") WITH (\n" +
                "  'connector' = 'blackhole'\n" +
                ")");
        DataStream<Row> dataGen= tEnv.toAppendStream(tEnv.sqlQuery("select * from datagen"), Row.class);
        DataStream<Tuple2<Integer, Integer>> dataOut = dataGen
                .filter(new FilterFunction<Row>() {
                    @Override
                    public boolean filter(Row value) throws Exception {
                        Integer f0 = (Integer) value.getField(0);
                        return f0 == null || f0 < 100;
                    }
                })
                .map(new MapFunction<Row, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Row value) throws Exception {
                        return Tuple2.of((Integer) value.getField(0), 1);
                    }
                })
                .keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });

        tEnv.fromDataStream(dataOut).executeInsert("blackhole_table");
//        env.execute("demo");
    }
}
