package com.tkh.flink.jobs;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import static com.tkh.flink.Environment.*;

public class PlayJob {
    public static void preExecution() {
        tEnv.executeSql("CREATE TABLE datagen (\n" +
                " f_random INT,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='10000',\n" +
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
        exeSet.addInsert("blackhole_table", tEnv.fromDataStream(dataOut));
    }
}
