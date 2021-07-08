package com.tkh.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.HashMap;
import java.util.Map;

public class Environment {
    public static Map<String, String> config = new HashMap<String, String>(){{
        put("rest.port", "8081");
    }};
    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(Configuration.fromMap(config));
    public static StreamTableEnvironment tEnv = StreamTableEnvironment.create(
            env,
            EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    );
    public static StatementSet exeSet = tEnv.createStatementSet();

}
