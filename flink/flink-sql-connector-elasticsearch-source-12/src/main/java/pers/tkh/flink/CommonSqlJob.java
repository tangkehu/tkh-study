package pers.tkh.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CommonSqlJob {
    public static void main(String[] args) {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        fsEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
        fsTableEnv.executeSql("CREATE TABLE sourceTable (\n" +
                "  user_id STRING,\n" +
                "  user_name STRING,\n" +
                "  uv BIGINT,\n" +
                "  pv BIGINT,\n" +
                "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch',\n" +
                "  'hosts' = 'http://localhost:9200',\n" +
                "  'indices' = 'users',\n" +
                "  'types' = 'users',\n" +
                "  'slices' = '1',\n" +
                "  'format' = 'json'\n"+
                ")");
        fsTableEnv.executeSql("CREATE TABLE myUserTable (\n" +
                "  user_id STRING,\n" +
                "  user_name STRING,\n" +
                "  uv BIGINT,\n" +
                "  pv BIGINT,\n" +
                "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-6',\n" +
                "  'hosts' = 'http://localhost:9200',\n" +
                "  'index' = 'users',\n" +
                "  'document-type' = 'users',\n" +
                "  'format' = 'json'\n"+
                ")");
        fsTableEnv.executeSql("INSERT INTO myUserTable SELECT * FROM sourceTable");
    }
}
