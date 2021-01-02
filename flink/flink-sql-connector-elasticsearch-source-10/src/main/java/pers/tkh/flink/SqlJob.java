package pers.tkh.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;


public class SqlJob {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment env = StreamTableEnvironment.create(bsEnv, bsSettings);

        String path = args[0];
        StringBuilder stringBuilder = null;
        for (String line: Files.readAllLines(Paths.get(path))){
            if (line.startsWith("CREATE") || line.startsWith("INSERT INTO")){
                if (stringBuilder != null){
                    System.out.println(stringBuilder.toString());
                    env.sqlUpdate(stringBuilder.toString());
                }
                stringBuilder = new StringBuilder();
                stringBuilder.append(line);
            } else if (!line.startsWith("--") && stringBuilder != null) {
                stringBuilder.append(line);
            }
        }
        if (null != stringBuilder) {
            System.out.println(stringBuilder.toString());
            env.sqlUpdate(stringBuilder.toString());
        }
        env.execute(Paths.get(path).getFileName().toString().split("\\.")[0]);
    }
}
