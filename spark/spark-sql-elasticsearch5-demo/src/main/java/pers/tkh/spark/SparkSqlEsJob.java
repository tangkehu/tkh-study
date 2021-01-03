package pers.tkh.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.collection.mutable.HashMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * SparkSql写入ES的通用任务。
 */
public class SparkSqlEsJob {
    /**
     * main
     * @param args sql文件路径
     */
    public static void main(String[] args) throws IOException {
        // sql文件路径
        Path sqlPath = Paths.get(args[0]);
        // spark sql 环境配置
        SparkConf sparkConf = new SparkConf().setMaster("local[*]");
        sparkConf.setAppName(sqlPath.getFileName().toString().split("\\.")[0]);
        // 设置默认的日期格式类型为String来避开潜在的日期类型转换错误
        sparkConf.set("es.mapping.date.rich", "false");
        // 优化从ES读取的速度和稳定性的配置
        sparkConf.set("es.scroll.size", "20000");
        sparkConf.set("es.http.timeout", "3m");
        sparkConf.set("es.http.retries", "5");
        // 优化写入ES的速度和稳定性的配置
        sparkConf.set("es.batch.size.bytes", "2mb");
        sparkConf.set("es.batch.size.entries", "20000");
        sparkConf.set("es.batch.write.refresh", "false");
        sparkConf.set("es.batch.write.retry.count", "5");
        sparkConf.set("es.batch.write.retry.wait", "30s");
        // 创建spark sql 环境
        SQLContext sqlContext = new SQLContext(new JavaSparkContext(sparkConf));
        // 执行sql取得输出数据
        DataFrame sqlOut = null;
        String indexOut = null;
        final String[] nodesOut = new String[1];
        for (String sql : getSql(sqlPath)) {
            System.out.println(sql);
            if (sql.startsWith("SINK")) {
                nodesOut[0] = sql.split(" ")[1];
                indexOut = sql.split(" ")[2];
            } else {
                sqlOut = sqlContext.sql(sql);
            }
        }
        if (nodesOut[0] == null || indexOut == null || sqlOut == null) {
            throw new IOException("输出的ES地址或索引或数据为空");
        }
        // 将输出写入ES
        EsSparkSQL.saveToEs(sqlOut, indexOut, new HashMap<String, String>(){{put("es.resource.write", nodesOut[0]);}});
    }

    /**
     * 从Sql文件获取sql语句，因为spark sql 只支持一句一句执行sql
     * @param path sql文件路径
     * @return sql语句数组
     */
    private static String[] getSql(Path path) throws IOException {
        List<String> sqlList = new ArrayList<>();
        StringBuilder sqlBuilder = null;
        for (String line : Files.readAllLines(path)) {
            if (line.startsWith("CREATE") || line.startsWith("SELECT") || line.startsWith("SINK")) {
                if (sqlBuilder != null) {
                    sqlList.add(sqlBuilder.toString());
                }
                sqlBuilder = new StringBuilder();
                sqlBuilder.append(line).append(" ");
            } else if (!line.isEmpty() && sqlBuilder != null && !line.startsWith("--") && !line.startsWith("//")) {
                sqlBuilder.append(line).append(" ");
            }
        }
        if (sqlBuilder != null) {
            sqlList.add(sqlBuilder.toString());
        }
        return sqlList.toArray(new String[0]);
    }
}
