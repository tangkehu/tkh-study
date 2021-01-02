//package pers.tkh.flink;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.Requests;
//
//import java.util.*;
//
//public class FlinkDemo {
//    public static void main(String[] args) throws Exception{
//        // 获取flink执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 配置kafka输入
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties);
//        flinkKafkaConsumer.setStartFromEarliest();
//        DataStream<String> source = env.addSource(flinkKafkaConsumer).name("kafka");
//
//        // 编写数据处理任务
//        DataStream<String > transformation = source.map(new MapFunction<String, String>() {
//            public String map(String s) throws Exception {
//                System.out.println(s);
//                return "flink-demo: " + s;
//            }
//        }) ;
//
//        // 配置es输出
//        List<HttpHost> httpHosts = new ArrayList<HttpHost>();
//        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
//        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<String>(
//                httpHosts,
//                new ElasticsearchSinkFunction<String>() {
//                    private IndexRequest createIndexRequest(String element) {
//                        Map<String, String> json = new HashMap<String, String>();
//                        json.put("data", element);
//                        return Requests.indexRequest()
//                                .index("flink-demo")
//                                .source(json);
//                    }
//
//                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
//                        indexer.add(createIndexRequest(element));
//                    }
//                }
//        );
//        esSinkBuilder.setBulkFlushMaxActions(1);
//        transformation.addSink(esSinkBuilder.build()).name("elasticsearch");
//
//        // 执行任务
//        env.execute("FlinkDemo");
//    }
//}
