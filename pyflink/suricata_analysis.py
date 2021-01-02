"""
统计suricata的dns日志数据
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import Kafka, Elasticsearch, Json, Schema
from pyflink.table.types import DataTypes
from pyflink.table.udf import udf


class SuricataAnalysis(object):
    """ Suricata日志分析 """

    def __init__(self):
        # 创建flink执行环境
        self.host = "localhost"  # 配置kafka,zookeeper,es的地址
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        self.env = StreamTableEnvironment.create(
            env,
            environment_settings=EnvironmentSettings.new_instance().use_blink_planner().build())
        self._stat_domain_ip()
        self._dga_detect()
        self._dubious_dns_type()
        self._dubious_http_method()
        self._dubious_http_login()
        self._dubious_ssh_flux()

    def _kafka_connector(self, topic, name, schema):
        self.env.connect(Kafka().version("universal").topic(topic).start_from_earliest()
                         .property("zookeeper.connect", self.host+":2181")
                         .property("bootstrap.servers", self.host+":9092")) \
            .with_format(Json().fail_on_missing_field(True).derive_schema()) \
            .with_schema(schema).in_append_mode().create_temporary_table(name)

    def _es_connector(self, index, name, schema):
        self.env.connect(Elasticsearch().version("7").index(index).document_type("_doc").bulk_flush_max_actions(1)
                         .host(self.host, "9200", "http")) \
            .with_format(Json().fail_on_missing_field(True).derive_schema()) \
            .with_schema(schema).in_upsert_mode().create_temporary_table(name)

    def _dubious_ssh_flux(self):
        self._kafka_connector("suricata_ssh", "_dubious_ssh_flux_it", Schema()
                              .field("event_time", DataTypes.STRING())
                              .field("flow_id", DataTypes.BIGINT()))
        self._es_connector("analysis_behaviour", "_dubious_ssh_flux_ot", Schema()
                           .field("event_time", DataTypes.STRING())
                           .field("flow_id", DataTypes.BIGINT())
                           .field("behave_type", DataTypes.STRING()))
        self.env.from_path("_dubious_ssh_flux_it").add_columns("'ssh_flux' as behave_type")\
            .insert_into("_dubious_ssh_flux_ot")

    def _dubious_http_login(self):
        self._kafka_connector("suricata_http", "dubious_http_login_it", Schema()
                              .field("event_time", DataTypes.STRING())
                              .field("flow_id", DataTypes.BIGINT())
                              .field("http", DataTypes.ROW([DataTypes.FIELD("url", DataTypes.STRING()),
                                                            DataTypes.FIELD("hostname", DataTypes.STRING())])))
        self._es_connector("analysis_behaviour", "dubious_http_login_ot", Schema()
                           .field("event_time", DataTypes.STRING())
                           .field("flow_id", DataTypes.BIGINT())
                           .field("hostname", DataTypes.STRING())
                           .field("url", DataTypes.STRING())
                           .field("behave_type", DataTypes.STRING()))
        self.env.sql_query(
            "select event_time, flow_id, http.hostname as hostname, http.url as url "
            "from dubious_http_login_it where http.url like '%login%'"
        ).add_columns("'dubious_http_login' as behave_type").insert_into("dubious_http_login_ot")

    def _dubious_http_method(self):
        self._kafka_connector("suricata_http", "dubious_http_method_it", Schema()
                              .field("event_time", DataTypes.STRING())
                              .field("flow_id", DataTypes.BIGINT())
                              .field("http", DataTypes.ROW([DataTypes.FIELD("http_method", DataTypes.STRING()),
                                                            DataTypes.FIELD("hostname", DataTypes.STRING())])))
        self._es_connector("analysis_behaviour", "dubious_http_method_ot", Schema()
                           .field("event_time", DataTypes.STRING())
                           .field("flow_id", DataTypes.BIGINT())
                           .field("hostname", DataTypes.STRING())
                           .field("http_method", DataTypes.STRING())
                           .field("behave_type", DataTypes.STRING()))
        self.env.sql_query(
            "select event_time, flow_id, http.hostname as hostname, http.http_method as http_method "
            "from dubious_http_method_it where http.http_method in('HEAD','OPTIONS','TRACE','CONNECT')"
        ).add_columns("'dubious_http_method' as behave_type").insert_into("dubious_http_method_ot")

    def _dubious_dns_type(self):
        self._kafka_connector("suricata_dns", "dubious_dns_type_it", Schema()
                              .field("event_time", DataTypes.STRING())
                              .field("flow_id", DataTypes.BIGINT())
                              .field('dns', DataTypes.ROW([
                                DataTypes.FIELD("rrname", DataTypes.STRING()),
                                DataTypes.FIELD("rrtype", DataTypes.STRING())])))
        self._es_connector("analysis_behaviour", "dubious_dns_type_ot", Schema()
                           .field("event_time", DataTypes.STRING())
                           .field("flow_id", DataTypes.BIGINT())
                           .field("rrname", DataTypes.STRING())
                           .field("rrtype", DataTypes.STRING())
                           .field("behave_type", DataTypes.STRING()))
        self.env.sql_query(
            "select event_time, flow_id, dns.rrname as rrname, dns.rrtype as rrtype from dubious_dns_type_it "
            "where dns.rrtype = 'TXT'"
        ).add_columns("'dubious_dns_type' as behave_type").insert_into("dubious_dns_type_ot")

    def _dga_detect(self):
        """对域名进行dga检测"""
        import numpy as np
        from keras.models import load_model
        from keras.preprocessing import sequence

        mapping = {u'$': 84, u'(': 89, u',': 63, u'0': 53, u'4': 57, u'8': 61, u'<': 64,
                   u'@': 82, u'D': 30, u'H': 34, u'L': 38, u'P': 42, u'T': 46, u'X': 50,
                   u'\\': 77, u'`': 79, u'd': 4, u'h': 8, u'l': 12, u'p': 16, u't': 20,
                   u'x': 24, u'|': 78, u'#': 83, u"'": 71, u'+': 94, u'/': 67, u'3': 56,
                   u'7': 60, u';': 69,
                   u'?': 68, u'C': 29, u'G': 33, u'K': 37, u'O': 41, u'S': 45, u'W': 49,
                   u'[': 73, u'_': 92, u'c': 3, u'g': 7, u'k': 11, u'o': 15, u's': 19,
                   u'w': 23, u'{': 74, u'"': 72,
                   u'&': 87, u'*': 88, u'.': 65, u'2': 55, u'6': 59, u':': 70, u'>': 66,
                   u'B': 28, u'F': 32, u'J': 36,
                   u'N': 40, u'R': 44, u'V': 48, u'Z': 52, u'^': 86, u'b': 2, u'f': 6,
                   u'j': 10, u'n': 14, u'r': 18,
                   u'v': 22, u'z': 26, u'~': 80, u'!': 81, u'%': 85, u')': 90, u'-': 91,
                   u'1': 54, u'5': 58, u'9': 62,
                   u'=': 93, u'A': 27, u'E': 31, u'I': 35, u'M': 39, u'Q': 43, u'U': 47,
                   u'Y': 51, u']': 75, u'a': 1,
                   u'e': 5, u'i': 9, u'm': 13, u'q': 17, u'u': 21, u'y': 25, u'}': 76}
        domain_type = {
            0: 'bamital', 1: 'banjori', 2: 'benign', 3: 'blackhole', 4: 'ccleaner',
            5: 'chinad', 6: 'conficker', 7: 'cryptolocker', 8: 'dircrypt', 9: 'dyre',
            10: 'emotet', 11: 'feodo', 12: 'fobber', 13: 'gameover', 14: 'gspy',
            15: 'locky', 16: 'madmax', 17: 'matsnu', 18: 'mirai', 19: 'murofet',
            20: 'mydoom', 21: 'necurs', 22: 'nymaim', 23: 'omexo', 24: 'padcrypt',
            25: 'proslikefan', 26: 'pykspa', 27: 'pykspa_v1', 28: 'pykspa_v2_fake',
            29: 'pykspa_v2_real', 30: 'qadars', 31: 'ramnit', 32: 'ranbyus',
            33: 'rovnix', 34: 'shifu', 35: 'shiotob', 36: 'simda', 37: 'suppobox',
            38: 'symmi', 39: 'tempedreve', 40: 'tinba', 41: 'tinynuke', 42: 'tofsee',
            43: 'vawtrak', 44: 'vidro', 45: 'virut', 46: 'xshellghost'
        }
        model = load_model('multi_train_model.h5')
        self._kafka_connector("suricata_dns", "dga_source_tb", Schema()
                              .field("event_time", DataTypes.STRING())
                              .field("flow_id", DataTypes.BIGINT())
                              .field('dns', DataTypes.ROW([DataTypes.FIELD("rrname", DataTypes.STRING())])))
        self._es_connector("analysis_behaviour", "dga_sink_tb", Schema()
                           .field("event_time", DataTypes.STRING())
                           .field("flow_id", DataTypes.BIGINT())
                           .field('domain', DataTypes.STRING())
                           .field('risk_type', DataTypes.STRING())
                           .field('possibility', DataTypes.FLOAT())
                           .field("behave_type", DataTypes.CHAR(8)))

        @udf(input_types=[DataTypes.STRING()],
             result_type=DataTypes.ARRAY(DataTypes.STRING()))
        def dga_detect(domain):
            data = [[mapping[char] for char in domain]]
            predict_data = sequence.pad_sequences(np.array(data), maxlen=48)
            results = model.predict(predict_data, batch_size=128)
            result = results[0]
            return [domain, domain_type[np.where(result == result.max())[0][0]], str(float(result.max()))]

        @udf(input_types=[DataTypes.ARRAY(DataTypes.STRING()), DataTypes.INT()],
             result_type=DataTypes.STRING())
        def get(array, index):
            return array[index]

        self.env.register_function('dga', dga_detect)
        self.env.register_function("get", get)
        self.env\
            .sql_query("select event_time, flow_id, dga(dns.rrname) as dga from dga_source_tb")\
            .select("event_time, flow_id, "
                    "get(dga, 0) as domain, "
                    "get(dga, 1) as risk_type, "
                    "get(dga, 2).cast(Float) as possibility")\
            .add_columns("'dga_flux' as behave_type").where("risk_type <> 'benign'").insert_into("dga_sink_tb")

    def _stat_domain_ip(self):
        """统计suricata的dns日志数据中域名的所有解析IP"""
        self._kafka_connector("suricata_dns_answer", "dns_answer_tb", Schema()
                              .field("dns", DataTypes.ROW([
                                DataTypes.FIELD("rrname", DataTypes.STRING()),
                                DataTypes.FIELD("grouped", DataTypes.ROW([
                                    DataTypes.FIELD("A", DataTypes.ARRAY(DataTypes.STRING()))
                                ]))])))
        self._es_connector("statistics_domain_ip", "statistics_domain_ip_tb", Schema()
                           .field("param", DataTypes.STRING())
                           .field("records", DataTypes.MULTISET(DataTypes.STRING())))

        # 消费dns应答数据，生成域名和解析IP的对应关表
        self.env.create_temporary_view("domain_ip_1_view", self.env.sql_query(  # 获取基本数据
            "select dns.rrname as rrname, dns.grouped.A as ips from dns_answer_tb"))
        self.env.create_temporary_view("domain_ip_2_view", self.env.sql_query(  # 展平ip数组
            "SELECT rrname, ip FROM domain_ip_1_view CROSS JOIN UNNEST(ips) AS ips (ip)"))
        self.env.create_temporary_view("domain_ip_3_view", self.env.sql_query(  #
            "SELECT rrname as param, ip as record FROM domain_ip_2_view UNION ALL "
            "SELECT ip as param, rrname as record FROM domain_ip_2_view"))
        self.env.sql_query(  # 统计域名对应的所有解析IP
            "SELECT param, COLLECT (record) as records FROM domain_ip_3_view GROUP BY param "
        ).insert_into("statistics_domain_ip_tb")

    def run(self):
        # 执行flink任务
        self.env.execute("suricata_analysis")


if __name__ == "__main__":
    SuricataAnalysis().run()
