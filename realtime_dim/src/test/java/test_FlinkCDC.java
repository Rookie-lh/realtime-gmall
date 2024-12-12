import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class test_FlinkCDC {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                    .hostname("hadoop102")
                    .port(3306)
                    .databaseList("yk_ads") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
//                    .tableList("yourDatabaseName.yourTableName") // 设置捕获的表
                    .username("root")
                    .password("root")
                    .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                    .build();


        DataStreamSource<String> streamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqldata");
        streamSource.print();


        env.execute();
    }
}
