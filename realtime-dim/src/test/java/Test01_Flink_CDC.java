import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01_Flink_CDC {
    public static void main(String[] args) throws Exception {

        // 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 并行度
        env.setParallelism(4);

        // 使用FlinkCDC读取MySQL数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("yk_ads") // set captured database
//                .tableList("yk_ads.ads") // set captured table
                .username("root")
                .password("root")
                .startupOptions(StartupOptions.earliest())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();



        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print();
        env.execute();


    }
}
