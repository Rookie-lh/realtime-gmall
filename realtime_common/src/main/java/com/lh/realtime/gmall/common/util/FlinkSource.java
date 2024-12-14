package com.lh.realtime.gmall.common.util;

import com.lh.realtime.gmall.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.util.Properties;

public class FlinkSource {

    //获取KafkaSource
    public static KafkaSource<String> getKafkaSource(String topic,String groupId){

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {return false;}

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                }).build();

        return kafkaSource;

    }

    // 获取MysqlSource

    public static MySqlSource<String> getMysqlSource(String database, String table){
        Properties prop = new Properties();
        prop.setProperty("useSSL","false");
        prop.setProperty("allowPublicKeyRetrieval","true");


        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList(Constant.DORIS_DATABASE)
                .tableList(database + "." + table)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(prop)
                .build();

        return mysqlSource;


    }


}
