package com.lh.realtime.gmall.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lh.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class DImApp {
    public static void main(String[] args) throws Exception {

        // TODO  1.准备基本环境
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);
        // TODO  2.检查点相关设置
        // 2.1 开启检查点
        // 默认EXACTLY_ONCE 精准一次
        // AT_LEAST_ONE    至少一次
      /*
        涉及到berry对齐底层？
       原理：当到了检查点时间触发时间，检查点协调器会给source发送检查点分界线，就是berry,
       当source接入到berry会对source就行一个备份,随着流的   传递，berry也向下游传递，
       当算子接触到berry后，算子的状态也会收到备份
       */


//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        // 2.2 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        // 2.3 设置job取消后检查点是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // 2.4 设置两个检查点之间最小间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        // 2.5 设置重启策略
//        // 第一个参数重启多少次   第二个参数  多长时间重启一次
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
//        // 第二个参数  相当于技术周期  30天重启三次
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
//        // 2.6 设置状态后端以及检查点的存储路径
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
//        // 2.7 设置操作用户Hadoop102
//        System.setProperty("HADOOP_USER_NAME","root");
        // TODO  3.从kafka的topic主题取数据
        // 3.1 声明消费的主题以及消费者组
        String topic_group = "dim_app_group";

        // 3.2 创建消费者对象
        KafkaSource<String> kafkasource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_DB)
                .setGroupId(topic_group)
                // 为了保证消费的精准一致性

                .setStartingOffsets(OffsetsInitializer.earliest())
                // 如果使用Flink提供的SimpleStringSchema对String进行反序列化，如果消息为空，会报错
                // 自定义序列化
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if( message != null){
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        // 3.3 消费数据，封装为流

        DataStreamSource<String> kafkaStrDs =
                env.fromSource(kafkasource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        // TODO  4.对业务数据类型进行转换 做个简单的ETL jsonStr -> jsonObject
        SingleOutputStreamOperator<JSONObject> KafkaJsonObj = kafkaStrDs.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);

                        String db = jsonObject.getString("database");
                        String type = jsonObject.getString("type");
                        String data = jsonObject.getString("data");


                        if("realtime_v1".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2){
                            out.collect(jsonObject);
                        }

                    }
                }
        );

        KafkaJsonObj.print();

        // TODO  5.使用FlinkCDC读取配置表信息
        // 5.1 创建MySQL对象
        // 5.2 读取数据，封装为流
        // TODO  6.对配置流中的数据类型进行转换  jsonStr -> jsonObject
        // TODO  7.根据配置表中的配置信息进行建表等操作
        // TODO  8.将配置信息进行广播 -- broadcast
        // TODO  9.将业务数据和广播流配置信息进行关联
        // TODO  10.处理关联后的数据(判断是否为维度)
        // TODO  11.将维度数据同步到Hbase中

        env.execute();

    }
}
