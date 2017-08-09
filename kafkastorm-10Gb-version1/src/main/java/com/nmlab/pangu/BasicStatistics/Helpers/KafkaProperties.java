package main.java.com.nmlab.pangu.BasicStatistics.Helpers;

public interface KafkaProperties
{
    final static String zkConnect = "45.32.84.75:12181,45.32.70.165:12181,45.32.83.185:12181";
    final static String groupId = "test-consumer-group";
    final static String topic = "topic2";
    final static String kafkaServerURL = "45.32.84.75";
    final static int kafkaServerPort = 19092;
    final static int kafkaProducerBufferSize = 64 * 1024;
    final static int connectionTimeOut = 20000;
    final static int reconnectInterval = 10000;
    final static String topic2 = "topic2";
    final static String topic3 = "topic3";
    final static String clientId = "SimpleConsumerDemoClient"; //此处不知道怎么改
}
