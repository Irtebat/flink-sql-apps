package irtebat.flink.utils;

public final class KafkaConstants {

    private KafkaConstants() {
    }

    // Environment variable keys
    public static final String ENV_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS";
    public static final String ENV_SECURITY_PROTOCOL = "KAFKA_SECURITY_PROTOCOL";
    public static final String ENV_SASL_MECHANISM = "KAFKA_SASL_MECHANISM";
    public static final String ENV_USERNAME = "KAFKA_USERNAME";
    public static final String ENV_PASSWORD = "KAFKA_PASSWORD";
    public static final String ENV_GROUP_ID = "KAFKA_GROUP_ID";

    //Local Kafka Setup Default values
    public static final String DEFAULT_BOOTSTRAP = "localhost:9092";
    public static final String DEFAULT_SECURITY_PROTOCOL = "PLAINTEXT";
    public static final String DEFAULT_SASL_MECHANISM = "";
    public static final String DEFAULT_USERNAME = "";
    public static final String DEFAULT_PASSWORD = "";
    public static final String DEFAULT_GROUP_ID = "flink-poc-default-group-v1";

}
