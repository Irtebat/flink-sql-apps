package irtebat.flink.utils;

import irtebat.flink.utils.KafkaConstants;

public class KafkaConfigUtil {

    private static String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }

    private static String getPlainLoginModuleClassName() {
        try {
            Class.forName("org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule");
            return "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule";
        } catch (ClassNotFoundException e) {
            return "org.apache.kafka.common.security.plain.PlainLoginModule";
        }
    }

    private static final String BOOTSTRAP_SERVERS =
            getEnvOrDefault(KafkaConstants.ENV_BOOTSTRAP_SERVERS, KafkaConstants.DEFAULT_BOOTSTRAP);
    private static final String SECURITY_PROTOCOL =
            getEnvOrDefault(KafkaConstants.ENV_SECURITY_PROTOCOL, KafkaConstants.DEFAULT_SECURITY_PROTOCOL);
    private static final String SASL_MECHANISM =
            getEnvOrDefault(KafkaConstants.ENV_SASL_MECHANISM, KafkaConstants.DEFAULT_SASL_MECHANISM);
    private static final String USERNAME =
            getEnvOrDefault(KafkaConstants.ENV_USERNAME, KafkaConstants.DEFAULT_USERNAME);
    private static final String PASSWORD =
            getEnvOrDefault(KafkaConstants.ENV_PASSWORD, KafkaConstants.DEFAULT_PASSWORD);
    private static final String GROUP_ID =
            getEnvOrDefault(KafkaConstants.ENV_GROUP_ID, KafkaConstants.DEFAULT_GROUP_ID);

    private static final String jaasConfig = getPlainLoginModuleClassName() +
            " required username=\"" + USERNAME + "\" password=\"" + PASSWORD + "\";";

    public static String getKafkaOptions(boolean includeGroupId) {
        StringBuilder sb = new StringBuilder();
        sb.append("'properties.bootstrap.servers' = '").append(BOOTSTRAP_SERVERS).append("',");
        sb.append("'properties.security.protocol' = '").append(SECURITY_PROTOCOL).append("',");

        if (!"PLAINTEXT".equalsIgnoreCase(SECURITY_PROTOCOL)) {
            sb.append("'properties.sasl.mechanism' = '").append(SASL_MECHANISM).append("',");
            sb.append("'properties.sasl.jaas.config' = '").append(jaasConfig).append("',");
        }

        if (includeGroupId) {
            sb.append("'properties.group.id' = '").append(GROUP_ID).append("',");
        }

        return sb.toString();
    }
}
