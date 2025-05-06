package irtebat.flink.apps;
import irtebat.flink.utils.archive.KafkaConfigUtil;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DatagenToKafkaJoinJob {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Datagen Source A
        tableEnv.executeSql(
                "CREATE TABLE DatagenSourceA (" +
                        " id INT," +
                        " name STRING," +
                        " PRIMARY KEY (id) NOT ENFORCED" +
                        ") WITH (" +
                        " 'connector' = 'datagen'," +
                        " 'rows-per-second' = '1'," +
                        " 'fields.id.kind' = 'random'," +
                        " 'fields.id.min' = '1'," +
                        " 'fields.id.max' = '100'," +
                        " 'fields.name.length' = '5'" +
                        ")"
        );

        // Datagen Source B
        tableEnv.executeSql(
                "CREATE TABLE DatagenSourceB (" +
                        " id INT," +
                        " description STRING," +
                        " PRIMARY KEY (id) NOT ENFORCED" +
                        ") WITH (" +
                        " 'connector' = 'datagen'," +
                        " 'rows-per-second' = '1'," +
                        " 'fields.id.kind' = 'random'," +
                        " 'fields.id.min' = '1'," +
                        " 'fields.id.max' = '100'," +
                        " 'fields.description.length' = '15'" +
                        ")"
        );

        // Kafka Source A
        tableEnv.executeSql(
                "CREATE TABLE KafkaSourceA (" +
                        " id INT," +
                        " name STRING," +
                        " PRIMARY KEY (id) NOT ENFORCED" +
                        ") WITH (" +
                        " 'connector' = 'upsert-kafka'," +
                        " 'topic' = 'flink-demo-source-A'," +
                        KafkaConfigUtil.getKafkaOptions(true) +
                        " 'key.format' = 'json'," +
                        " 'value.format' = 'json'," +
                        " 'key.json.ignore-parse-errors' = 'true'," +
                        " 'value.json.ignore-parse-errors' = 'true'," +
                        " 'value.json.fail-on-missing-field' = 'false'" +
                        ")"
        );

        // Kafka Source B
        tableEnv.executeSql(
                "CREATE TABLE KafkaSourceB (" +
                        " id INT," +
                        " description STRING," +
                        " PRIMARY KEY (id) NOT ENFORCED" +
                        ") WITH (" +
                        " 'connector' = 'upsert-kafka'," +
                        " 'topic' = 'flink-demo-source-B'," +
                        KafkaConfigUtil.getKafkaOptions(true) +
                        " 'key.format' = 'json'," +
                        " 'value.format' = 'json'," +
                        " 'key.json.ignore-parse-errors' = 'true'," +
                        " 'value.json.ignore-parse-errors' = 'true'," +
                        " 'value.json.fail-on-missing-field' = 'false'" +
                        ")"
        );

        // Final Sink
        tableEnv.executeSql(
                "CREATE TABLE FinalSink (" +
                        " id INT," +
                        " name STRING," +
                        " description STRING," +
                        " PRIMARY KEY (id) NOT ENFORCED" +
                        ") WITH (" +
                        " 'connector' = 'upsert-kafka'," +
                        " 'topic' = 'flink-demo-sink-topic'," +
                        KafkaConfigUtil.getKafkaOptions(false) +
                        " 'key.format' = 'json'," +
                        " 'value.format' = 'json'," +
                        " 'key.json.ignore-parse-errors' = 'true'," +
                        " 'value.json.ignore-parse-errors' = 'true'," +
                        " 'value.json.fail-on-missing-field' = 'false'" +
                        ")"
        );

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO KafkaSourceA SELECT * FROM DatagenSourceA");
        statementSet.addInsertSql("INSERT INTO KafkaSourceB SELECT * FROM DatagenSourceB");
        statementSet.addInsertSql(
                "INSERT INTO FinalSink " +
                        "SELECT a.id, UPPER(a.name), INITCAP(b.description) " +
                        "FROM KafkaSourceA a " +
                        "JOIN KafkaSourceB b ON a.id = b.id"
        );

        TableResult result = statementSet.execute();

        result.getJobClient().ifPresentOrElse(
                jobClient -> System.out.println("Job submitted with JobID: " + jobClient.getJobID()),
                () -> System.err.println("Job submission failed.")
        );
    }
}
