package irtebat.flink.apps.datagen;

import irtebat.flink.utils.archive.KafkaConfigUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DatagenToKafkaUpsertJob {

	public static void main(String[] args) throws Exception {

		// Set up the Table Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings
				.newInstance()
				.inStreamingMode()
				.build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
		// Enable checkpointing
		env.enableCheckpointing(30_000);
		// Set checkpoint storage
		env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");
		// Set up the Table Environment

		// Create Datagen Source Table
		tableEnv.executeSql(
				"CREATE TABLE DatagenSource (" +
						" id INT," +
						" name STRING," +
						" PRIMARY KEY (id) NOT ENFORCED" +
						") WITH (" +
						" 'connector' = 'datagen'," +
						" 'rows-per-second' = '1'," +
						" 'fields.id.kind' = 'random'," +
						" 'fields.id.min' = '1'," +
						" 'fields.id.max' = '100'," +
						" 'fields.name.length' = '10'" +
						")"
		);

		// Create Upsert Kafka Sink Table
		tableEnv.executeSql(
				"CREATE TABLE KafkaSink (" +
						" id INT," +
						" name STRING," +
						" PRIMARY KEY (id) NOT ENFORCED" +
						") WITH (" +
						" 'connector' = 'upsert-kafka'," +
						" 'topic' = 'flink-demo-source-topic'," +
						" 'properties.bootstrap.servers' = 'pkc-312o0.ap-southeast-1.aws.confluent.cloud:9092'," +
						" 'properties.security.protocol' = 'SASL_SSL'," +
						" 'properties.sasl.mechanism' = 'PLAIN'," +
						KafkaConfigUtil.getKafkaOptions(false) +
						" 'key.format' = 'json'," +
						" 'key.json.ignore-parse-errors' = 'true'," +
						" 'value.format' = 'json'," +
						" 'value.json.fail-on-missing-field' = 'false'," +
						" 'value.json.ignore-parse-errors' = 'true'" +
						")"
		);

		// Step 4: Insert into Kafka topic
		Table tableObj = tableEnv.sqlQuery("SELECT * FROM DatagenSource");
		TableResult result = tableObj.executeInsert("KafkaSink");

		result.getJobClient().ifPresentOrElse(
				jobClient -> {
					System.out.println("Job submitted with JobID: " + jobClient.getJobID());
				},
				() -> {
					System.err.println("Job submission failed.");
				}
		);

	}

}