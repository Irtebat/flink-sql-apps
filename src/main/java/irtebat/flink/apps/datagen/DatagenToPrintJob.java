package irtebat.flink.apps.datagen;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class DatagenToPrintJob {

	public static void main(String[] args) throws Exception {

		EnvironmentSettings settings = EnvironmentSettings
				.newInstance()
				.inStreamingMode()
				.build();

		TableEnvironment tableEnv = TableEnvironment.create(settings);

//		tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
//				.schema(Schema.newBuilder()
//						.column("f0", DataTypes.STRING())
//						.build())
//				.option("fields.f0.length", "10")
//				.option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
//				.build());

		tableEnv.executeSql(
				"CREATE TABLE SourceTable (" +
						" id INT," +
						" name STRING" +
						") WITH (" +
						" 'connector' = 'datagen'," +
						" 'rows-per-second' = '5'," +
						" 'fields.id.kind' = 'sequence'," +
						" 'fields.id.start' = '1'," +
						" 'fields.id.end' = '100'" +
						")"
		);

		tableEnv.executeSql(
				"CREATE TEMPORARY TABLE SinkTable " +
						"WITH ('connector' = 'print') " +
						"LIKE SourceTable (EXCLUDING OPTIONS)"
		);

		//		Table tableObj = tableEnv.from("SourceTable");
		Table tableObj = tableEnv.sqlQuery("SELECT * FROM SourceTable");

		TableResult tableResult = tableObj.executeInsert("SinkTable");

	}
}
