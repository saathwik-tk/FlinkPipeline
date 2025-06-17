package org.learn;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello world!");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

//        DataStream<String> jsonStream = env.fromElements(
//                "{\"txn_id\": \"1\", \"txn_date\": \"2025-06-10\", \"payer\": \"Alice\", \"payee\": \"Bob\"}",
//                "{\"txn_id\": \"2\", \"txn_date\": \"2025-06-11\", \"payer\": \"Charlie\", \"payee\": \"David\"}"
//        );

//        jsonStream.print();

        Map<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), "s3a://beambucket/BeamFolder/");
        options.put(HoodieCommonConfig.HOODIE_FS_ATOMIC_CREATION_SUPPORT.key(), "s3a");
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");
        options.put(FlinkOptions.RECORD_KEY_FIELD.key(), "uuid");
        options.put(FlinkOptions.IGNORE_FAILED.key(), "true");
        options.put(FlinkOptions.PARTITION_PATH_FIELD.key(), "city");
        options.put(FlinkOptions.TABLE_NAME.key(), "UBER");
        options.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ); // Already likely set
        options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "true");  // Enable compaction
        options.put(FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(), "true"); // Schedule compaction
        options.put(FlinkOptions.COMPACTION_DELTA_COMMITS.key(), "1"); // Run after 1 delta commit



        DataStreamSource<RowData> dataStream = env.addSource(new SampleDataSource());
        dataStream.map(row -> {
            System.out.println("ROW: " + row.getString(1));
            return row;
        });

        HoodiePipeline.Builder builder = HoodiePipeline.builder("UBER")
                .column("ts TIMESTAMP(3)")
                .column("uuid VARCHAR(40)")
                .column("rider VARCHAR(20)")
                .column("driver VARCHAR(20)")
                .column("fare DOUBLE")
                .column("city VARCHAR(20)")
                .pk("uuid")
                .partition("city")
                .options(options);
        builder.sink(dataStream,false);

        env.execute("Flink Try");

    }
}