package org.learn;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class SampleDataSource implements SourceFunction<RowData> {
    private volatile boolean isRunning = true;
    public static DataType ROW_DATA_TYPE = DataTypes.ROW(
            DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)), // precombine field
            DataTypes.FIELD("uuid", DataTypes.VARCHAR(40)),// record key
            DataTypes.FIELD("rider", DataTypes.VARCHAR(20)),
            DataTypes.FIELD("driver", DataTypes.VARCHAR(20)),
            DataTypes.FIELD("fare", DataTypes.DOUBLE()),
            DataTypes.FIELD("city", DataTypes.VARCHAR(20)));

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        int batchNum = 0;
        while (isRunning) {
            batchNum++;
            List<RowData> dataSetInsert = DataGenerator.generateRandomRowData(ROW_DATA_TYPE);
            if (batchNum < 11) {
                // For first 10 batches, inserting 4 records. 2 with random id (INSERTS) and 2 with hardcoded UUID(UPDATE)
                for (RowData row : dataSetInsert) {
                    ctx.collect(row);
                }
            } else {
                // For 11th Batch, inserting only one record with row kind delete.
                RowData rowToBeDeleted = dataSetInsert.get(2);
                rowToBeDeleted.setRowKind(RowKind.DELETE);
                ctx.collect(rowToBeDeleted);
                TimeUnit.MILLISECONDS.sleep(10000);
                // Stop the stream once deleted
                isRunning = false;
            }
            TimeUnit.MILLISECONDS.sleep(1000); // Simulate a delay
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}