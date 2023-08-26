package com.karakarua.serialize;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

public class ProtobufEncodingFormat implements EncodingFormat<SerializationSchema<RowData>> {

    public String messageClassName;

    public ProtobufEncodingFormat(String messageClassName) {
        this.messageClassName = messageClassName;
    }

    @Override
    public SerializationSchema<RowData> createRuntimeEncoder(DynamicTableSink.Context context, DataType physicalDataType) {
        LogicalType fieldsType = physicalDataType.getLogicalType();
        return new ProtobufSerializer(messageClassName,fieldsType);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }
}
