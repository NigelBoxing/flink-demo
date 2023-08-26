package com.karakarua.deserialize;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class ProtobufDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {
    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType physicalDataType) {
        return null;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return null;
    }
}
