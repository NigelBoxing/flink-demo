package com.karakarua.deserialize;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

public class ProtobufDeserializer implements DeserializationSchema<RowData> {
    @Override
    public RowData deserialize(byte[] message) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }
}
