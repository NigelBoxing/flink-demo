package com.karakarua;

import com.karakarua.ProtobufOptions;
import com.karakarua.serialize.ProtobufEncodingFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.calcite.shaded.com.google.common.collect.Sets;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;

import java.util.Set;

public class ProtobufFormatFactory implements SerializationFormatFactory,DeserializationFormatFactory {

    private final static String IDENTIFIER = "protobuf";


    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this,formatOptions);
        String messageClassName = formatOptions.getOptional(ProtobufOptions.MESSAGE_CLASS_NAME).get();
        return new ProtobufEncodingFormat(messageClassName);
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return null;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = Sets.newHashSet();
        set.add(ProtobufOptions.MESSAGE_CLASS_NAME);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return null;
    }
}
