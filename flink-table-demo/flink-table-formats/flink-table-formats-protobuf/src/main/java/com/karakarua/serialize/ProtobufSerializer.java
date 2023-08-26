package com.karakarua.serialize;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import com.google.protobuf.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class ProtobufSerializer implements SerializationSchema<RowData> {


    private String messageClassName;

    private LogicalType fieldsType;

    public ProtobufSerializer(String messageClassName, LogicalType fieldsType) {
        this.messageClassName = messageClassName;
        this.fieldsType = fieldsType;
    }

    @Override
    public byte[] serialize(RowData element) {
        try {
            Class<?> pbClass = Class.forName(messageClassName);
            Method newBuilderMethod = pbClass.getMethod("newBuilder");
            Message.Builder pbBuilder = (Message.Builder) newBuilderMethod.invoke(pbClass.newInstance());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
        return new byte[0];
    }
}
