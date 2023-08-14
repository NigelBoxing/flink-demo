package org.karakarua;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;

public class CommonTest {
    @Test
    public void testPhisicalTypeAndLogicalType() {
        TableSchema resolvedSchema = TableSchema.builder().field("id", DataTypes.INT()).field("name", DataTypes.ROW()).build();
        DataType physicalRowDataType = resolvedSchema.toPhysicalRowDataType();
        LogicalType logicalType = physicalRowDataType.getLogicalType();
        System.out.println("physicalType = " + physicalRowDataType);
        System.out.println("logicalType = " + logicalType);

    }
}
