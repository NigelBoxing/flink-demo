import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

public class ProtobufStreamTest {
    @Test
    public void testProtobufSerialize(){
        RowData rowData = createRowData();

    }


    private RowData createRowData(){
        GenericRowData rowData = new GenericRowData(5);
        rowData.setField(1,"6f5a0abd8u9ty5");
        rowData.setField(2, System.currentTimeMillis());
        rowData.setField(3, System.currentTimeMillis() + 1000);
        return rowData;
    }

}
