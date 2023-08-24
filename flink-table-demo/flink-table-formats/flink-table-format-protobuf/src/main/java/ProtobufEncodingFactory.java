import org.apache.flink.calcite.shaded.com.google.common.collect.Sets;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

import java.util.Set;

public class ProtobufEncodingFactory implements DynamicTableSinkFactory {

    private final static String IDENTIFIER = "protobuf";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        //
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
