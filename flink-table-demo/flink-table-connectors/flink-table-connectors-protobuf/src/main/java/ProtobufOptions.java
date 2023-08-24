import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class ProtobufOptions {
    public static ConfigOption<String> MESSAGE_CLASS_NAME = ConfigOptions
            .key("message-class-name")
            .stringType()
            .noDefaultValue()
            .withDescription("pb serialize class name");
}
