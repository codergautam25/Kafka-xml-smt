package xml.converter;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Custom Kafka Connect Sink Connector to convert JSON/Avro records to XML and write to a target topic.
 */
public class XmlConverterSinkConnector extends SinkConnector {

    private Map<String, String> configProperties;

    @Override
    public String version() {
        return "1.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        // Validate configuration on start
        new XmlConverterConfig(props);
        this.configProperties = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return XmlConverterSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> taskConfig = new HashMap<>(configProperties);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(taskConfig);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do for connector-level shutdown
    }

    @Override
    public ConfigDef config() {
        return XmlConverterConfig.CONFIG_DEF;
    }
}
