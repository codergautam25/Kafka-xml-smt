package xml.converter;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Configuration definition for the XML Converter Sink Connector.
 */
public class XmlConverterConfig extends AbstractConfig {

    public static final String XML_TARGET_TOPIC_CONFIG = "xml.target.topic";
    private static final String XML_TARGET_TOPIC_DOC = "The name of the Kafka topic where XML converted records will be stored.";

    public static final String XML_ROOT_ELEMENT_CONFIG = "xml.root.element.name";
    private static final String XML_ROOT_ELEMENT_DOC = "The XML root element name for the generated XML records.";
    public static final String XML_ROOT_ELEMENT_DEFAULT = "record";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(XML_TARGET_TOPIC_CONFIG, 
                    ConfigDef.Type.STRING, 
                    ConfigDef.Importance.HIGH, 
                    XML_TARGET_TOPIC_DOC)
            .define(XML_ROOT_ELEMENT_CONFIG, 
                    ConfigDef.Type.STRING, 
                    XML_ROOT_ELEMENT_DEFAULT, 
                    ConfigDef.Importance.MEDIUM, 
                    XML_ROOT_ELEMENT_DOC);

    public XmlConverterConfig(Map<String, String> parsedConfig) {
        super(CONFIG_DEF, parsedConfig);
    }

    public String getTargetTopic() {
        return this.getString(XML_TARGET_TOPIC_CONFIG);
    }

    public String getRootElementName() {
        return this.getString(XML_ROOT_ELEMENT_CONFIG);
    }

    /**
     * Extracts properties starting with "xml.producer." to configure the internal Kafka Producer.
     */
    public Map<String, Object> getProducerConfigs() {
        return this.originalsWithPrefix("xml.producer.");
    }
}
