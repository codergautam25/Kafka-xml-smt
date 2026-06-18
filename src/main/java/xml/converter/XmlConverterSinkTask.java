package xml.converter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Task implementation for the XML Converter Sink Connector.
 * Consumes source records, serializes them to XML, and publishes to the target topic.
 */
public class XmlConverterSinkTask extends SinkTask {

    private XmlConverterConfig config;
    private Producer<byte[], String> producer;
    private String targetTopic;
    private String rootElementName;

    public XmlConverterSinkTask() {
        // Default constructor for Kafka Connect
    }

    // Visible for testing
    XmlConverterSinkTask(Producer<byte[], String> producer) {
        this.producer = producer;
    }

    @Override
    public String version() {
        return "1.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new XmlConverterConfig(props);
        this.targetTopic = config.getTargetTopic();
        this.rootElementName = config.getRootElementName();

        Map<String, Object> producerProps = new HashMap<>(config.getProducerConfigs());
        
        // Ensure bootstrap.servers is provided
        if (!producerProps.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) && this.producer == null) {
            throw new ConnectException("Missing required configuration: xml.producer.bootstrap.servers must be set for the internal producer.");
        }

        // Set default serializers if not explicitly overridden
        producerProps.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        if (this.producer == null) {
            try {
                this.producer = new KafkaProducer<>(producerProps);
            } catch (Exception e) {
                throw new ConnectException("Failed to initialize internal Kafka Producer for XML target topic: " + targetTopic, e);
            }
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        List<Future<RecordMetadata>> futures = new ArrayList<>(records.size());

        for (SinkRecord record : records) {
            // Convert the record payload value to XML string
            String xmlValue;
            try {
                xmlValue = XmlRecordWriter.convertToXml(record.value(), rootElementName);
            } catch (Exception e) {
                throw new SerializationException("Failed to convert record to XML. Source topic: " 
                        + record.topic() + ", partition: " + record.kafkaPartition() + ", offset: " + record.kafkaOffset(), e);
            }

            // Serialize the key as bytes (preserving null keys)
            byte[] serializedKey = null;
            if (record.key() != null) {
                if (record.key() instanceof byte[]) {
                    serializedKey = (byte[]) record.key();
                } else {
                    serializedKey = String.valueOf(record.key()).getBytes(StandardCharsets.UTF_8);
                }
            }

            // Build the ProducerRecord for target topic
            ProducerRecord<byte[], String> producerRecord = new ProducerRecord<>(
                    targetTopic, 
                    null, // Let Kafka partitioner assign target partition
                    record.timestamp(), 
                    serializedKey, 
                    xmlValue
            );

            // Copy original headers to preserve tracing and metadata
            if (record.headers() != null) {
                for (org.apache.kafka.connect.header.Header header : record.headers()) {
                    if (header.value() != null) {
                        if (header.value() instanceof byte[]) {
                            producerRecord.headers().add(header.key(), (byte[]) header.value());
                        } else {
                            producerRecord.headers().add(header.key(), String.valueOf(header.value()).getBytes(StandardCharsets.UTF_8));
                        }
                    }
                }
            }

            // Publish asynchronously (to take advantage of batching)
            futures.add(producer.send(producerRecord));
        }

        // Block and verify all writes in the batch succeeded before returning.
        // This ensures at-least-once delivery guarantee in Kafka Connect.
        for (Future<RecordMetadata> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                throw new ConnectException("Failed to write XML record to target topic: " + targetTopic, e);
            }
        }
    }

    @Override
    public void stop() {
        if (producer != null) {
            try {
                producer.close(Duration.ofSeconds(10));
            } catch (Exception e) {
                // Log and ignore close errors on shutdown
            }
        }
    }
}
