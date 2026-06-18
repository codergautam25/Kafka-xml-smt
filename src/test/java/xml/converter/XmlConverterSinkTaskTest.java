package xml.converter;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class XmlConverterSinkTaskTest {

    private Producer<byte[], String> mockProducer;
    private XmlConverterSinkTask task;
    private Map<String, String> validConfig;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() {
        mockProducer = mock(Producer.class);
        task = new XmlConverterSinkTask(mockProducer);

        validConfig = new HashMap<>();
        validConfig.put("xml.target.topic", "xml-target-topic");
        validConfig.put("xml.root.element.name", "record");
        validConfig.put("xml.producer.bootstrap.servers", "localhost:9092");
    }

    @Test
    public void testStartMissingBootstrapServers() {
        Map<String, String> invalidConfig = new HashMap<>();
        invalidConfig.put("xml.target.topic", "xml-target-topic");

        // The default constructor-based task should fail if no bootstrap servers are provided
        XmlConverterSinkTask defaultTask = new XmlConverterSinkTask();
        assertThrows(ConnectException.class, () -> {
            defaultTask.start(invalidConfig);
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPutMultipleRecords() throws InterruptedException, ExecutionException {
        task.start(validConfig);

        // Mock the producer send future
        Future<RecordMetadata> mockFuture = mock(Future.class);
        RecordMetadata metadata = mock(RecordMetadata.class);
        when(mockFuture.get()).thenReturn(metadata);
        when(mockProducer.send(any(ProducerRecord.class))).thenReturn(mockFuture);

        // Create mock records
        List<SinkRecord> records = new ArrayList<>();
        
        // Record 1: Simple key-value
        Map<String, Object> val1 = new HashMap<>();
        val1.put("name", "John");
        records.add(new SinkRecord("source-topic", 0, 
                Schema.STRING_SCHEMA, "key-1", 
                null, val1, 
                100L));

        // Record 2: Record with headers
        Map<String, Object> val2 = new HashMap<>();
        val2.put("name", "Alice");
        SinkRecord record2 = new SinkRecord("source-topic", 0, 
                Schema.STRING_SCHEMA, "key-2", 
                null, val2, 
                200L);
        record2.headers().addString("span-id", "12345");
        records.add(record2);

        // Invoke put
        task.put(records);

        // Capture sent records
        ArgumentCaptor<ProducerRecord<byte[], String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer, times(2)).send(captor.capture());

        List<ProducerRecord<byte[], String>> sentRecords = captor.getAllValues();
        assertEquals(2, sentRecords.size());

        // Assert record 1
        ProducerRecord<byte[], String> sent1 = sentRecords.get(0);
        assertEquals("xml-target-topic", sent1.topic());
        assertEquals("key-1", new String(sent1.key(), StandardCharsets.UTF_8));
        assertTrue(sent1.value().contains("<name>John</name>"));

        // Assert record 2 and headers
        ProducerRecord<byte[], String> sent2 = sentRecords.get(2 - 1);
        assertEquals("xml-target-topic", sent2.topic());
        assertEquals("key-2", new String(sent2.key(), StandardCharsets.UTF_8));
        assertTrue(sent2.value().contains("<name>Alice</name>"));
        assertNotNull(sent2.headers().lastHeader("span-id"));
        assertEquals("12345", new String(sent2.headers().lastHeader("span-id").value(), StandardCharsets.UTF_8));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProducerExceptionPropagates() throws InterruptedException, ExecutionException {
        task.start(validConfig);

        // Mock producer.send to throw an exception on future.get()
        Future<RecordMetadata> mockFuture = mock(Future.class);
        when(mockFuture.get()).thenThrow(new ExecutionException(new RuntimeException("Kafka connection lost")));
        when(mockProducer.send(any(ProducerRecord.class))).thenReturn(mockFuture);

        SinkRecord record = new SinkRecord("source-topic", 0, 
                Schema.STRING_SCHEMA, "key", 
                null, "payload", 
                100L);

        // Assert that the write failure is propagated as a ConnectException
        assertThrows(ConnectException.class, () -> {
            task.put(Collections.singletonList(record));
        });
    }

    @Test
    public void testStopClosesProducer() {
        task.start(validConfig);
        task.stop();
        verify(mockProducer, times(1)).close(any());
    }
}
