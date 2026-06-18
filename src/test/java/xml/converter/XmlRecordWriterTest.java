package xml.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class XmlRecordWriterTest {

    private static final String DECLARATION = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";

    @Test
    public void testNullValue() {
        String xml = XmlRecordWriter.convertToXml(null, "user");
        assertEquals(DECLARATION + "<user/>", xml);
    }

    @Test
    public void testPrimitiveValue() {
        String xml = XmlRecordWriter.convertToXml("John Doe", "name");
        assertEquals(DECLARATION + "<name>John Doe</name>", xml);
    }

    @Test
    public void testMapSerialization() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("id", 123);
        data.put("name", "Alice");
        data.put("active", true);

        String xml = XmlRecordWriter.convertToXml(data, "user");
        String expected = DECLARATION + "<user><id>123</id><name>Alice</name><active>true</active></user>";
        assertEquals(expected, xml);
    }

    @Test
    public void testStructSerialization() {
        Schema addressSchema = SchemaBuilder.struct()
                .field("city", Schema.STRING_SCHEMA)
                .field("zip", Schema.STRING_SCHEMA)
                .build();

        Schema userSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("address", addressSchema)
                .build();

        Struct address = new Struct(addressSchema)
                .put("city", "New York")
                .put("zip", "10001");

        Struct user = new Struct(userSchema)
                .put("id", 456)
                .put("name", "Bob")
                .put("address", address);

        String xml = XmlRecordWriter.convertToXml(user, "customer");
        
        String expected = DECLARATION + "<customer>" +
                "<id>456</id>" +
                "<name>Bob</name>" +
                "<address><city>New York</city><zip>10001</zip></address>" +
                "</customer>";
                
        assertEquals(expected, xml);
    }

    @Test
    public void testCollectionSerialization() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("id", 789);
        data.put("tags", Arrays.asList("kafka", "xml", "connect"));

        String xml = XmlRecordWriter.convertToXml(data, "project");
        String expected = DECLARATION + "<project>" +
                "<id>789</id>" +
                "<tags>kafka</tags>" +
                "<tags>xml</tags>" +
                "<tags>connect</tags>" +
                "</project>";
        assertEquals(expected, xml);
    }

    @Test
    public void testByteArraySerialization() {
        Map<String, Object> data = new LinkedHashMap<>();
        byte[] payload = "Hello Kafka".getBytes();
        data.put("data", payload);

        String xml = XmlRecordWriter.convertToXml(data, "payload");
        
        // Base64 encoding of "Hello Kafka" is "SGVsbG8gS2Fma2E="
        String expected = DECLARATION + "<payload><data>SGVsbG8gS2Fma2E=</data></payload>";
        assertEquals(expected, xml);
    }

    @Test
    public void testXmlEscaping() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("escaped", "John & Bob <alice> \"quotes\" 'single'");

        String xml = XmlRecordWriter.convertToXml(data, "root");
        String expected = DECLARATION + "<root><escaped>John &amp; Bob &lt;alice&gt; &quot;quotes&quot; &apos;single&apos;</escaped></root>";
        assertEquals(expected, xml);
    }

    @Test
    public void testTagNameSanitization() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("1_invalid_leading_number", "val1");
        data.put("invalid space tag", "val2");
        data.put("valid-hyphen.period_underscore", "val3");

        String xml = XmlRecordWriter.convertToXml(data, "root-node");
        String expected = DECLARATION + "<root-node>" +
                "<_1_invalid_leading_number>val1</_1_invalid_leading_number>" +
                "<invalid_space_tag>val2</invalid_space_tag>" +
                "<valid-hyphen.period_underscore>val3</valid-hyphen.period_underscore>" +
                "</root-node>";
        assertEquals(expected, xml);
    }

    @Test
    public void testNullFieldsInStructAndMap() {
        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        Struct struct = new Struct(schema)
                .put("name", null)
                .put("age", 25);

        String xmlStruct = XmlRecordWriter.convertToXml(struct, "user");
        String expectedStruct = DECLARATION + "<user><name/><age>25</age></user>";
        assertEquals(expectedStruct, xmlStruct);

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", null);
        map.put("age", 25);

        String xmlMap = XmlRecordWriter.convertToXml(map, "user");
        String expectedMap = DECLARATION + "<user><name/><age>25</age></user>";
        assertEquals(expectedMap, xmlMap);
    }
}
