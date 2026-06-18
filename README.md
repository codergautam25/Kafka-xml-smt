# Kafka Connect XML Converter Sink Connector

A high-performance, lightweight Kafka Connect Sink Connector that consumes records from a Kafka topic (serialized as Avro or JSON), translates the payloads (Struct or Map) into XML format, and publishes the serialized XML records to a target Kafka topic.

---

## Architectural Design

The connector acts as a bridge within the Kafka Connect ecosystem. It separates deserialization of incoming formats (Avro, JSON, Protobuf) from the XML generation and target publishing phases.

```mermaid
graph TD
    %% Define Styles
    classDef kafka fill:#FFFAF0,stroke:#D2691E,stroke-width:2px;
    classDef connect fill:#F0F8FF,stroke:#4682B4,stroke-width:2px;
    classDef logic fill:#F9F0FF,stroke:#8A2BE2,stroke-width:2px;

    subgraph Kafka_Cluster["Kafka Cluster"]
        A["Source Topic<br/>(Avro/JSON/Protobuf)"]:::kafka
        B["Target XML Topic<br/>(XML Messages)"]:::kafka
    end

    subgraph Connect_Worker["Kafka Connect Worker Process"]
        C["Kafka Connect Engine"]:::connect
        D["Deserialization Converter<br/>(AvroConverter / JsonConverter)"]:::connect
        E["XmlConverterSinkTask<br/>(Task Lifecycle / Producer Management)"]:::connect
        F["XmlRecordWriter<br/>(Schema Translation & Sanitization)"]:::logic
        G["Internal Kafka Producer<br/>(Batched Async Publisher)"]:::connect
    end

    %% Flow arrows
    A -->|"1. Fetch Bytes"| C
    C -->|"2. Hand over Bytes"| D
    D -->|"3. Convert to In-Memory Struct/Map"| E
    E -->|"4. Serialize Object to XML"| F
    F -->|"5. Return XML String"| E
    E -->|"6. Publish XML Record"| G
    G -->|"7. Produce XML Bytes"| B
```

### Architectural Components

1. **Source Topic**: The origin topic containing messages serialized in standard Kafka formats such as Apache Avro (with Confluent Schema Registry) or JSON (with or without schema).
2. **Deserialization Converter**: The connector delegates record parsing to the standard built-in or plugin converters (`AvroConverter` or `JsonConverter`). The converter turns raw byte arrays from the source topic into JVM objects (specifically Kafka Connect `Struct` or standard Java `Map`).
3. **XmlConverterSinkTask**: Receives batches of deserialized records from the Connect framework. It manages the task lifecycle, configuration, and coordinates XML conversion and republishing.
4. **XmlRecordWriter**: A zero-dependency utility that recursively converts the `Struct`, `Map`, `Collection`, or primitive types into a standard-compliant XML string. It handles XML escaping, element tag sanitization, and base64-encoding for binary payloads.
5. **Internal Kafka Producer**: A dedicated Kafka Producer instance configured under the prefix `xml.producer.*`. It writes the generated XML strings to the target topic asynchronously to exploit batching optimizations, then blocks on batch completion futures before task completion to guarantee **at-least-once delivery**.
6. **Target XML Topic**: The destination topic containing the final, generated XML payload (wrapped by a configurable root element).

---

## Data Flow & Processing Lifecycle

Below is the step-by-step execution path for every record batch processed by the task:

1. **Polling**: The Kafka Connect engine polls records from the Source Topic and routes them to the converter.
2. **Task Intake**: Connect calls `SinkTask.put(Collection<SinkRecord> records)` with the deserialized records.
3. **XML Serialization**: For each `SinkRecord`, the task calls `XmlRecordWriter.convertToXml(...)` passing:
   - The record value payload.
   - The root tag name configured via `xml.root.element.name`.
4. **Sanitization & Escaping**: The writer cleans XML tags (removing invalid characters/spaces) and escapes illegal XML characters (such as `&`, `<`, `>`) in text values.
5. **Header & Key Propagation**: The task extracts the original key (as bytes) and all headers from the source record and copies them onto the target record.
6. **Asynchronous Dispatch**: The task submits the new record to the internal Kafka Producer.
7. **Commit Block**: The task waits for all pending producer writes in the current batch to acknowledge (`future.get()`). If any write fails, a `ConnectException` is thrown to halt offsets and force a retry.

---

## Detailed Example Walkthrough

This example details exactly how the connector takes a JSON/Avro record, transforms it, and formats the output XML.

### 1. The Input Payload (JSON or Avro Struct)
Consider a Kafka record in the source topic representing a user order. 
* If using **Avro/Schema Registry**, this is serialized as Avro bytes.
* If using **Schemaless JSON**, the record payload looks like this:

```json
{
  "order id": 9988,
  "customer": {
    "first name": "Alice & Bob",
    "status": "VIP"
  },
  "tags": ["retail", "urgent"],
  "binaryToken": "SGVsbG8="
}
```

### 2. Intermediate In-Memory Representation
The configured Converter deserializes the JSON/Avro bytes into Java memory. In the task, the object is resolved as:
* A `Map` (specifically `LinkedHashMap` for schemaless JSON).
* A `Struct` with schemas (for Avro or JSON-with-schema).

### 3. XML Conversion Logic
The task invokes `XmlRecordWriter.convertToXml(payload, "orderRecord")`. The writer performs the following operations:

* **Root Wrapping**: Creates opening `<orderRecord>` and closing `</orderRecord>` tags.
* **Tag Sanitization**: XML tags cannot contain spaces. The writer sanitizes key names:
  - `"order id"` $\rightarrow$ `<order_id>`
  - `"first name"` $\rightarrow$ `<first_name>`
* **Special Character Escaping**: XML-unsafe text values are escaped to prevent parsing errors:
  - `"Alice & Bob"` $\rightarrow$ `Alice &amp; Bob`
* **Array / Collection Handling**: Arrays are unrolled into repeating tags of the same name:
  - `"tags": ["retail", "urgent"]` $\rightarrow$ `<tags>retail</tags><tags>urgent</tags>`
* **Binary Serialization**: Binary fields (`byte[]`) are encoded to Base64 strings.

### 4. Output XML Message
The generated payload published to the target topic is:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<orderRecord>
  <order_id>9988</order_id>
  <customer>
    <first_name>Alice &amp; Bob</first_name>
    <status>VIP</status>
  </customer>
  <tags>retail</tags>
  <tags>urgent</tags>
  <binaryToken>SGVsbG8=</binaryToken>
</orderRecord>
```

---

## Configuration Reference

| Property | Type | Importance | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `xml.target.topic` | String | High | *(Required)* | The name of the Kafka topic where XML messages will be written. |
| `xml.root.element.name` | String | Medium | `record` | The root tag element for the generated XML documents. |
| `xml.producer.bootstrap.servers` | String | High | *(Required)* | Bootstrap servers for the internal Kafka Producer. |
| `xml.producer.*` | Config | Medium | - | Any standard Kafka Producer property can be passed to the internal producer by prefixing it with `xml.producer.` (e.g., `xml.producer.acks=all`). |

---

## Build & Installation

### Requirements
* Java JDK 17
* Apache Maven

### Steps
1. Clean and package the JAR:
   ```bash
   mvn clean package
   ```
2. Retrieve the plugin artifact from `target/kafka-xml-smt-1.0-SNAPSHOT.jar`.
3. Copy the JAR file to your Kafka Connect worker's plugins directory (e.g., `/usr/share/java/kafka/plugins/`).
4. Restart your Kafka Connect cluster instances to load the connector.

---

## Connector Deployment Example

Use the Kafka Connect REST API to deploy the connector. Below is an example payload to configure the connector for handling **Avro** data:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data '{
    "name": "xml-converter-connector",
    "config": {
      "connector.class": "xml.converter.XmlConverterSinkConnector",
      "tasks.max": "3",
      "topics": "avro-source-orders",
      
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter.schema.registry.url": "http://localhost:8081",
      
      "xml.target.topic": "xml-target-orders",
      "xml.root.element.name": "orderRecord",
      
      "xml.producer.bootstrap.servers": "localhost:9092",
      "xml.producer.acks": "all",
      "xml.producer.retries": "5"
    }
  }' \
  http://localhost:8083/connectors
```
