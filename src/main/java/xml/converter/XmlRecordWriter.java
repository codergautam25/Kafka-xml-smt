package xml.converter;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.util.Base64;
import java.util.Collection;
import java.util.Map;

/**
 * Utility class to convert Kafka Connect records (Struct/Map) into XML documents.
 */
public class XmlRecordWriter {

    private static final String XML_DECLARATION = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";

    /**
     * Converts a record value (Struct, Map, or Primitive) to an XML String.
     *
     * @param value       the record value payload.
     * @param rootElement the name of the XML root element.
     * @return the serialized XML string.
     */
    public static String convertToXml(Object value, String rootElement) {
        StringBuilder xml = new StringBuilder(XML_DECLARATION);
        String root = sanitizeXmlTagName(rootElement);
        
        if (value == null) {
            xml.append("<").append(root).append("/>");
            return xml.toString();
        }

        xml.append("<").append(root).append(">");
        appendValue(xml, value);
        xml.append("</").append(root).append(">");
        return xml.toString();
    }

    private static void appendField(StringBuilder xml, String name, Object value) {
        if (value == null) {
            xml.append("<").append(name).append("/>");
        } else if (value instanceof Collection) {
            for (Object item : (Collection<?>) value) {
                xml.append("<").append(name).append(">");
                appendValue(xml, item);
                xml.append("</").append(name).append(">");
            }
        } else if (value.getClass().isArray()) {
            if (value instanceof byte[]) {
                xml.append("<").append(name).append(">");
                xml.append(Base64.getEncoder().encodeToString((byte[]) value));
                xml.append("</").append(name).append(">");
            } else {
                int length = java.lang.reflect.Array.getLength(value);
                for (int i = 0; i < length; i++) {
                    Object item = java.lang.reflect.Array.get(value, i);
                    xml.append("<").append(name).append(">");
                    appendValue(xml, item);
                    xml.append("</").append(name).append(">");
                }
            }
        } else {
            xml.append("<").append(name).append(">");
            appendValue(xml, value);
            xml.append("</").append(name).append(">");
        }
    }

    private static void appendValue(StringBuilder xml, Object value) {
        if (value == null) {
            return;
        }

        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            if (struct.schema() != null && struct.schema().fields() != null) {
                for (Field field : struct.schema().fields()) {
                    appendField(xml, sanitizeXmlTagName(field.name()), struct.get(field));
                }
            }
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                appendField(xml, sanitizeXmlTagName(String.valueOf(entry.getKey())), entry.getValue());
            }
        } else if (value instanceof Collection) {
            for (Object item : (Collection<?>) value) {
                appendField(xml, "item", item);
            }
        } else if (value.getClass().isArray()) {
            if (value instanceof byte[]) {
                xml.append(Base64.getEncoder().encodeToString((byte[]) value));
            } else {
                int length = java.lang.reflect.Array.getLength(value);
                for (int i = 0; i < length; i++) {
                    appendField(xml, "item", java.lang.reflect.Array.get(value, i));
                }
            }
        } else {
            xml.append(escapeXml(String.valueOf(value)));
        }
    }

    /**
     * Sanitizes a string to be a valid XML tag name.
     * Valid tag names must start with a letter or underscore, and contain only letters, numbers,
     * underscores, hyphens, and periods.
     */
    public static String sanitizeXmlTagName(String name) {
        if (name == null || name.trim().isEmpty()) {
            return "element";
        }
        
        // Clean leading/trailing spaces
        name = name.trim();
        
        StringBuilder sb = new StringBuilder();
        char first = name.charAt(0);
        if (Character.isLetter(first) || first == '_') {
            sb.append(first);
        } else {
            sb.append('_');
            if (Character.isDigit(first) || first == '-' || first == '.') {
                sb.append(first);
            }
        }

        for (int i = 1; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_' || c == '-' || c == '.') {
                sb.append(c);
            } else {
                sb.append('_');
            }
        }
        return sb.toString();
    }

    /**
     * Escapes special XML characters to prevent issues with parsed strings.
     */
    public static String escapeXml(String input) {
        if (input == null) {
            return "";
        }
        return input.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                    .replace("\"", "&quot;")
                    .replace("'", "&apos;");
    }
}
