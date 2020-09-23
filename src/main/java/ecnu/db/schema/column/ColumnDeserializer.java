package ecnu.db.schema.column;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;

/**
 * @author xuechao.lian
 */
public class ColumnDeserializer extends StdDeserializer<AbstractColumn> {

    public ColumnDeserializer() {
        this(null);
    }

    public ColumnDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public AbstractColumn deserialize(JsonParser parser, DeserializationContext deserializationContext) throws IOException {
        JsonNode node = parser.getCodec().readTree(parser);
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        switch (ColumnType.valueOf(node.get("columnType").asText())) {
            case INTEGER:
                return mapper.readValue(node.toString(), IntColumn.class);
            case BOOL:
                return mapper.readValue(node.toString(), BoolColumn.class);
            case DECIMAL:
                return mapper.readValue(node.toString(), DecimalColumn.class);
            case VARCHAR:
                return mapper.readValue(node.toString(), StringColumn.class);
            case DATETIME:
                return mapper.readValue(node.toString(), DateTimeColumn.class);
            case DATE:
                return mapper.readValue(node.toString(), DateColumn.class);
            default:
                throw new IOException(String.format("无法识别的Column数据 %s", node));
        }
    }
}
