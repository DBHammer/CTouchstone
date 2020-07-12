package ecnu.db.schema.column;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

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
    public AbstractColumn deserialize(JsonParser parser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        JsonNode node = parser.getCodec().readTree(parser);
        switch (ColumnType.valueOf(node.get("columnType").asText())) {
            case INTEGER:
                IntColumn intColumn = new IntColumn(node.get("columnName").asText());
                intColumn.setNullPercentage(node.get("nullPercentage").floatValue());
                intColumn.setNdv(node.get("ndv").asInt());
                intColumn.setMin(node.get("min").asInt());
                intColumn.setMax(node.get("max").asInt());
                return intColumn;
            case BOOL:
                BoolColumn boolColumn = new BoolColumn(node.get("columnName").asText());
                boolColumn.setNullPercentage(node.get("nullPercentage").floatValue());
                return boolColumn;
            case DECIMAL:
                DecimalColumn decimalColumn = new DecimalColumn(node.get("columnName").asText());
                decimalColumn.setNullPercentage(node.get("nullPercentage").floatValue());
                decimalColumn.setMin(node.get("min").asInt());
                decimalColumn.setMax(node.get("max").asInt());
                return decimalColumn;
            case VARCHAR:
                StringColumn stringColumn = new StringColumn(node.get("columnName").asText());
                stringColumn.setNullPercentage(node.get("nullPercentage").floatValue());
                stringColumn.setNdv(node.get("ndv").asInt());
                stringColumn.setMaxLength(node.get("maxLength").asInt());
                stringColumn.setAvgLength(node.get("avgLength").decimalValue());
                return stringColumn;
            case DATETIME:
                DateColumn dateColumn = new DateColumn(node.get("columnName").asText());
                dateColumn.setNullPercentage(node.get("nullPercentage").floatValue());
                dateColumn.setBegin(node.get("begin").asText());
                dateColumn.setEnd(node.get("end").asText());
                return dateColumn;
            default:
                throw new IOException(String.format("无法识别的Column数据 %s", node));
        }
    }
}
