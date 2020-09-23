package ecnu.db.constraintchain.arithmetic;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import ecnu.db.constraintchain.arithmetic.operator.DivNode;
import ecnu.db.constraintchain.arithmetic.operator.MinusNode;
import ecnu.db.constraintchain.arithmetic.operator.MulNode;
import ecnu.db.constraintchain.arithmetic.operator.PlusNode;
import ecnu.db.constraintchain.arithmetic.value.ColumnNode;
import ecnu.db.constraintchain.arithmetic.value.NumericNode;

import java.io.IOException;

/**
 * @author alan
 */
public class ArithmeticNodeDeserializer extends StdDeserializer<ArithmeticNode> {

    public ArithmeticNodeDeserializer() {
        this(null);
    }

    public ArithmeticNodeDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public ArithmeticNode deserialize(JsonParser parser, DeserializationContext deserializationContext) throws IOException {
        JsonNode node = parser.getCodec().readTree(parser);
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ArithmeticNode.class, new ArithmeticNodeDeserializer());
        mapper.registerModule(module);
        switch (ArithmeticNodeType.valueOf(node.get("type").asText())) {
            case CONSTANT:
                return mapper.readValue(node.toString(), NumericNode.class);
            case MINUS:
                return mapper.readValue(node.toString(), MinusNode.class);
            case PLUS:
                return mapper.readValue(node.toString(), PlusNode.class);
            case MUL:
                return mapper.readValue(node.toString(), MulNode.class);
            case DIV:
                return mapper.readValue(node.toString(), DivNode.class);
            case COLUMN:
                return mapper.readValue(node.toString(), ColumnNode.class);
            default:
                throw new IOException(String.format("无法识别的ArithmeticNode数据 %s", node));
        }
    }
}
