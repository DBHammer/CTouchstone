package ecnu.db.generator.constraintchain.filter;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import ecnu.db.generator.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.generator.constraintchain.arithmetic.ArithmeticNodeDeserializer;
import ecnu.db.generator.constraintchain.filter.logical.AndNode;
import ecnu.db.generator.constraintchain.filter.logical.OrNode;
import ecnu.db.generator.constraintchain.filter.operation.IsNullFilterOperation;
import ecnu.db.generator.constraintchain.filter.operation.MultiVarFilterOperation;
import ecnu.db.generator.constraintchain.filter.operation.UniVarFilterOperation;

import java.io.IOException;

/**
 * @author alan
 */
public class BoolExprNodeDeserializer extends StdDeserializer<BoolExprNode> {

    public BoolExprNodeDeserializer() {
        this(null);
    }

    public BoolExprNodeDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public BoolExprNode deserialize(JsonParser parser, DeserializationContext deserializationContext) throws IOException {
        JsonNode node = parser.getCodec().readTree(parser);
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(BoolExprNode.class, new BoolExprNodeDeserializer());
        module.addDeserializer(ArithmeticNode.class, new ArithmeticNodeDeserializer());
        mapper.registerModule(module);
        switch (BoolExprType.valueOf(node.get("type").asText())) {
            case AND:
                return mapper.readValue(node.toString(), AndNode.class);
            case OR:
                return mapper.readValue(node.toString(), OrNode.class);
            case UNI_FILTER_OPERATION:
                return mapper.readValue(node.toString(), UniVarFilterOperation.class);
            case MULTI_FILTER_OPERATION:
                return mapper.readValue(node.toString(), MultiVarFilterOperation.class);
            case ISNULL_FILTER_OPERATION:
                return mapper.readValue(node.toString(), IsNullFilterOperation.class);
            case BETWEEN_FILTER_OPERATION:
            default:
                throw new IOException(String.format("无法识别的BoolExpr数据 %s", node));
        }
    }
}

