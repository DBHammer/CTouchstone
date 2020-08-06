package ecnu.db.constraintchain.chain;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprNodeDeserializer;

import java.io.IOException;

/**
 * @author alan
 */
public class ConstraintChainNodeDeserializer extends StdDeserializer<ConstraintChainNode> {

    public ConstraintChainNodeDeserializer() {
        this(null);
    }

    public ConstraintChainNodeDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public ConstraintChainNode deserialize(JsonParser parser, DeserializationContext deserializationContext) throws IOException {
        JsonNode node = parser.getCodec().readTree(parser);
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(BoolExprNode.class, new BoolExprNodeDeserializer());
        mapper.registerModule(module);
        switch (ConstraintChainNodeType.valueOf(node.get("constraintChainNodeType").asText())) {
            case FILTER:
                return mapper.readValue(node.toString(), ConstraintChainFilterNode.class);
            case FK_JOIN:
                return mapper.readValue(node.toString(), ConstraintChainFkJoinNode.class);
            case PK_JOIN:
                return mapper.readValue(node.toString(), ConstraintChainPkJoinNode.class);
            default:
                throw new IOException(String.format("无法识别的ConstraintChain数据 %s", node));
        }
    }
}
