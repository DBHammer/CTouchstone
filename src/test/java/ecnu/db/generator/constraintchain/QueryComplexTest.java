package ecnu.db.generator.constraintchain;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.generator.constraintchain.filter.ConstraintChainFilterNode;
import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.utils.CommonUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ecnu.db.utils.CommonUtils.readFile;
import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryComplexTest {
    Map<String, List<ConstraintChain>> query2chains;
    int samplingSize = 10_000;

    @BeforeEach
    public void setUp() throws IOException {
        String content = readFile("src/test/resources/data/query-instantiation/basic/constraintChain.json");
        query2chains = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
    }

    @Test
    void getOperationsTest() {
        Map<String, List<AbstractFilterOperation>> query2operations = new HashMap<>();
        for (String query : query2chains.keySet()) {
            List<ConstraintChain> chains = query2chains.get(query);
            for (ConstraintChain chain : chains) {
                String tableName = chain.getTableName();
                for (ConstraintChainNode node : chain.getNodes()) {
                    if (node instanceof ConstraintChainFilterNode) {
                        List<AbstractFilterOperation> operations = ((ConstraintChainFilterNode) node).pushDownProbability();
                        String key = query + "_" + tableName;
                        if (!query2operations.containsKey(key)) {
                            query2operations.put(key, new ArrayList<>());
                        }
                        query2operations.get(key).addAll(operations);
                    }
                }
            }
        }
        List<AbstractFilterOperation> operations;
        operations = query2operations.get("19_1.sql_public.lineitem");
        assertEquals(8, operations.size());
        assertEquals(0.01924177021, operations.get(0).getProbability().doubleValue(), 0.0000001);

        operations = query2operations.get("19_1.sql_public.lineitem");
        assertEquals(8, operations.size());
        assertEquals(1.0, operations.get(1).getProbability().doubleValue(), 0.0000001);

        operations = query2operations.get("19_1.sql_public.lineitem");
        assertEquals(8, operations.size());
        assertEquals(1.0, operations.get(2).getProbability().doubleValue(), 0.0000001);

        operations = query2operations.get("19_1.sql_public.lineitem");
        assertEquals(8, operations.size());
        assertEquals( 0.01924177021, operations.get(7).getProbability().doubleValue(), 0.0000001);

        operations = query2operations.get("19_1.sql_public.part");
        assertEquals(10, operations.size());
        assertEquals(0.00243, operations.get(1).getProbability().doubleValue(), 0.0000001);

        operations = query2operations.get("19_1.sql_public.part");
        assertEquals(10, operations.size());
        assertEquals(0.00243, operations.get(3).getProbability().doubleValue(), 0.0000001);

        operations = query2operations.get("19_1.sql_public.part");
        assertEquals(10, operations.size());
        assertEquals(0, operations.get(6).getProbability().doubleValue(), 0.0000001);
    }

}
