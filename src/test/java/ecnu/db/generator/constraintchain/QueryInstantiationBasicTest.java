package ecnu.db.generator.constraintchain;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.generator.constraintchain.chain.ConstraintChain;
import ecnu.db.generator.constraintchain.chain.ConstraintChainFilterNode;
import ecnu.db.generator.constraintchain.chain.ConstraintChainNode;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.schema.ColumnManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ecnu.db.analyzer.TaskConfigurator.queryInstantiation;
import static ecnu.db.utils.CommonUtils.readFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class QueryInstantiationBasicTest {
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
        operations = query2operations.get("2_1.sql_tpch.part");
        assertNull(operations);

        operations = query2operations.get("3_1.sql_tpch.customer");
        assertEquals(1, operations.size());
        assertEquals(0.1983466667, operations.get(0).getProbability().doubleValue(), 0.0000001);
        operations = query2operations.get("3_1.sql_tpch.orders");
        assertEquals(1, operations.size());
        assertEquals(0.4838209734, operations.get(0).getProbability().doubleValue(), 0.0000001);


        operations = query2operations.get("1_1.sql_tpch.lineitem");
        assertEquals(1, operations.size());
        assertEquals(0.9797396027, operations.get(0).getProbability().doubleValue(), 0.0000001);

        operations = query2operations.get("6_1.sql_tpch.lineitem");
        //todo merge uni filter 2 range filter
        assertEquals(5, operations.size());
        assertEquals(0.01902281455, operations.get(0).getProbability().doubleValue(), 0.0000001);
    }

    @Disabled
    @Test
    void computeTest() throws Exception {
        ColumnManager.getInstance().setResultDir("src/test/resources/data/query-instantiation/basic");
        ColumnManager.getInstance().loadColumnDistribution();
        // **********************************
        // *    test query instantiation    *
        // **********************************
        queryInstantiation(query2chains.values().stream().flatMap(Collection::stream).collect(Collectors.toList()), samplingSize);
        Map<Integer, Parameter> id2Parameter = new HashMap<>();
        for (String key : query2chains.keySet()) {
            List<Parameter> parameters = query2chains.get(key).stream().flatMap((l) -> l.getParameters().stream()).collect(Collectors.toList());
            parameters.forEach((param) -> id2Parameter.put(param.getId(), param));
        }
//        // 2.sql_1 simple eq
//        // todo
//        Column col = ColumnManager.getInstance().getColumn("tpch.part.p_size");
//        assertTrue(Integer.parseInt(id2Parameter.get(19).getDataValue()) >= col.getMin(),
//                String.format("'%s' should be greater than or equal to '%d'", id2Parameter.get(19).getData(), col.getMin()));
//        assertTrue(Integer.parseInt(id2Parameter.get(19).getDataValue()) <= col.getMax(),
//                String.format("'%s' should be less than '%d'", id2Parameter.get(19).getData(), col.getMax()));
//        assertThat(id2Parameter.get(20).getDataValue(), startsWith("%"));
//        assertEquals(id2Parameter.get(21).getData(), id2Parameter.get(22).getData());
        // 6.sql_1 between
        long left = id2Parameter.get(0).getData();
        long right = id2Parameter.get(2).getData();
        assertEquals(26694, right - left, 0);

        // ******************************
        // *    test data generation    *
        // ******************************
        List<ConstraintChain> chains;
        Map<String, Double> map;
        chains = query2chains.get("2_1.sql");
        map = getRate(chains);
        // todo 精度有待提高
        assertEquals(0.00416, map.get("tpch.part"), 0.003);
        assertEquals(0.2, map.get("tpch.region"), 0.003);

        chains = query2chains.get("6_1.sql");
        map = getRate(chains);
        assertEquals(0.01904131080, map.get("tpch.lineitem"), 0.003);
    }

    private Map<String, Double> getRate(List<ConstraintChain> chains) throws TouchstoneException {
        Map<String, Double> ret = new HashMap<>();
        for (ConstraintChain chain : chains) {
            String tableName = chain.getTableName();
            for (ConstraintChainNode node : chain.getNodes()) {
                if (node instanceof ConstraintChainFilterNode) {
                    boolean[] evaluation = ((ConstraintChainFilterNode) node).getRoot().evaluate();
                    double rate = IntStream.range(0, evaluation.length).filter((i) -> evaluation[i]).count() * 1.0 / evaluation.length;
                    ret.put(tableName, rate);
                }
            }
        }
        return ret;
    }
}