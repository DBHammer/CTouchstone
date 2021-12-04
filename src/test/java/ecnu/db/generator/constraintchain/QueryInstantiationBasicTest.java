package ecnu.db.generator.constraintchain;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.generator.constraintchain.chain.ConstraintChain;
import ecnu.db.generator.constraintchain.chain.ConstraintChainFilterNode;
import ecnu.db.generator.constraintchain.chain.ConstraintChainNode;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.schema.Column;
import ecnu.db.schema.ColumnManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

import static ecnu.db.utils.CommonUtils.readFile;
import static org.junit.jupiter.api.Assertions.*;

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
        operations = query2operations.get("2_1.sql_public.part");
        assertEquals(2, operations.size());

        operations = query2operations.get("3_1.sql_public.customer");
        assertEquals(1, operations.size());
        assertEquals(0.20126, operations.get(0).getProbability().doubleValue(), 0.0000001);
        operations = query2operations.get("3_1.sql_public.orders");
        assertEquals(1, operations.size());
        assertEquals(0.480684, operations.get(0).getProbability().doubleValue(), 0.0000001);


        operations = query2operations.get("1_1.sql_public.lineitem");
        assertEquals(1, operations.size());
        assertEquals(0.9928309517, operations.get(0).getProbability().doubleValue(), 0.0000001);

        operations = query2operations.get("6_1.sql_public.lineitem");
        //todo merge uni filter 2 range filter
        assertEquals(5, operations.size());
        assertEquals(0.01904131080, operations.get(0).getProbability().doubleValue(), 0.0000001);
    }

    @Test
    void computeTest() throws Exception {
        ColumnManager.getInstance().setResultDir("src/test/resources/data/query-instantiation/basic");
        ColumnManager.getInstance().loadColumnMetaData();
        ColumnManager.getInstance().loadColumnDistribution();
        // **********************************
        // *    test query instantiation    *
        // **********************************
        Map<Integer, Parameter> id2Parameter = new HashMap<>();
        for (String key : query2chains.keySet()) {
            List<Parameter> parameters = query2chains.get(key).stream().flatMap((l) -> l.getParameters().stream()).toList();
            parameters.forEach((param) -> id2Parameter.put(param.getId(), param));
        }
        // 2.sql_1 simple eq
        Column col = ColumnManager.getInstance().getColumn("public.lineitem.l_quantity");
        assertTrue(id2Parameter.get(15).getData() >= col.getMin(),
                String.format("'%s' should be greater than or equal to '%d'", id2Parameter.get(15).getData(), col.getMin()));
        assertTrue(id2Parameter.get(15).getData() <= col.getRange(),
                String.format("'%s' should be less than '%d'", id2Parameter.get(15).getData(), col.getRange()));

        // 6.sql_1 between
        long left = id2Parameter.get(13).getData();
        long right = id2Parameter.get(14).getData();
        assertEquals(100000, right - left, 0);

        // ******************************
        // *    test data generation    *
        // ******************************
        List<ConstraintChain> chains;
        Map<String, Double> map;
        ColumnManager.getInstance().prepareGeneration(new HashSet<>(List.of("public.lineitem.l_shipdate")), samplingSize);
        ColumnManager.getInstance().prepareGeneration(new HashSet<>(List.of("public.lineitem.l_quantity", "public.lineitem.l_discount", "public.orders.o_orderdate", "public.customer.c_mktsegment")), samplingSize);
        chains = query2chains.get("1_1.sql");
        map = getRate(chains);
        assertEquals(0.9928309517, map.get("public.lineitem"), 0.00005);

        chains = query2chains.get("6_1.sql");
        map = getRate(chains);
        assertEquals(0.01904131080, map.get("public.lineitem"), 0.001);

        chains = query2chains.get("3_1.sql");
        map = getRate(chains);
        assertEquals(0.480684, map.get("public.orders"), 0.0001);

        ColumnManager.getInstance().prepareGeneration(new HashSet<>(List.of("public.orders.o_comment")), samplingSize);
        chains = query2chains.get("13_1.sql");
        map = getRate(chains);
        assertEquals(0.9892086667, map.get("public.orders"), 0.0001);
    }

    private Map<String, Double> getRate(List<ConstraintChain> chains) throws TouchstoneException {
        Map<String, Double> ret = new HashMap<>();
        for (ConstraintChain chain : chains) {
            String tableName = chain.getTableName();
            for (ConstraintChainNode node : chain.getNodes()) {
                if (node instanceof ConstraintChainFilterNode) {
                    boolean[] evaluation = ((ConstraintChainFilterNode) node).getRoot().evaluate();
                    long satisfyRowCount = IntStream.range(0, evaluation.length).filter((i) -> evaluation[i]).count();
                    double rate = satisfyRowCount * 1.0 / evaluation.length;
                    ret.put(tableName, rate);
                }
            }
        }
        return ret;
    }
}