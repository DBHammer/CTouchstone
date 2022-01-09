package ecnu.db.generator.constraintchain;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.generator.constraintchain.filter.ConstraintChainFilterNode;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryInstantiationBasicTest {
    Map<String, List<ConstraintChain>> query2chains;
    private final static List<String> allColumns = List.of(
            //part table
            "public.part.p_type", "public.part.p_name", "public.part.p_size", "public.part.p_brand", "public.part.p_container",
            //region table
            "public.region.r_name", "public.orders.o_orderdate",
            //customer table
            "public.customer.c_mktsegment", "public.customer.c_acctbal", "public.customer.c_phone",
            //nation table
            "public.nation.n_name",
            //orders table
            "public.orders.o_orderdate", "public.orders.o_orderstatus", "public.orders.o_comment",
            //supplier table
            "public.supplier.s_comment"
    );
    private final static List<String> lineColumns = List.of("public.lineitem.l_commitdate", "public.lineitem.l_shipdate",
            "public.lineitem.l_receiptdate", "public.lineitem.l_quantity", "public.lineitem.l_discount",
            "public.lineitem.l_shipmode", "public.lineitem.l_shipinstruct", "public.lineitem.l_returnflag");

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
        assertEquals(0.1997866667, operations.get(0).getProbability().doubleValue(), 0.0000001);
        operations = query2operations.get("3_1.sql_public.orders");
        assertEquals(1, operations.size());
        assertEquals(0.4827473333, operations.get(0).getProbability().doubleValue(), 0.0000001);


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
        ColumnManager.getInstance().prepareParameterInit(new HashSet<>(allColumns), 10_000);
        ColumnManager.getInstance().prepareParameterInit(new HashSet<>(lineColumns), 400_0000);
        // **********************************
        // *    test query instantiation    *
        // **********************************
        Map<Integer, Parameter> id2Parameter = new HashMap<>();
        for (String key : query2chains.keySet()) {
            List<Parameter> parameters = query2chains.get(key).stream().flatMap((l) -> l.getParameters().stream()).toList();
            parameters.forEach((param) -> id2Parameter.put(param.getId(), param));
        }
        // 2.sql_1 simple eq
        Column col = ColumnManager.getInstance().getColumn("public.part.p_size");
        assertTrue(id2Parameter.get(88).getData() >= col.getMin(),
                String.format("'%s' should be greater than or equal to '%d'", id2Parameter.get(88).getData(), col.getMin()));
        assertTrue(id2Parameter.get(88).getData() <= col.getRange(),
                String.format("'%s' should be less than '%d'", id2Parameter.get(88).getData(), col.getRange()));

        // 6.sql_1 between
        long left = id2Parameter.get(126).getData();
        long right = id2Parameter.get(127).getData();
        assertEquals(49, right - left, 0);

        // ******************************
        // *    test data generation    *
        // ******************************
        List<ConstraintChain> chains;
        Map<String, Double> map;

        chains = query2chains.get("1_1.sql");
        map = getRate(chains);
        assertEquals(0.9928309517, map.get("public.lineitem"), 0.00005);

        chains = query2chains.get("6_1.sql");
        map = getRate(chains);
        assertEquals(0.01904131080, map.get("public.lineitem"), 0.0005);

        chains = query2chains.get("3_1.sql");
        map = getRate(chains);
        assertEquals(0.4827473333, map.get("public.orders"), 0.0001);

        chains = query2chains.get("3_1.sql");
        map = getRate(chains);
        assertEquals(0.1997866667, map.get("public.customer"), 0.0001);

        chains = query2chains.get("4_1.sql");
        map = getRate(chains);
        assertEquals(0.038316, map.get("public.orders"), 0.0001);

        chains = query2chains.get("4_1.sql");
        map = getRate(chains);
        assertEquals(0.6320880022, map.get("public.lineitem"), 0.001);

        chains = query2chains.get("5_1.sql");
        map = getRate(chains);
        assertEquals(0.2, map.get("public.region"), 0.0001);

        chains = query2chains.get("5_1.sql");
        map = getRate(chains);
        assertEquals(0.1510966667, map.get("public.orders"), 0.0001);

        chains = query2chains.get("7_1.sql");
        map = getRate(chains);
        assertEquals(0.08, map.get("public.nation"), 0.0001);

        chains = query2chains.get("7_1.sql");
        map = getRate(chains);
        assertEquals(0.3046799690, map.get("public.lineitem"), 0.0001);

        chains = query2chains.get("8_1.sql");
        map = getRate(chains);
        assertEquals(0.006835, map.get("public.part"), 0.0001);

        chains = query2chains.get("8_1.sql");
        map = getRate(chains);
        assertEquals(0.2, map.get("public.region"), 0.0001);

        chains = query2chains.get("8_1.sql");
        map = getRate(chains);
        assertEquals(0.304842, map.get("public.orders"), 0.0001);

        chains = query2chains.get("9_1.sql");
        map = getRate(chains);
        assertEquals(0.0544, map.get("public.part"), 0.0001);

        chains = query2chains.get("10_1.sql");
        map = getRate(chains);
        assertEquals(0.2464284316, map.get("public.lineitem"), 0.0001);

        chains = query2chains.get("10_1.sql");
        map = getRate(chains);
        assertEquals(0.03767733333, map.get("public.orders"), 0.0001);

        chains = query2chains.get("11_1.sql");
        map = getRate(chains);
        assertEquals(0.04, map.get("public.nation"), 0.0001);

        chains = query2chains.get("12_1.sql");
        map = getRate(chains);
        assertEquals(0.005138459462, map.get("public.lineitem"), 0.0001);

        chains = query2chains.get("13_1.sql");
        map = getRate(chains);
        assertEquals(0.9892086667, map.get("public.orders"), 0.0001);

        chains = query2chains.get("14_1.sql");
        map = getRate(chains);
        assertEquals(0.01254845894, map.get("public.lineitem"), 0.0001);

        chains = query2chains.get("15_1.sql");
        map = getRate(chains);
        assertEquals(0.03837122983, map.get("public.lineitem"), 0.0001);

        chains = query2chains.get("16_1.sql");
        map = getRate(chains);
        assertEquals(0.0004, map.get("public.supplier"), 0.0001);

        chains = query2chains.get("17_1.sql");
        map = getRate(chains);
        assertEquals(0.001025, map.get("public.part"), 0.0005);

        chains = query2chains.get("19_1.sql");
        map = getRate(chains);
        assertEquals(0.00243, map.get("public.part"), 0.0005);

        chains = query2chains.get("19_1.sql");
        map = getRate(chains);
        assertEquals(0.01924177021, map.get("public.lineitem"), 0.0005);

        chains = query2chains.get("20_1.sql");
        map = getRate(chains);
        assertEquals(0.04, map.get("public.nation"), 0.0001);

        chains = query2chains.get("20_1.sql");
        map = getRate(chains);
        assertEquals(0.011165, map.get("public.part"), 0.0001);

        chains = query2chains.get("20_1.sql_2");
        map = getRate(chains);
        assertEquals(0.1514228369, map.get("public.lineitem"), 0.0001);

        chains = query2chains.get("21_1.sql");
        map = getRate(chains);
        assertEquals(0.6320880022, map.get("public.lineitem"), 0.001);

        chains = query2chains.get("21_1.sql");
        map = getRate(chains);
        assertEquals(0.04, map.get("public.nation"), 0.0001);

        chains = query2chains.get("21_1.sql");
        map = getRate(chains);
        assertEquals(0.4862753333, map.get("public.orders"), 0.0001);

        chains = query2chains.get("22_1.sql");
        map = getRate(chains);
        assertEquals(0.2555866667, map.get("public.customer"), 0.0005);

        chains = query2chains.get("22_1.sql_2");
        map = getRate(chains);
        assertEquals(0.1272, map.get("public.customer"), 0.0006);
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