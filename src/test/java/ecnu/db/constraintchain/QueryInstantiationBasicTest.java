package ecnu.db.constraintchain;

import ch.obermuhlner.math.big.BigDecimalMath;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.chain.ConstraintChainFilterNode;
import ecnu.db.constraintchain.chain.ConstraintChainNode;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.constraintchain.filter.ParameterResolver;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.column.DateTimeColumn;
import ecnu.db.schema.column.IntColumn;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import org.apache.commons.io.FileUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ecnu.db.app.TaskConfigurator.queryInstantiation;
import static ecnu.db.utils.CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryInstantiationBasicTest {
    Map<String, List<ConstraintChain>> query2chains;
    int samplingSize = 10_000;

    @BeforeEach
    public void setUp() throws IOException {
        ParameterResolver.ITEMS.clear();
        query2chains = CommonUtils.MAPPER.readValue(FileUtils.readFileToString(
                new File("src/test/resources/data/query-instantiation/basic/constraintChain.json"), UTF_8),
                new TypeReference<Map<String, List<ConstraintChain>>>() {
                });
    }


    @Test
    public void getOperationsTest() throws Exception {
        Multimap<String, AbstractFilterOperation> query2operations = ArrayListMultimap.create();
        for (String query : query2chains.keySet()) {
            List<ConstraintChain> chains = query2chains.get(query);
            for (ConstraintChain chain : chains) {
                String tableName = chain.getTableName();
                for (ConstraintChainNode node : chain.getNodes()) {
                    if (node instanceof ConstraintChainFilterNode) {
                        List<AbstractFilterOperation> operations = ((ConstraintChainFilterNode) node).pushDownProbability();
                        query2operations.putAll(query + "_" + tableName, operations);
                    }
                }
            }
        }
        List<AbstractFilterOperation> operations;
        operations = new ArrayList<>(query2operations.get("2_1.sql_tpch.part"));
        assertEquals(2, operations.size());
        assertThat(BigDecimalMath.pow(BigDecimal.valueOf(0.00416), BigDecimal.valueOf(0.5), BIG_DECIMAL_DEFAULT_PRECISION), Matchers.comparesEqualTo(operations.get(0).getProbability()));

        operations = new ArrayList<>(query2operations.get("3_1.sql_tpch.customer"));
        assertEquals(1, operations.size());
        assertThat(BigDecimal.valueOf(0.20126), Matchers.comparesEqualTo(operations.get(0).getProbability()));
        operations = new ArrayList<>(query2operations.get("3_1.sql_tpch.orders"));
        assertEquals(1, operations.size());
        assertThat(BigDecimal.valueOf(0.4827473333), Matchers.comparesEqualTo(operations.get(0).getProbability()));

        operations = new ArrayList<>(query2operations.get("11_1.sql_tpch.nation"));
        assertEquals(1, operations.size());
        assertThat(BigDecimal.valueOf(0.04), Matchers.comparesEqualTo(operations.get(0).getProbability()));

        //todo check 
        operations = new ArrayList<>(query2operations.get("6_1.sql_tpch.lineitem"));
        assertEquals(3, operations.size());
        assertThat(BigDecimalMath.pow(BigDecimal.valueOf(0.01904131080122942), BigDecimal.ONE.divide(BigDecimal.valueOf(3), BIG_DECIMAL_DEFAULT_PRECISION), BIG_DECIMAL_DEFAULT_PRECISION), Matchers.comparesEqualTo(operations.get(0).getProbability()));

    }

    @Test
    public void computeTest() throws Exception {
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
        // 2.sql_1 simple eq
        IntColumn col = (IntColumn) ColumnManager.getInstance().getColumn("tpch.part.p_size");
        assertTrue(Integer.parseInt(id2Parameter.get(19).getData()) >= col.getMin(),
                String.format("'%s' should be greater than or equal to '%d'", id2Parameter.get(19).getData(), col.getMin()));
        assertTrue(Integer.parseInt(id2Parameter.get(19).getData()) <= col.getMax(),
                String.format("'%s' should be less than '%d'", id2Parameter.get(19).getData(), col.getMax()));
        assertThat(id2Parameter.get(20).getData(), startsWith("%"));
        assertEquals(id2Parameter.get(21).getData(), id2Parameter.get(22).getData());
        // 6.sql_1 between
        LocalDateTime left = LocalDateTime.parse(id2Parameter.get(26).getData(), DateTimeColumn.FMT),
                right = LocalDateTime.parse(id2Parameter.get(29).getData(), DateTimeColumn.FMT);
        Duration duration = Duration.between(left, right),
                wholeDuration = Duration.between(
                        LocalDateTime.parse("1992-01-02 00:00:00", DateTimeColumn.FMT),
                        LocalDateTime.parse("1998-12-01 00:00:00", DateTimeColumn.FMT));
        double rate = duration.getSeconds() * 1.0 / wholeDuration.getSeconds();
        assertEquals(rate, 0.267, 0.001);

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