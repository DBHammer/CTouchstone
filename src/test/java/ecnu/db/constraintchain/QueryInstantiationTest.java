package ecnu.db.constraintchain;

import ch.obermuhlner.math.big.BigDecimalMath;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.chain.ConstraintChainFilterNode;
import ecnu.db.constraintchain.chain.ConstraintChainNode;
import ecnu.db.constraintchain.chain.ConstraintChainReader;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.constraintchain.filter.ParameterResolver;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.ColumnDeserializer;
import ecnu.db.schema.column.DateTimeColumn;
import ecnu.db.schema.column.IntColumn;
import org.apache.commons.io.FileUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ecnu.db.utils.CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryInstantiationTest {
    @Test
    public void getOperationsTest() throws Exception {
        ParameterResolver.items.clear();
        Map<String, List<ConstraintChain>> query2chains = ConstraintChainReader.readConstraintChain("src/test/resources/data/query-instantiation/constraintChain.json");
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
        operations = new ArrayList<>(query2operations.get("2.sql_1_tpch.part"));
        assertEquals(2, operations.size());
        assertThat(BigDecimalMath.pow(BigDecimal.valueOf(0.00416), BigDecimal.valueOf(0.5), BIG_DECIMAL_DEFAULT_PRECISION), Matchers.comparesEqualTo(operations.get(0).getProbability()));

        operations = new ArrayList<>(query2operations.get("3.sql_1_tpch.customer"));
        assertEquals(1, operations.size());
        assertThat(BigDecimal.valueOf(0.20126), Matchers.comparesEqualTo(operations.get(0).getProbability()));
        operations = new ArrayList<>(query2operations.get("3.sql_1_tpch.orders"));
        assertEquals(1, operations.size());
        assertThat(BigDecimal.valueOf(0.4827473333), Matchers.comparesEqualTo(operations.get(0).getProbability()));

        operations = new ArrayList<>(query2operations.get("11.sql_1_tpch.nation"));
        assertEquals(1, operations.size());
        assertThat(BigDecimal.valueOf(0.04), Matchers.comparesEqualTo(operations.get(0).getProbability()));

        operations = new ArrayList<>(query2operations.get("6.sql_1_tpch.lineitem"));
        assertEquals(3, operations.size());
        assertThat(BigDecimalMath.pow(BigDecimal.valueOf(0.01904131080122942), BigDecimal.ONE.divide(BigDecimal.valueOf(3), BIG_DECIMAL_DEFAULT_PRECISION), BIG_DECIMAL_DEFAULT_PRECISION), Matchers.comparesEqualTo(operations.get(0).getProbability()));

    }

    @Test
    public void computeTest() throws Exception {
        ParameterResolver.items.clear();
        Map<String, List<ConstraintChain>> query2chains = ConstraintChainReader.readConstraintChain("src/test/resources/data/query-instantiation/constraintChain.json");
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(AbstractColumn.class, new ColumnDeserializer());
        mapper.registerModule(module);
        mapper.findAndRegisterModules();
        Map<String, Schema> schemas = mapper.readValue(
                FileUtils.readFileToString(new File("src/test/resources/data/query-instantiation/schema.json"), UTF_8),
                new TypeReference<HashMap<String, Schema>>() {
                });
        // **********************************
        // *    test query instantiation    *
        // **********************************
        QueryInstantiation.compute(query2chains.values().stream().flatMap(Collection::stream).collect(Collectors.toList()), schemas);
        Map<Integer, Parameter> id2Parameter = new HashMap<>();
        for (String key : query2chains.keySet()) {
            List<Parameter> parameters = query2chains.get(key).stream().flatMap((l) -> l.getParameters().stream()).collect(Collectors.toList());
            parameters.forEach((param) -> id2Parameter.put(param.getId(), param));
        }
        // 2.sql_1 simple eq
        IntColumn col = (IntColumn) schemas.get("tpch.part").getColumn("p_size");
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
        int generateSize = 10_000;
        for (Schema schema : schemas.values()) {
            for (AbstractColumn column : schema.getColumns().values()) {
                column.prepareGeneration(generateSize);
            }
        }

        List<ConstraintChain> chains;
        Map<String, Double> map;
        chains = query2chains.get("2.sql_1");
        map = getRate(schemas, generateSize, chains);
        // todo 精度有待提高
        assertEquals(0.00416, map.get("tpch.part"), 0.002);
        assertEquals(0.2, map.get("tpch.region"), 0.002);

        chains = query2chains.get("6.sql_1");
        map = getRate(schemas, generateSize, chains);
        assertEquals(0.01904131080, map.get("tpch.lineitem"), 0.003);
    }

    @Test
    public void computeMultiVarTest() throws Exception {
        int samplingSize = 100_000;
        ArithmeticNode.setSize(samplingSize);
        ParameterResolver.items.clear();
        Map<String, List<ConstraintChain>> query2chains = ConstraintChainReader.readConstraintChain("src/test/resources/data/query-instantiation/multi-var-test/constraintChain.json");
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(AbstractColumn.class, new ColumnDeserializer());
        mapper.registerModule(module);
        mapper.findAndRegisterModules();
        Map<String, Schema> schemas = mapper.readValue(
                FileUtils.readFileToString(new File("src/test/resources/data/query-instantiation/multi-var-test/schema.json"), UTF_8),
                new TypeReference<HashMap<String, Schema>>() {
                });
        // *********************************
        // *    test query instantiation   *
        // *********************************
        QueryInstantiation.compute(query2chains.values().stream().flatMap(Collection::stream).collect(Collectors.toList()), schemas);
        Map<Integer, Parameter> id2Parameter = new HashMap<>();
        for (String key : query2chains.keySet()) {
            List<Parameter> parameters = query2chains.get(key).stream().flatMap((l) -> l.getParameters().stream()).collect(Collectors.toList());
            parameters.forEach((param) -> {
                id2Parameter.put(param.getId(), param);
            });
        }
        // known distribution c2:[2,23], c3[-3,9], c4[0,10]
        Float[] c2 = new Float[samplingSize], c3 = new Float[samplingSize], c4 = new Float[samplingSize];
        Random random = new Random();
        initVectorData(samplingSize, c2, 2, 23, random);
        initVectorData(samplingSize, c3, -3, 9, random);
        initVectorData(samplingSize, c4, 0, 10, random);
        // ====================== t1.sql_1: c2 + 2 * c3 * c4 > p0 (ratio = 0.3270440252)
        Float[] v = new Float[samplingSize];
        for (int i = 0; i < samplingSize; i++) {
            v[i] = c2[i] + 2 * c3[i] * c4[i];
        }
        Arrays.sort(v);
        int target = (int) v[(int) ((1 - 0.3270440252) * samplingSize)].floatValue();
        assertEquals(target, Integer.parseInt(id2Parameter.get(0).getData()), 2);
        // ====================== t1.sql_2: c2 + 2 * c3 + c4 > p1 (ratio = 0.8364779874)
        for (int i = 0; i < samplingSize; i++) {
            v[i] = c2[i] + 2 * c3[i] + c4[i];
        }
        Arrays.sort(v);
        target = (int) v[(int) ((1 - 0.8364779874) * samplingSize)].floatValue();
        assertEquals(target, Integer.parseInt(id2Parameter.get(1).getData()), 2);

        // ******************************
        // *    test data generation    *
        // ******************************
        int generateSize = 10_000;
        for (Schema schema : schemas.values()) {
            for (AbstractColumn column : schema.getColumns().values()) {
                column.prepareGeneration(generateSize);
            }
        }
        List<ConstraintChain> chains;
        double rate;
        chains = query2chains.get("t1.sql_1");
        rate = getRate(schemas, generateSize, chains).get("test.test");
        assertEquals(0.3270440252, rate, 0.03);

        chains = query2chains.get("t1.sql_2");
        rate = getRate(schemas, generateSize, chains).get("test.test");
        assertEquals(0.8364779874, rate, 0.03);

    }

    private Map<String, Double> getRate(Map<String, Schema> schemas, int generateSize, List<ConstraintChain> chains) throws TouchstoneToolChainException {
        Map<String, Double> ret = new HashMap<>();
        for (ConstraintChain chain : chains) {
            String tableName = chain.getTableName();
            Schema schema = schemas.get(tableName);
            for (ConstraintChainNode node : chain.getNodes()) {
                if (node instanceof ConstraintChainFilterNode) {
                    boolean[] evaluation = ((ConstraintChainFilterNode) node).getRoot().evaluate(schema, generateSize);
                    double rate = IntStream.range(0, evaluation.length).filter((i) -> evaluation[i]).count() * 1.0 / evaluation.length;
                    ret.put(schema.getTableName(), rate);
                }
            }
        }
        return ret;
    }

    public void initVectorData(int test_size, Float[] c2, int c2_min, int c2_max, Random random) {
        for (int i = 0; i < test_size; i++) {
            c2[i] = random.nextFloat() * (c2_max - c2_min) + c2_min;
        }
    }
}