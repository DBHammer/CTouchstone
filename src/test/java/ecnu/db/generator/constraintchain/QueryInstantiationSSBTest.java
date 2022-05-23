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

public class QueryInstantiationSSBTest {
    Map<String, List<ConstraintChain>> query2chains;
    private final static List<String> allColumnsInSSB = List.of(
            "public.lineorder.lo_discount", "public.lineorder.lo_quantity",
            "public.date.d_year", "public.date.d_yearmonthnum", "public.date.d_weeknuminyear", "public.date.d_yearmonth",
            "public.part.p_category", "public.part.p_brand1", "public.part.p_mfgr", "public.part.p_category",
            "public.supplier.s_region", "public.supplier.s_nation", "public.supplier.s_city",
            "public.customer.c_region", "public.customer.c_nation", "public.customer.c_city"
    );

    @BeforeEach
    public void setUp() throws IOException {
        String content = readFile("src/test/resources/data/query-instantiation/SSB/constraintChain.json");
        query2chains = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
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

    @Test
    void computeTestForSSB() throws Exception {
        ColumnManager.getInstance().setResultDir("src/test/resources/data/query-instantiation/SSB");
        ColumnManager.getInstance().loadColumnMetaData();
        ColumnManager.getInstance().loadColumnDistribution();
        ColumnManager.getInstance().prepareParameterInit(new HashSet<>(allColumnsInSSB), 4000_000);
        //ColumnManager.getInstance().prepareParameterInit(new HashSet<>(lineColumns), 400_0000);
        // **********************************
        // *    test query instantiation    *
        // **********************************
        Map<Integer, Parameter> id2Parameter = new HashMap<>();
        for (String key : query2chains.keySet()) {
            List<Parameter> parameters = query2chains.get(key).stream().flatMap((l) -> l.getParameters().stream()).toList();
            parameters.forEach((param) -> id2Parameter.put(param.getId(), param));
        }

        // ******************************
        // *    test data generation    *
        // ******************************
        List<ConstraintChain> chains;
        Map<String, Double> map;

        chains = query2chains.get("1_1_1.sql");
        map = getRate(chains);
        assertEquals(0.1309646067, map.get("public.lineorder"), 0.00005);

        chains = query2chains.get("1_1_1.sql");
        map = getRate(chains);
        assertEquals(0.142801252, map.get("public.date"), 0.00005);

        chains = query2chains.get("1_2_1.sql");
        map = getRate(chains);
        assertEquals(0.05464333544, map.get("public.lineorder"), 0.00005);

        chains = query2chains.get("1_2_1.sql");
        map = getRate(chains);
        assertEquals(0.01212832551, map.get("public.date"), 0.00005);

        chains = query2chains.get("1_3_1.sql");
        map = getRate(chains);
        assertEquals(0.05466533115, map.get("public.lineorder"), 0.00005);

        chains = query2chains.get("1_3_1.sql");
        map = getRate(chains);
        assertEquals(0.002738654147, map.get("public.date"), 0.00005);

        chains = query2chains.get("2_1_1.sql");
        map = getRate(chains);
        assertEquals(0.039415, map.get("public.part"), 0.00005);

        chains = query2chains.get("2_1_1.sql");
        map = getRate(chains);
        assertEquals(0.189, map.get("public.supplier"), 0.00005);

        chains = query2chains.get("2_2_1.sql");
        map = getRate(chains);
        assertEquals(0.00792, map.get("public.part"), 0.00005);

        chains = query2chains.get("2_2_1.sql");
        map = getRate(chains);
        assertEquals(0.2245, map.get("public.supplier"), 0.00005);

        chains = query2chains.get("2_3_1.sql");
        map = getRate(chains);
        assertEquals(0.001015, map.get("public.part"), 0.00005);

        chains = query2chains.get("2_3_1.sql");
        map = getRate(chains);
        assertEquals(0.19, map.get("public.supplier"), 0.00005);

        chains = query2chains.get("3_1_1.sql");
        map = getRate(chains);
        assertEquals(0.2017, map.get("public.customer"), 0.00005);

        chains = query2chains.get("3_1_1.sql");
        map = getRate(chains);
        assertEquals(0.2245, map.get("public.supplier"), 0.00005);

        chains = query2chains.get("3_1_1.sql");
        map = getRate(chains);
        assertEquals(0.8575899844, map.get("public.date"), 0.00005);

        chains = query2chains.get("3_2_1.sql");
        map = getRate(chains);
        assertEquals(0.038, map.get("public.supplier"), 0.00005);

        chains = query2chains.get("3_2_1.sql");
        map = getRate(chains);
        assertEquals(0.042, map.get("public.customer"), 0.00005);

        chains = query2chains.get("3_2_1.sql");
        map = getRate(chains);
        assertEquals(0.8575899844, map.get("public.date"), 0.00005);

        chains = query2chains.get("3_3_1.sql");
        map = getRate(chains);
        assertEquals(0.008633333333, map.get("public.customer"), 0.00005);

        chains = query2chains.get("3_3_1.sql");
        map = getRate(chains);
        assertEquals(0.0095, map.get("public.supplier"), 0.00005);

        chains = query2chains.get("3_3_1.sql");
        map = getRate(chains);
        assertEquals(0.8575899844, map.get("public.date"), 0.00005);

        chains = query2chains.get("3_4_1.sql");
        map = getRate(chains);
        assertEquals(0.0095, map.get("public.supplier"), 0.00005);

        chains = query2chains.get("3_4_1.sql");
        map = getRate(chains);
        assertEquals(0.01212832551, map.get("public.date"), 0.00005);

        chains = query2chains.get("3_4_1.sql");
        map = getRate(chains);
        assertEquals(0.008633333333, map.get("public.customer"), 0.00005);

        chains = query2chains.get("4_1_1.sql");
        map = getRate(chains);
        assertEquals(0.189, map.get("public.supplier"), 0.00005);

        chains = query2chains.get("4_1_1.sql");
        map = getRate(chains);
        assertEquals(0.1997333333, map.get("public.customer"), 0.00005);

        chains = query2chains.get("4_1_1.sql");
        map = getRate(chains);
        assertEquals(0.400225, map.get("public.part"), 0.00005);

        chains = query2chains.get("4_2_1.sql");
        map = getRate(chains);
        assertEquals(0.189, map.get("public.supplier"), 0.00005);

        chains = query2chains.get("4_2_1.sql");
        map = getRate(chains);
        assertEquals(0.1997333333, map.get("public.customer"), 0.00005);

        chains = query2chains.get("4_2_1.sql");
        map = getRate(chains);
        assertEquals(0.2852112676, map.get("public.date"), 0.00005);

        chains = query2chains.get("4_2_1.sql");
        map = getRate(chains);
        assertEquals(0.400225, map.get("public.part"), 0.00005);

        chains = query2chains.get("4_3_1.sql");
        map = getRate(chains);
        assertEquals(0.038, map.get("public.supplier"), 0.00005);

        chains = query2chains.get("4_3_1.sql");
        map = getRate(chains);
        assertEquals(0.1997333333, map.get("public.customer"), 0.00005);

        chains = query2chains.get("4_3_1.sql");
        map = getRate(chains);
        assertEquals(0.2852112676, map.get("public.date"), 0.00005);

        chains = query2chains.get("4_3_1.sql");
        map = getRate(chains);
        assertEquals(0.040145, map.get("public.part"), 0.00005);
    }
}
