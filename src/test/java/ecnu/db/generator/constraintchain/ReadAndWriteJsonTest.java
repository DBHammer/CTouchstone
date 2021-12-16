package ecnu.db.generator.constraintchain;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.generator.constraintchain.chain.ConstraintChain;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.utils.CommonUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static ecnu.db.utils.CommonUtils.readFile;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReadAndWriteJsonTest {
    private static final String dir = "src/test/resources/data/query-instantiation/basic/";

    @Test
    void writeTestConstraintChain() throws IOException {
        String content = readFile(dir + "constraintChain.json");
        Map<String, List<ConstraintChain>> query2chains = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
        String contentWrite = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(query2chains);
        assertEquals(content, contentWrite);
    }

    @Test
    void writeTestStringTemplate() throws IOException {
        String content = readFile(dir + "StringTemplate.json");
        Map<String, Map<Long, boolean[]>> columName2StringTemplate = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
        String contentWrite = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(columName2StringTemplate);
        assertEquals(content, contentWrite);
    }

    @Test
    void writeTestDistribution() throws IOException {
        String content = readFile(dir + "distribution.json");
        Map<String, List<Map.Entry<Long, BigDecimal>>> bucket2Probabilities = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
        String contentWrite = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(bucket2Probabilities);
        assertEquals(content, contentWrite);
    }

    @Test
    void writeTestEqDistribution() throws IOException {
        String content = readFile(dir + "eq_distribution.json");
        Map<String, Map<Long, BigDecimal>> eq2Probabilities = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
        String contentWrite = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(eq2Probabilities);
        assertEquals(content, contentWrite);
    }

    @Test
    void writeTestBoundPara() throws IOException {
        String content = readFile(dir + "boundPara.json");
        Map<String, List<Parameter>> boundPara = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
        String contentWrite = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(boundPara);
        assertEquals(content, contentWrite);
    }
}
