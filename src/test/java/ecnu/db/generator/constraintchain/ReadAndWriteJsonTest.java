package ecnu.db.generator.constraintchain;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.generator.constraintchain.chain.ConstraintChain;
import ecnu.db.utils.CommonUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ecnu.db.utils.CommonUtils.readFile;

public class ReadAndWriteJsonTest {
    Map<String, List<ConstraintChain>> query2chains;
    Map<String, Map<Long, boolean[]>> columName2StringTemplate = new HashMap<>();
    Map<String, List<Map.Entry<Long, BigDecimal>>> bucket2Probabilities = new HashMap<>();
    Map<String, Map<Long, BigDecimal>> eq2Probabilities = new HashMap<>();
    int samplingSize = 4000_000;

    @Disabled
    @Test
    void writeTest() throws IOException {
        String content = readFile("src/test/resources/data/query-instantiation/basic/constraintChain.json");
        query2chains = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
        String contentWrite = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(query2chains);
        CommonUtils.writeFile("src/test/resources/data/query-instantiation/basic/constraintChain1.json", contentWrite);

        content = readFile("src/test/resources/data/query-instantiation/basic/StringTemplate.json");
        columName2StringTemplate = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
        contentWrite = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(columName2StringTemplate);
        CommonUtils.writeFile("src/test/resources/data/query-instantiation/basic/stringTemplate1.json", contentWrite);

        content = readFile("src/test/resources/data/query-instantiation/basic/distribution.json");
        bucket2Probabilities = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
        contentWrite = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(bucket2Probabilities);
        CommonUtils.writeFile("src/test/resources/data/query-instantiation/basic/distribtion1.json", contentWrite);

        content = readFile("src/test/resources/data/query-instantiation/basic/eq_distribution.json");
        eq2Probabilities = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
        contentWrite = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(eq2Probabilities);
        CommonUtils.writeFile("src/test/resources/data/query-instantiation/basic/eq_distribtion1.json", contentWrite);
    }
}
