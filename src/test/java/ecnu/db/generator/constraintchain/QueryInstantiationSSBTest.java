package ecnu.db.generator.constraintchain;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.generator.constraintchain.filter.ConstraintChainFilterNode;
import ecnu.db.schema.ColumnManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.schema.CannotFindColumnException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ecnu.db.utils.CommonUtils.readFile;
import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryInstantiationSSBTest {
    private static final BigDecimal sampleSize = BigDecimal.valueOf(400_0000L);

    @ParameterizedTest
    @CsvSource(delimiter = ';', value = {
            "src/test/resources/data/query-instantiation/SSB/;0.000005",
            "src/test/resources/data/query-instantiation/TPCDS/;0.000005"
    })
    void computeTestForSSB(String configPath, double delta) throws Exception {
        // load column configuration
        ColumnManager.getInstance().setResultDir(configPath);
        ColumnManager.getInstance().loadColumnMetaData();
        ColumnManager.getInstance().loadColumnDistribution();

        // load constraintChain configuration
        Map<String, List<ConstraintChain>> query2chains = loadConstrainChainResult(configPath);
        List<ConstraintChain> constraintChains = query2chains.values().stream().flatMap(Collection::stream).toList();

        // 筛选所有的filterNode
        List<ConstraintChainFilterNode> filterNodes = constraintChains.stream()
                .map(ConstraintChain::getNodes)
                .flatMap(Collection::stream)
                .filter(ConstraintChainFilterNode.class::isInstance)
                .map(ConstraintChainFilterNode.class::cast).toList();

        // 筛选涉及到的列名
        Set<String> columnNames = filterNodes.stream()
                .map(ConstraintChainFilterNode::getColumns)
                .flatMap(Collection::stream).collect(Collectors.toSet());

        // 生成测试数据集
        ColumnManager.getInstance().prepareParameterInit(columnNames, sampleSize.intValue());

        //验证每个filterNode的执行结果
        filterNodes.stream().parallel().forEach(filterNode -> {
            boolean[] evaluation;
            try {
                evaluation = filterNode.getRoot().evaluate();
            } catch (CannotFindColumnException e) {
                throw new RuntimeException(e);
            }
            long satisfyRowCount = IntStream.range(0, evaluation.length).filter((i) -> evaluation[i]).count();
            BigDecimal bSatisfyRowCount = BigDecimal.valueOf(satisfyRowCount);
            BigDecimal realFilterProbability = bSatisfyRowCount.divide(sampleSize, CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION);
            double rate = filterNode.getProbability().subtract(realFilterProbability).doubleValue();
            assertEquals(0, rate, delta, filterNode.toString());
        });
    }

    private Map<String, List<ConstraintChain>> loadConstrainChainResult(String resultDir) throws IOException {
        String path = resultDir + "/workload";
        File sqlDic = new File(path);
        File[] sqlArray = sqlDic.listFiles();
        assert sqlArray != null;
        Map<String, List<ConstraintChain>> result = new HashMap<>();
        for (File file : sqlArray) {
            File[] graphArray = file.listFiles();
            assert graphArray != null;
            for (File file1 : graphArray) {
                if (file1.getName().contains("json")) {
                    Map<String, List<ConstraintChain>> eachresult = CommonUtils.MAPPER.readValue(readFile(file1.getPath()), new TypeReference<>() {
                    });
                    result.putAll(eachresult);
                }
            }
        }
        return result;
    }
}
