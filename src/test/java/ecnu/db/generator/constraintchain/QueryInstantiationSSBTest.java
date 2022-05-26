package ecnu.db.generator.constraintchain;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.generator.constraintchain.filter.ConstraintChainFilterNode;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.schema.Column;
import ecnu.db.schema.ColumnManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import org.checkerframework.checker.units.qual.C;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ecnu.db.utils.CommonUtils.readFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryInstantiationSSBTest {
    private static final BigDecimal sampleSize = BigDecimal.valueOf(400_0000L);

    @ParameterizedTest
    @ValueSource(strings = {"src/test/resources/data/query-instantiation/SSB/"})
    void computeTestForSSB(String configPath) throws Exception {
        // load column configuration
        ColumnManager.getInstance().setResultDir(configPath);
        ColumnManager.getInstance().loadColumnMetaData();
        ColumnManager.getInstance().loadColumnDistribution();

        // load constraintChain configuration
        String content = readFile(configPath + "constraintChain.json");
        Map<String, List<ConstraintChain>> query2chains = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
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
        ColumnManager.getInstance().prepareParameterInit(new HashSet<>(columnNames), sampleSize.intValue());

        //验证每个filterNode的执行结果
        for (ConstraintChainFilterNode filterNode : filterNodes) {
            boolean[] evaluation = filterNode.getRoot().evaluate();
            long satisfyRowCount = IntStream.range(0, evaluation.length).filter((i) -> evaluation[i]).count();
            BigDecimal bSatisfyRowCount = BigDecimal.valueOf(satisfyRowCount);
            BigDecimal realFilterProbability = bSatisfyRowCount.divide(sampleSize, CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION);
            double rate = filterNode.getProbability().subtract(realFilterProbability).doubleValue();
            assertEquals(0, rate, 0.00005, filterNode.toString());
        }
    }
}
