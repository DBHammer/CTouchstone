package ecnu.db.app;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.joininfo.JoinInfoTableManager;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.SchemaManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static ecnu.db.utils.CommonUtils.stepSize;

@CommandLine.Command(name = "generate", description = "generate database according to gathered information",
        mixinStandardHelpOptions = true, sortOptions = false)
class DataGenerator implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(DataGenerator.class);
    @CommandLine.Option(names = {"-c", "--config_path"}, required = true, description = "the config path for data generation")
    private String configPath;
    @CommandLine.Option(names = {"-o", "--output_path"}, description = "output path for data and join info")
    private String outputPath;
    @CommandLine.Option(names = {"-i", "--generator_id"}, description = "the id of current generator")
    private int generatorId;
    @CommandLine.Option(names = {"-n", "--num"}, description = "size of generators")
    private int generatorNum;

    @Override
    public Integer call() throws Exception {
        SchemaManager.getInstance().setResultDir(configPath);
        SchemaManager.getInstance().loadSchemaInfo();
        ColumnManager.getInstance().setResultDir(configPath);
        ColumnManager.getInstance().loadColumnDistribution();
        JoinInfoTableManager.setJoinInfoTablePath(configPath);
        Map<String, List<ConstraintChain>> query2chains = CommonUtils.loadConstrainChainResult(configPath);
        int stepRange = stepSize * generatorNum;
        Multimap<String, ConstraintChain> schema2chains = getSchema2Chains(query2chains);
        for (String schemaName : SchemaManager.getInstance().createTopologicalOrder()) {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputPath + "/" + schemaName + generatorId));
            int resultStart = stepSize * generatorId;
            int tableSize = SchemaManager.getInstance().getTableSize(schemaName);
            List<String> attColumnNames = SchemaManager.getInstance().getColumnNamesNotKey(schemaName);
            while (resultStart < tableSize) {
                int range = Math.min(resultStart + stepSize, tableSize) - resultStart;
                ColumnManager.getInstance().prepareGeneration(attColumnNames, range);
                computeFksAndPkJoinInfo(resultStart, range, schema2chains.get(schemaName));
                bufferedWriter.write(transferData());
                resultStart += range + stepRange;
            }
            bufferedWriter.close();
        }
        return 0;
    }

    private static Multimap<String, ConstraintChain> getSchema2Chains(Map<String, List<ConstraintChain>> query2chains) {
        Multimap<String, ConstraintChain> schema2chains = ArrayListMultimap.create();
        for (List<ConstraintChain> chains : query2chains.values()) {
            for (ConstraintChain chain : chains) {
                schema2chains.put(chain.getTableName(), chain);
            }
        }
        return schema2chains;
    }

    /**
     * 准备好生成tuple
     *
     * @param pkStart 需要生成的数据起始
     * @param range   需要生成的数据范围
     * @param chains  约束链条
     * @throws TouchstoneException 生成失败
     */
    public void computeFksAndPkJoinInfo(int pkStart, int range, Collection<ConstraintChain> chains)
            throws TouchstoneException {
        long[] pkBitMap = new long[range];
        Map<String, long[]> fkBitMap = new HashMap<>();
        for (ConstraintChain chain : chains) {
            chain.evaluate(pkBitMap, fkBitMap);
        }

    }

    //todo
    public String transferData() {
        Map<String, List<String>> columnName2Data = new HashMap<>();
//        for (String columnName : columns.keySet()) {
//            columnName2Data.put(columnName, ColumnManager.getInstance().get(columnName));
//        }
        StringBuilder data = new StringBuilder();
        //todo 添加对于数据的格式化的处理
        return data.toString();
    }
}
