package ecnu.db.analyzer;

import ecnu.db.LanguageManager;
import ecnu.db.generator.constraintchain.ConstraintChain;
import ecnu.db.generator.constraintchain.ConstraintChainManager;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.schema.ColumnManager;
import ecnu.db.utils.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import static ecnu.db.analyzer.TaskConfigurator.queryInstantiation;
import static ecnu.db.utils.CommonUtils.matchPattern;

@CommandLine.Command(name = "instantiate", description = "instantiate the query")
public class QueryInstantiate implements Callable<Integer> {

    private static final Pattern PATTERN = Pattern.compile("'([0-9]+)'");
    private static final String WORKLOAD_DIR = "/workload";
    private static final String QUERIES = "/queries";
    private final Logger logger = LoggerFactory.getLogger(QueryInstantiate.class);
    private final ResourceBundle rb = LanguageManager.getInstance().getRb();
    @CommandLine.Option(names = {"-c", "--config_path"}, required = true, description = "the config path for instantiating query ")
    private String configPath;
    @CommandLine.Option(names = {"-s", "--sampling_size"}, defaultValue = "4000000", description = "samplingSize")
    private String samplingSize;
    private Map<String, List<ConstraintChain>> query2constraintChains;

    @Override
    public Integer call() throws IOException {
        init();
        Map<String, String> queryName2QueryTemplates = getQueryName2QueryTemplates();
        logger.info(rb.getString("StartInstantiatingTheQueryPlan"));
        List<ConstraintChain> allConstraintChains = query2constraintChains.values().stream().flatMap(Collection::stream).toList();
        Map<Integer, Parameter> id2Parameter = queryInstantiation(allConstraintChains, Integer.parseInt(samplingSize));
        logger.info(rb.getString("TheInstantiatedQueryPlanSucceed"), id2Parameter.values());
        logger.info(rb.getString("StartPersistentQueryPlanWithNewDataDistribution"));
        ConstraintChainManager.getInstance().storeConstraintChain(query2constraintChains);
        ColumnManager.getInstance().storeColumnDistribution();
        logger.info(rb.getString("PersistentQueryPlanCompleted"));
        logger.info(rb.getString("StartPopulatingTheQueryTemplate"));
        writeQuery(queryName2QueryTemplates, id2Parameter);
        logger.info(rb.getString("FillInTheQueryTemplateComplete"));
        if (id2Parameter.size() > 0) {
            logger.info(rb.getString("TheParametersThatWereNotSuccessfullyReplaced"), id2Parameter.values());
        }
        return null;
    }

    private void init() throws IOException {
        ColumnManager.getInstance().setResultDir(configPath);
        ColumnManager.getInstance().loadColumnMetaData();
        //载入约束链，并进行transform
        ConstraintChainManager.getInstance().setResultDir(configPath);
        query2constraintChains = ConstraintChainManager.getInstance().loadConstrainChainResult(configPath);
        ConstraintChainManager.getInstance().cleanConstrainChains(query2constraintChains);
    }

    public void writeQuery(Map<String, String> queryName2QueryTemplates, Map<Integer, Parameter> id2Parameter) throws IOException {
        for (Map.Entry<String, String> queryName2QueryTemplate : queryName2QueryTemplates.entrySet()) {
            String query = queryName2QueryTemplate.getValue();
            File queryPath = new File(configPath + QUERIES);
            if(!queryPath.exists()){
                queryPath.mkdir();
            }
            String path = configPath + QUERIES + '/' + queryName2QueryTemplate.getKey();
            List<List<String>> matches = matchPattern(PATTERN, query);
            if (matches.isEmpty()) {
                CommonUtils.writeFile(path, query);
            } else {
                for (List<String> group : matches) {
                    int parameterId = Integer.parseInt(group.get(1));
                    Parameter parameter = id2Parameter.remove(parameterId);
                    if (parameter != null) {
                        String parameterData = parameter.getDataValue();
                        try {
                            if (parameterData.contains("interval")) {
                                query = query.replaceAll(group.get(0), String.format("%s", parameterData));
                            } else {
                                query = query.replaceAll(group.get(0), String.format("'%s'", parameterData));
                            }
                        } catch (IllegalArgumentException e) {
                            logger.error("query is " + query + "; group is " + group + "; parameter data is " + parameterData, e);
                        }
                    }
                    CommonUtils.writeFile(path, query);
                }
            }
        }
    }

    private Map<String, String> getQueryName2QueryTemplates() throws IOException {
        String path = configPath + WORKLOAD_DIR;
        File sqlDic = new File(path);
        File[] sqlArray = sqlDic.listFiles();
        assert sqlArray != null;
        Map<String, String> queryName2QueryTemplates = new HashMap<>();
        for (File file : sqlArray) {
            File[] eachFile = file.listFiles();
            assert eachFile != null;
            for (File sqlTemplate : eachFile) {
                if (sqlTemplate.getName().contains("Template")) {
                    String key = sqlTemplate.getName().replace("Template", "");
                    StringBuilder buffer = new StringBuilder();
                    try (BufferedReader bf = new BufferedReader(new FileReader(sqlTemplate.getPath()))) {
                        String s;
                        while ((s = bf.readLine()) != null) {//使用readLine方法，一次读一行
                            buffer.append(s.trim()).append("\n");
                        }
                    }
                    String value = buffer.toString();
                    queryName2QueryTemplates.put(key, value);
                }
            }
        }
        return queryName2QueryTemplates;
    }
}
