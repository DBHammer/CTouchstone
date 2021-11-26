package ecnu.db.generator.constraintchain.chain;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.utils.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.readFile;

public class ConstraintChainManager {
    private static final Logger logger = LoggerFactory.getLogger(ConstraintChainManager.class);
    private static final ConstraintChainManager INSTANCE = new ConstraintChainManager();
    private static final String[] COLOR_LIST = {"#FFFFCC", "#CCFFFF", "#FFCCCC"};
    private static final String GRAPH_TEMPLATE = "digraph \"%s\" {rankdir=BT;" + System.lineSeparator() + "%s}";
    public static final String CONSTRAINT_CHAINS_INFO = "/constraintChain.json";
    private String resultDir;

    public void setResultDir(String resultDir) {
        this.resultDir = resultDir;
    }


    private ConstraintChainManager() {
    }

    public static ConstraintChainManager getInstance() {
        return INSTANCE;
    }

    public Map<String, List<ConstraintChain>> loadConstrainChainResult(String resultDir) throws IOException {
        return CommonUtils.MAPPER.readValue(readFile(resultDir + CONSTRAINT_CHAINS_INFO), new TypeReference<>() {
        });
    }

    public void storeConstraintChain(Map<String, List<ConstraintChain>> query2constraintChains) throws IOException {
        String allConstraintChainsContent = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(query2constraintChains);
        CommonUtils.writeFile(resultDir + CONSTRAINT_CHAINS_INFO, allConstraintChainsContent);
        if (new File(resultDir + "/pic/").mkdir()) {
            logger.info("创建约束链的图形化文件夹");
        }
        for (Map.Entry<String, List<ConstraintChain>> stringListEntry : query2constraintChains.entrySet()) {
            String path = resultDir + "/pic/" + stringListEntry.getKey() + ".dot";
            File file = new File(resultDir + "/pic/");
            File[] array = file.listFiles();
            assert array != null;
            if (!graphIsExists(array, stringListEntry.getKey() + ".dot")) {
                String graph = presentConstraintChains(stringListEntry.getKey(), stringListEntry.getValue());
                CommonUtils.writeFile(path, graph);
            } else {
                String oldGraph = Files.readString(Paths.get(path));
                String graph = presentConstraintChains(stringListEntry.getKey(), stringListEntry.getValue());
                if (removeData(graph).equals(removeData(oldGraph))) {
                    CommonUtils.writeFile(path, graph);
                } else {
                    String newPath = resultDir + "/pic/" + "new" + System.currentTimeMillis() + stringListEntry.getKey() + ".dot";
                    CommonUtils.writeFile(newPath + "", graph);
                    logger.warn("graph {} is different", stringListEntry.getKey());
                }
            }
        }
    }

    private String presentConstraintChains(String queryName, List<ConstraintChain> constraintChains) {
        StringBuilder graph = new StringBuilder();
        HashMap<String, ConstraintChain.SubGraph> subGraphHashMap = new HashMap<>(constraintChains.size());
        constraintChains.sort(Comparator.comparing(ConstraintChain::getTableName));
        for (int i = 0; i < constraintChains.size(); i++) {
            graph.append(constraintChains.get(i).presentConstraintChains(subGraphHashMap, COLOR_LIST[i % COLOR_LIST.length]));
        }
        String subGraphs = subGraphHashMap.values().stream().
                map(ConstraintChain.SubGraph::toString).collect(Collectors.joining(""));

        return String.format(GRAPH_TEMPLATE, queryName, subGraphs + graph);
    }

    private boolean graphIsExists(File[] array, String graphName) {
        return Arrays.stream(array).map(File::getName).anyMatch(fileName -> fileName.equals(graphName));
    }

    private String removeData(String graph) {
        //Pattern data = Pattern.compile(", data:[^}]");
        String newGraph = graph.replaceAll("\\{id:[0-9]+, data:[^}]+", "");
        newGraph = newGraph.replaceAll("key[0-9]+", "key");
        return newGraph;
    }
}
