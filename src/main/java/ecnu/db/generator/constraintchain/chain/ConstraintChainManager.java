package ecnu.db.generator.constraintchain.chain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ConstraintChainManager {
    private static final String[] COLOR_LIST = {"#FFFFCC", "#CCFFFF", "#FFCCCC"};
    private static final String GRAPH_TEMPLATE = "digraph \"%s\" {rankdir=BT;" + System.lineSeparator() + "%s}";

    public static String presentConstraintChains(String queryName, List<ConstraintChain> constraintChains) {
        StringBuilder graph = new StringBuilder();
        HashMap<String, SubGraph> subGraphHashMap = new HashMap<>(constraintChains.size());
        for (int i = 0; i < constraintChains.size(); i++) {
            ConstraintChain constraintChain = constraintChains.get(i);
            String color = "[style=filled, color=\"" + COLOR_LIST[i % COLOR_LIST.length] + "\"];" + System.lineSeparator();
            String lastNodeInfo = "";
            double lastProbability = 0;
            for (ConstraintChainNode node : constraintChain.getNodes()) {
                String currentNodeInfo;
                double currentProbability = 0;
                switch (node.constraintChainNodeType) {
                    case FILTER -> {
                        currentNodeInfo = "\"" + node + "\"";
                        currentProbability = ((ConstraintChainFilterNode) node).getProbability().doubleValue();
                        graph.append("\t").append(currentNodeInfo).append(color);
                    }
                    case FK_JOIN -> {
                        ConstraintChainFkJoinNode fkJoinNode = ((ConstraintChainFkJoinNode) node);
                        long tag = fkJoinNode.getPkTag();
                        String pkCols = fkJoinNode.getRefCols().split("\\.")[2];
                        currentNodeInfo = "\"Fk" + fkJoinNode.getLocalCols().split("\\.")[2] + tag + "\"";
                        String subGraphTag = "cluster" + pkCols + tag;
                        currentProbability = fkJoinNode.getProbability().doubleValue();
                        if (!subGraphHashMap.containsKey(subGraphTag)) {
                            subGraphHashMap.put(subGraphTag, new SubGraph(subGraphTag));
                        }
                        subGraphHashMap.get(subGraphTag).fkInfo = currentNodeInfo + color;
                    }
                    case PK_JOIN -> {
                        ConstraintChainPkJoinNode pkJoinNode = ((ConstraintChainPkJoinNode) node);
                        long pkTag = pkJoinNode.getPkTag();
                        String locPks = pkJoinNode.getPkColumns()[0];
                        currentNodeInfo = "\"Pk" + locPks + pkTag + "\"";
                        String localSubGraph = "cluster" + locPks + pkTag;
                        if (!subGraphHashMap.containsKey(localSubGraph)) {
                            subGraphHashMap.put(localSubGraph, new SubGraph(localSubGraph));
                        }
                        subGraphHashMap.get(localSubGraph).pkInfo = currentNodeInfo + color;
                    }
                    default -> throw new UnsupportedOperationException();
                }
                if (!"".equals(lastNodeInfo)) {
                    graph.append("\t").append(lastNodeInfo).append("->").append(currentNodeInfo).append("[label=\"")
                            .append(lastProbability).append("\"];").append(System.lineSeparator());
                } else {
                    graph.append("\t\"").append(constraintChain.getTableName()).append("\"[shape=box,style=filled, color=\"")
                            .append(COLOR_LIST[i % COLOR_LIST.length]).append("\"];").append(System.lineSeparator());
                    graph.append("\t\"").append(constraintChain.getTableName()).append("\"->").append(currentNodeInfo).append(System.lineSeparator());
                }
                lastNodeInfo = currentNodeInfo;
                lastProbability = currentProbability;
            }
            if (!lastNodeInfo.startsWith("\"Pk")) {
                graph.append("\t").append("RESULT").append(color).append("\t").append(lastNodeInfo).append("->")
                        .append("RESULT").append("[label=\"").append(lastProbability).append("\"];").append(System.lineSeparator());
            }
        }
        StringBuilder subGraphInfo = new StringBuilder();
        List<SubGraph> subGraphs = new ArrayList<>(subGraphHashMap.values());
        for (int size = subGraphs.size() - 1; size >= 0; size--) {
            subGraphInfo.append(subGraphs.get(size));
        }
        return String.format(GRAPH_TEMPLATE, queryName, subGraphInfo.toString() + graph);
    }

    private static class SubGraph {
        private final String joinTag;
        private String pkInfo;
        private String fkInfo;

        public SubGraph(String joinTag) {
            this.joinTag = joinTag;
        }

        @Override
        public String toString() {
            return String.format("\tsubgraph \"%s\" {" + System.lineSeparator() + "\t\t%s\t\t%s\t}"
                    + System.lineSeparator(), joinTag, pkInfo, fkInfo);
        }
    }
}
