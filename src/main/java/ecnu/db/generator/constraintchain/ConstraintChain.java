package ecnu.db.generator.constraintchain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import ecnu.db.LanguageManager;
import ecnu.db.generator.ConstructCpModel;
import ecnu.db.generator.constraintchain.agg.ConstraintChainAggregateNode;
import ecnu.db.generator.constraintchain.filter.ConstraintChainFilterNode;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.join.ConstraintChainFkJoinNode;
import ecnu.db.generator.constraintchain.join.ConstraintChainPkJoinNode;
import ecnu.db.generator.joininfo.JoinStatus;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.TableManager;
import ecnu.db.utils.exception.schema.CannotFindColumnException;
import ecnu.db.utils.exception.schema.CannotFindSchemaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/**
 * @author wangqingshuai
 */
public class ConstraintChain {

    private final List<ConstraintChainNode> nodes = new ArrayList<>();
    private final Logger logger = LoggerFactory.getLogger(ConstraintChain.class);

    @JsonIgnore
    private final Set<String> joinTables = new HashSet<>();
    private String tableName;

    public ConstraintChain() {
    }

    public ConstraintChain(String tableName) {
        this.tableName = tableName;
    }

    public void addJoinTable(String tableName) {
        joinTables.add(tableName);
    }

    public Set<String> getJoinTables() {
        return joinTables;
    }

    public void addNode(ConstraintChainNode node) {
        nodes.add(node);
    }

    public List<ConstraintChainNode> getNodes() {
        return nodes;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @JsonIgnore
    private final ResourceBundle rb = LanguageManager.getInstance().getRb();

    @JsonIgnore
    public List<Parameter> getParameters() {
        return nodes.stream().filter(ConstraintChainFilterNode.class::isInstance)
                .map(ConstraintChainFilterNode.class::cast)
                .map(ConstraintChainFilterNode::getParameters)
                .flatMap(Collection::stream).toList();
    }

    @Override
    public String toString() {
        return "{tableName:" + tableName + ",nodes:" + nodes + "}";
    }

    /**
     * 给定range空间 计算filter的状态
     *
     * @param range 批大小
     * @return filter状态
     */
    public boolean[] evaluateFilterStatus(int range) {
        if (nodes.get(0).getConstraintChainNodeType() == ConstraintChainNodeType.FILTER) {
            try {
                return ((ConstraintChainFilterNode) nodes.get(0)).evaluate();
            } catch (CannotFindColumnException e) {
                e.printStackTrace();
                return new boolean[0];
            }
        } else {
            boolean[] ret = new boolean[range];
            Arrays.fill(ret, true);
            return ret;
        }
    }

    public boolean hasFkNode() {
        return nodes.stream().anyMatch(node -> node.getConstraintChainNodeType() == ConstraintChainNodeType.FK_JOIN);
    }

    public boolean hasPkNode() {
        return nodes.stream().anyMatch(node -> node.getConstraintChainNodeType() == ConstraintChainNodeType.PK_JOIN);
    }

    public boolean hasCardinalityConstraint() {
        boolean hasCardinalityConstraint;
        hasCardinalityConstraint = nodes.stream()
                .filter(node -> node.getConstraintChainNodeType() == ConstraintChainNodeType.FK_JOIN)
                .map(ConstraintChainFkJoinNode.class::cast)
                .anyMatch(node -> node.getType().hasCardinalityConstraint());
        hasCardinalityConstraint |= nodes.stream()
                .filter(node -> node.getConstraintChainNodeType() == ConstraintChainNodeType.AGGREGATE)
                .map(ConstraintChainAggregateNode.class::cast)
                .anyMatch(node -> node.getGroupKey() != null);
        return hasCardinalityConstraint;
    }

    public void computeVectorStatus(List<List<Map.Entry<boolean[], Long>>> fkStatus, boolean[] filterStatus) {
        for (ConstraintChainNode node : nodes) {
            if (node.getConstraintChainNodeType() == ConstraintChainNodeType.FK_JOIN) {
                ConstraintChainFkJoinNode fkJoinNode = (ConstraintChainFkJoinNode) node;
                int joinStatusIndex = fkJoinNode.joinStatusIndex;
                int joinStatusLocation = fkJoinNode.joinStatusLocation;
                boolean status = !fkJoinNode.getType().isAnti();
                for (int i = 0; i < fkStatus.size(); i++) {
                    if (filterStatus[i]) {
                        filterStatus[i] = fkStatus.get(i).get(joinStatusIndex).getKey()[joinStatusLocation] == status;
                    }
                }
            }
        }
    }

    @JsonIgnore
    public List<ConstraintChainAggregateNode> getAggNodes() {
        return nodes.stream().filter(constraintChainNode ->
                        constraintChainNode.getConstraintChainNodeType() == ConstraintChainNodeType.AGGREGATE)
                .map(ConstraintChainAggregateNode.class::cast).toList();
    }

    @JsonIgnore
    public List<ConstraintChainFkJoinNode> getFkNodes() {
        return nodes.stream().filter(constraintChainNode ->
                        constraintChainNode.getConstraintChainNodeType() == ConstraintChainNodeType.FK_JOIN)
                .map(ConstraintChainFkJoinNode.class::cast).toList();
    }

    private boolean[] getAllJoinStatus(int joinStatusIndex, int joinStatusLocation,
                                       List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2TransferStatus) {
        boolean[] allJoinStatus = new boolean[filterStatus2TransferStatus.size()];
        int i = 0;
        for (Map.Entry<JoinStatus, List<boolean[]>> status2TransferStatus : filterStatus2TransferStatus) {
            allJoinStatus[i++] = status2TransferStatus.getValue().get(joinStatusIndex)[joinStatusLocation];
        }
        return allJoinStatus;
    }

    public void addConstraint2Model(int filterIndex, int filterSize, int unFilterSize,
                                    List<Map<JoinStatus, Long>> statusHash2Size,
                                    List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2TransferStatus) throws CannotFindSchemaException {
        // 每一组填充状态是否能到达这个算子
        boolean[] canBeInput = new boolean[filterStatus2TransferStatus.size()];
        Arrays.fill(canBeInput, true);
        for (ConstraintChainNode node : nodes) {
            // filter的状态 根据其filter status对应的index的位置获得
            switch (node.getConstraintChainNodeType()) {
                case FILTER -> {
                    int i = 0;
                    for (Map.Entry<JoinStatus, List<boolean[]>> status2PkStatus : filterStatus2TransferStatus) {
                        canBeInput[i++] = status2PkStatus.getKey().status()[filterIndex];
                    }
                }
                case FK_JOIN -> {
                    ConstraintChainFkJoinNode fkJoinNode = (ConstraintChainFkJoinNode) node;
                    int joinStatusIndex = fkJoinNode.joinStatusIndex;
                    int joinStatusLocation = fkJoinNode.joinStatusLocation;
                    if (!fkJoinNode.getType().isSemi()) {
                        boolean[] allJoinStatus = getAllJoinStatus(joinStatusIndex, joinStatusLocation, filterStatus2TransferStatus);
                        boolean status = !fkJoinNode.getType().isAnti();
                        if (fkJoinNode.getProbabilityWithFailFilter() != null) {
                            BigDecimal bIndexJoinSize = BigDecimal.valueOf(unFilterSize).multiply(fkJoinNode.getProbabilityWithFailFilter());
                            int indexJoinSize = bIndexJoinSize.setScale(0, RoundingMode.HALF_UP).intValue();
                            List<Integer> validIndexes = new ArrayList<>();
                            for (int i = 0; i < filterStatus2TransferStatus.size(); i++) {
                                if (!canBeInput[i] && allJoinStatus[i] == status) {
                                    validIndexes.add(i);
                                }
                            }
                            ConstructCpModel.addJoinCardinalityConstraint(validIndexes, indexJoinSize);
                            logger.info("indexjoin输出的数据量为:{}, 为第{}个表的第{}个状态", indexJoinSize, joinStatusIndex, joinStatusLocation);
                        }
                        BigDecimal bFilterSize = BigDecimal.valueOf(filterSize).multiply(fkJoinNode.getProbability());
                        filterSize = bFilterSize.setScale(0, RoundingMode.HALF_UP).intValue();
                        List<Integer> validIndexes = new ArrayList<>();
                        for (int i = 0; i < allJoinStatus.length; i++) {
                            if (canBeInput[i] && allJoinStatus[i] == status) {
                                validIndexes.add(i);
                            } else {
                                canBeInput[i] = false;
                            }
                        }
                        logger.info("输出的数据量为:{}, 为第{}个表的第{}个状态", filterIndex, joinStatusIndex, joinStatusLocation);
                        ConstructCpModel.addJoinCardinalityConstraint(validIndexes, filterSize);
                    }
                    if (fkJoinNode.getType().hasCardinalityConstraint()) {
                        // 获取join对应的位置
                        int pkSize = fkJoinNode.getPkDistinctProbability().multiply(BigDecimal.valueOf(filterSize)).setScale(0, RoundingMode.HALF_UP).intValue();
                        int tableSize = TableManager.getInstance().getTableSizeWithFK(fkJoinNode.getLocalCols());
                        int fkNum = ColumnManager.getInstance().getNdv(fkJoinNode.getLocalCols());
                        logger.info(rb.getString("StateOfTable"), joinStatusIndex, joinStatusLocation, pkSize);
                        ConstructCpModel.addJoinDistinctConstraint(joinStatusIndex, joinStatusLocation,
                                pkSize, tableSize, fkNum, statusHash2Size, canBeInput, filterStatus2TransferStatus);
                    }
                }
                case AGGREGATE -> {
                    ConstraintChainAggregateNode aggregateNode = (ConstraintChainAggregateNode) node;
                    if (aggregateNode.joinStatusIndex >= 0) {
                        int pkSize = aggregateNode.getAggProbability().multiply(BigDecimal.valueOf(filterSize)).setScale(0, RoundingMode.HALF_UP).intValue();
                        logger.info(rb.getString("LocationOfAgg"), aggregateNode.joinStatusIndex, pkSize);
                        int tableSize = TableManager.getInstance().getTableSizeWithFK(aggregateNode.getGroupKey().get(0));
                        int fkNum = ColumnManager.getInstance().getNdv(aggregateNode.getGroupKey().get(0));
                        ConstructCpModel.addJoinDistinctConstraint(aggregateNode.joinStatusIndex, -1,
                                pkSize, tableSize, fkNum, statusHash2Size, canBeInput, filterStatus2TransferStatus);
                    }
                }
                default -> {
                }
            }
        }
    }

    public StringBuilder presentConstraintChains(Map<String, SubGraph> subGraphHashMap, String color) {
        String lastNodeInfo = "";
        double lastProbability = 0;
        String conditionColor = String.format("[style=filled, color=\"%s\"];%n", color);
        String tableColor = String.format("[shape=box,style=filled, color=\"%s\"];%n", color);
        StringBuilder graph = new StringBuilder();
        for (ConstraintChainNode node : nodes) {
            String currentNodeInfo;
            double currentProbability = 0;
            switch (node.constraintChainNodeType) {
                case FILTER -> {
                    currentNodeInfo = String.format("\"%s\"", node);
                    currentProbability = ((ConstraintChainFilterNode) node).getProbability().doubleValue();
                    graph.append("\t").append(currentNodeInfo).append(conditionColor);
                }
                case FK_JOIN -> {
                    ConstraintChainFkJoinNode fkJoinNode = ((ConstraintChainFkJoinNode) node);
                    String pkCols = fkJoinNode.getRefCols().split("\\.")[2];
                    currentNodeInfo = String.format("\"Fk%s%d\"", pkCols, fkJoinNode.getPkTag());
                    String subGraphTag = String.format("cluster%s%d", pkCols, fkJoinNode.getPkTag());
                    currentProbability = fkJoinNode.getProbability().doubleValue();
                    subGraphHashMap.putIfAbsent(subGraphTag, new SubGraph(subGraphTag));
                    subGraphHashMap.get(subGraphTag).fkInfo = currentNodeInfo + conditionColor;
                    subGraphHashMap.get(subGraphTag).joinLabel = switch (fkJoinNode.getType()) {
                        case INNER_JOIN -> "eq join";
                        case SEMI_JOIN -> "semi join: " + fkJoinNode.getPkDistinctProbability();
                        case OUTER_JOIN -> "outer join: " + fkJoinNode.getPkDistinctProbability();
                        case ANTI_SEMI_JOIN -> "anti semi join";
                        case ANTI_JOIN -> "anti join";
                    };
                    if (fkJoinNode.getProbabilityWithFailFilter() != null) {
                        subGraphHashMap.get(subGraphTag).joinLabel = String.format("%s filterWithCannotJoin: %2$,.4f",
                                subGraphHashMap.get(subGraphTag).joinLabel,
                                fkJoinNode.getProbabilityWithFailFilter());
                    }
                }
                case PK_JOIN -> {
                    ConstraintChainPkJoinNode pkJoinNode = ((ConstraintChainPkJoinNode) node);
                    String locPks = pkJoinNode.getPkColumns()[0];
                    currentNodeInfo = String.format("\"Pk%s%d\"", locPks, pkJoinNode.getPkTag());
                    String localSubGraph = String.format("cluster%s%d", locPks, pkJoinNode.getPkTag());
                    subGraphHashMap.putIfAbsent(localSubGraph, new SubGraph(localSubGraph));
                    subGraphHashMap.get(localSubGraph).pkInfo = currentNodeInfo + conditionColor;
                }
                case AGGREGATE -> {
                    ConstraintChainAggregateNode aggregateNode = ((ConstraintChainAggregateNode) node);
                    List<String> keys = aggregateNode.getGroupKey();
                    currentProbability = aggregateNode.getAggProbability().doubleValue();
                    currentNodeInfo = String.format("\"GroupKey:%s\"", keys == null ? "" : String.join(",", keys));
                    graph.append("\t").append(currentNodeInfo).append(conditionColor);
                    if (aggregateNode.getAggFilter() != null) {
                        if (!lastNodeInfo.isBlank()) {
                            graph.append(String.format("\t%s->%s[label=\"%3$,.4f\"];%n", lastNodeInfo, currentNodeInfo, lastProbability));
                        } else {
                            graph.append(String.format("\t\"%s\"%s", tableName, tableColor));
                            graph.append(String.format("\t\"%s\"->%s[label=\"1.0\"]%n", tableName, currentNodeInfo));
                        }
                        lastNodeInfo = currentNodeInfo;
                        lastProbability = currentProbability;
                        ConstraintChainFilterNode aggFilter = aggregateNode.getAggFilter();
                        currentNodeInfo = String.format("\"%s\"", aggFilter);
                        graph.append("\t").append(currentNodeInfo).append(conditionColor);
                        currentProbability = aggFilter.getProbability().doubleValue();
                    }
                }
                default -> throw new UnsupportedOperationException();
            }
            if (!lastNodeInfo.isBlank()) {
                graph.append(String.format("\t%s->%s[label=\"%3$,.4f\"];%n", lastNodeInfo, currentNodeInfo, lastProbability));
            } else {
                graph.append(String.format("\t\"%s\"%s", tableName, tableColor));
                graph.append(String.format("\t\"%s\"->%s[label=\"1.0\"]%n", tableName, currentNodeInfo));
            }
            lastNodeInfo = currentNodeInfo;
            lastProbability = currentProbability;
        }
        if (!lastNodeInfo.startsWith("\"Pk")) {
            graph.append("\t").append("RESULT").append(conditionColor);
            graph.append(String.format("\t%s->RESULT[label=\"%2$,.4f\"]%n", lastNodeInfo, lastProbability));
        }
        return graph;
    }

    @JsonIgnore
    public int getJoinTag() {
        int joinTag = Integer.MIN_VALUE;
        for (ConstraintChainNode node : nodes) {
            if (node.getConstraintChainNodeType() == ConstraintChainNodeType.PK_JOIN) {
                if (joinTag == Integer.MIN_VALUE) {
                    joinTag = ((ConstraintChainPkJoinNode) node).getPkTag();
                } else {
                    throw new UnsupportedOperationException(rb.getString("DoublePKInConstraintChain"));
                }
            }
        }
        return joinTag;
    }

    static class SubGraph {
        private final String joinTag;
        String pkInfo;
        String fkInfo;
        String joinLabel;

        public SubGraph(String joinTag) {
            this.joinTag = joinTag;
        }

        @Override
        public String toString() {
            return String.format("""
                    subgraph "%s" {
                            %s
                            %slabel="%s";labelloc=b;
                    }""".indent(4), joinTag, pkInfo, fkInfo, joinLabel);
        }
    }
}
