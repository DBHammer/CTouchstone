package ecnu.db.analyzer.online;

import ecnu.db.analyzer.online.node.*;
import ecnu.db.dbconnector.DbConnector;
import ecnu.db.generator.constraintchain.ConstraintChain;
import ecnu.db.generator.constraintchain.agg.ConstraintChainAggregateNode;
import ecnu.db.generator.constraintchain.filter.ConstraintChainFilterNode;
import ecnu.db.generator.constraintchain.filter.LogicNode;
import ecnu.db.generator.constraintchain.join.ConstraintChainFkJoinNode;
import ecnu.db.generator.constraintchain.join.ConstraintChainPkJoinNode;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.TableManager;
import ecnu.db.utils.exception.TouchstoneException;
import ecnu.db.utils.exception.analyze.UnsupportedSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION;
import static ecnu.db.utils.CommonUtils.CANONICAL_NAME_CONTACT_SYMBOL;

public class QueryAnalyzer {

    protected static final Logger logger = LoggerFactory.getLogger(QueryAnalyzer.class);
    private static final int SKIP_JOIN_TAG = -1;
    private static final int STOP_CONSTRUCT = -3;
    private static final int SKIP_SELF_JOIN = -4;
    private final AbstractAnalyzer abstractAnalyzer;
    private final DbConnector dbConnector;
    protected double skipNodeThreshold = 0.01;


    public QueryAnalyzer(AbstractAnalyzer abstractAnalyzer, DbConnector dbConnector) {
        this.abstractAnalyzer = abstractAnalyzer;
        this.dbConnector = dbConnector;
    }

    public void setSkipNodeThreshold(double skipNodeThreshold) {
        this.skipNodeThreshold = skipNodeThreshold;
    }

    public void setAliasDic(Map<String, String> aliasDic) {
        abstractAnalyzer.setAliasDic(aliasDic);
    }


    /**
     * 根据输入的列名统计非重复值的个数，进而给出该列是否为主键
     *
     * @param pkTable 需要测试的主表
     * @param pkCol   主键
     * @param fkTable 外表
     * @param fkCol   外键
     * @return 该列是否为主键
     * @throws TouchstoneException 由于逻辑错误无法判断是否为主键的异常
     * @throws SQLException        无法通过数据库SQL查询获得多列属性的ndv
     */
    private boolean isPrimaryKey(String pkTable, String pkCol, String fkTable, String fkCol) throws TouchstoneException, SQLException {
        if (TableManager.getInstance().isRefTable(fkTable, fkCol, pkTable + "." + pkCol)) {
            return true;
        }
        if (TableManager.getInstance().isRefTable(pkTable, pkCol, fkTable + "." + fkCol)) {
            return false;
        }
        if (!pkCol.contains(",")) {
            if (ColumnManager.getInstance().getNdv(pkTable + CANONICAL_NAME_CONTACT_SYMBOL + pkCol) ==
                    ColumnManager.getInstance().getNdv(fkTable + CANONICAL_NAME_CONTACT_SYMBOL + fkCol)) {
                return TableManager.getInstance().getTableSize(pkTable) < TableManager.getInstance().getTableSize(fkTable);
            } else {
                int pkTableNdv = ColumnManager.getInstance().getNdv(pkTable + CANONICAL_NAME_CONTACT_SYMBOL + pkCol);
                int fkTableNdv = ColumnManager.getInstance().getNdv(fkTable + CANONICAL_NAME_CONTACT_SYMBOL + fkCol);
                return pkTableNdv > fkTableNdv;
            }
        } else {
            int leftTableNdv = dbConnector.getMultiColNdv(pkTable, pkCol);
            int rightTableNdv = dbConnector.getMultiColNdv(fkTable, fkCol);
            if (leftTableNdv == rightTableNdv) {
                return TableManager.getInstance().getTableSize(pkTable) < TableManager.getInstance().getTableSize(fkTable);
            } else {
                return leftTableNdv > rightTableNdv;
            }
        }
    }

    /**
     * 分析一个节点，提取约束链信息
     *
     * @param node            需要分析的节点
     * @param constraintChain 约束链
     * @return 节点行数，小于零代表停止继续向上分析
     * @throws TouchstoneException 节点分析出错
     * @throws SQLException        无法收集多列主键的ndv
     */
    private int analyzeNode(ExecutionNode node, ConstraintChain constraintChain, int lastNodeLineCount) throws TouchstoneException, SQLException {
        return switch (node.getType()) {
            case join -> analyzeJoinNode((JoinNode) node, constraintChain, lastNodeLineCount);
            case filter -> analyzeSelectNode(node, constraintChain, lastNodeLineCount);
            case aggregate -> analyzeAggregateNode((AggNode) node, constraintChain, lastNodeLineCount);
        };
    }

    private int analyzeSelectNode(ExecutionNode node, ConstraintChain constraintChain, int lastNodeLineCount) throws TouchstoneException {
        LogicNode root = analyzeSelectInfo(node.getInfo());
        BigDecimal ratio = BigDecimal.valueOf(node.getOutputRows()).divide(BigDecimal.valueOf(lastNodeLineCount), BIG_DECIMAL_DEFAULT_PRECISION);
        ConstraintChainFilterNode filterNode = new ConstraintChainFilterNode(ratio, root);
        constraintChain.addNode(filterNode);
        return node.getOutputRows();
    }

    private int analyzeAggregateNode(AggNode node, ConstraintChain constraintChain, int lastNodeLineCount) throws TouchstoneException {
        // multiple aggregation
        if (!constraintChain.getTableName().equals(node.getTableName())) {
            return STOP_CONSTRUCT;
        }

        List<String> groupKeys = null;
        if (node.getInfo() != null) {
            groupKeys = new ArrayList<>(Arrays.stream(node.getInfo().trim().split(";")).toList());
        }
        BigDecimal aggProbability = BigDecimal.valueOf(node.getOutputRows()).divide(BigDecimal.valueOf(lastNodeLineCount), BIG_DECIMAL_DEFAULT_PRECISION);
        ConstraintChainAggregateNode aggregateNode = new ConstraintChainAggregateNode(groupKeys, aggProbability);

        if (node.getAggFilter() != null) {
            ExecutionNode aggNode = node.getAggFilter();
            LogicNode root = analyzeSelectInfo(aggNode.getInfo());
            BigDecimal filterProbability = BigDecimal.valueOf(aggNode.getOutputRows()).divide(BigDecimal.valueOf(node.getOutputRows()), BIG_DECIMAL_DEFAULT_PRECISION);
            aggregateNode.setAggFilter(new ConstraintChainFilterNode(filterProbability, root));
        }
        constraintChain.addNode(aggregateNode);
        return node.getOutputRows();
    }

    private int analyzeJoinNode(JoinNode node, ConstraintChain constraintChain, int lastNodeLineCount) throws TouchstoneException, SQLException {
        String[] joinColumnInfos = abstractAnalyzer.analyzeJoinInfo(node.getInfo());
        String localTable = joinColumnInfos[0];
        String localCol = joinColumnInfos[1];
        String externalTable = joinColumnInfos[2];
        String externalCol = joinColumnInfos[3];
        if (localTable.equals(externalTable)) {
            node.setJoinTag(SKIP_SELF_JOIN);
            logger.error("skip node {} due to self join", node.getInfo());
            return STOP_CONSTRUCT;
        }
        // 如果当前的join节点，不属于之前遍历的节点
        Set<String> currentJoinTables = new HashSet<>(List.of(localTable, externalTable));
        if (!currentJoinTables.contains(constraintChain.getTableName())) {
            // 如果与之前join过的表一致，且本次join无效，则继续推进
            if (currentJoinTables.removeAll(constraintChain.getJoinTables()) && node.getJoinTag() < 0) {
                return lastNodeLineCount;
            }
            // 停止继续向上访问
            else {
                return STOP_CONSTRUCT;
            }
        }
        //将本表的信息放在前面，交换位置
        if (constraintChain.getTableName().equals(externalTable)) {
            localTable = joinColumnInfos[2];
            localCol = joinColumnInfos[3];
            externalTable = joinColumnInfos[0];
            externalCol = joinColumnInfos[1];
        }
        //根据主外键分别设置约束链输出信息
        if (isPrimaryKey(localTable, localCol, externalTable, externalCol)) {
            //设置主键
            if (constraintChain.getJoinTables().contains(externalTable)) {
                logger.error("由于self join，跳过节点{}", node.getInfo());
                node.setJoinTag(SKIP_SELF_JOIN);
                return STOP_CONSTRUCT;
            } else {
                constraintChain.addJoinTable(externalTable);
            }
            if (TableManager.getInstance().getTableSize(localTable) == lastNodeLineCount && node.getPkDistinctSize() == 0) {
                logger.debug("由于输入的主键为全集，跳过节点{}", node.getInfo());
                node.setJoinTag(SKIP_JOIN_TAG);
            } else {
                node.setJoinTag(TableManager.getInstance().getJoinTag(localTable));
                ConstraintChainPkJoinNode pkJoinNode = new ConstraintChainPkJoinNode(node.getJoinTag(), localCol.split(","));
                constraintChain.addNode(pkJoinNode);
                TableManager.getInstance().setPrimaryKeys(localTable, localCol);
            }
            return lastNodeLineCount;
        } else {
            constraintChain.addJoinTable(externalTable);
            logger.debug("{} wait join tag", node.getInfo());
            long fkJoinTag = node.getJoinTag();
            logger.debug("{} get join tag", node.getInfo());
            if (fkJoinTag == SKIP_JOIN_TAG) {
                logger.debug("由于join节点对应的主键输入为全集，跳过节点{}", node.getInfo());
                return node.getOutputRows();
            } else if (fkJoinTag == SKIP_SELF_JOIN) {
                logger.error("由于self join，跳过节点{}", node.getInfo());
                return STOP_CONSTRUCT;
            }
            TableManager.getInstance().setForeignKeys(localTable, localCol, externalTable, externalCol);
            BigDecimal probability = BigDecimal.valueOf(node.getOutputRows()).divide(BigDecimal.valueOf(lastNodeLineCount), BIG_DECIMAL_DEFAULT_PRECISION);
            ConstraintChainFkJoinNode fkJoinNode = new ConstraintChainFkJoinNode(localTable + "." + localCol, externalTable + "." + externalCol, fkJoinTag, probability);
            if (node.isAntiJoin()) {
                fkJoinNode.setAntiJoin();
            }
            fkJoinNode.setPkDistinctProbability(node.getPkDistinctSize());
            if (node.getRightNode().getType() == ExecutionNodeType.filter && node.getRightNode().getInfo() != null &&
                    ((FilterNode) node.getRightNode()).isIndexScan()) {
                int tableSize = TableManager.getInstance().getTableSize(node.getRightNode().getTableName());
                int rowsRemovedByScanFilter = tableSize - node.getRightNode().getOutputRows();
                BigDecimal probabilityWithFailFilter = new BigDecimal(node.getRowsRemoveByFilterAfterJoin()).divide(BigDecimal.valueOf(rowsRemovedByScanFilter), BIG_DECIMAL_DEFAULT_PRECISION);
                fkJoinNode.setProbabilityWithFailFilter(probabilityWithFailFilter);
            }
            if (node.isSemiJoin()) {
                fkJoinNode.setPkDistinctSize(node.getOutputRows());
            }
            constraintChain.addNode(fkJoinNode);
            return node.getOutputRows();
        }
    }

    /**
     * 获取一条路径上的约束链
     *
     * @param path 需要处理的路径
     * @return 获取的约束链
     */
    private ConstraintChain extractConstraintChain(List<ExecutionNode> path, Set<ExecutionNode> inputNodes) throws TouchstoneException, SQLException {
        if (path == null || path.isEmpty()) {
            throw new TouchstoneException(String.format("非法的path输入 '%s'", path));
        }
        ExecutionNode headNode = path.get(0);
        ConstraintChain constraintChain;
        int lastNodeLineCount;
        //分析约束链的第一个node
        if (headNode.getType() == ExecutionNodeType.filter) {
            constraintChain = new ConstraintChain(headNode.getTableName());
            FilterNode filterNode = (FilterNode) headNode;
            if (filterNode.getInfo() != null) {
                LogicNode result = analyzeSelectInfo(filterNode.getInfo());
                if (filterNode.isIndexScan()) {
                    result.removeOtherTablesOperation(filterNode.getTableName());
                    int rowsAfterFilter = dbConnector.getRowsAfterFilter(filterNode.getTableName(), result.toSQL());
                    filterNode.setOutputRows(rowsAfterFilter);
                }
                BigDecimal tableSize = BigDecimal.valueOf(TableManager.getInstance().getTableSize(filterNode.getTableName()));
                BigDecimal ratio = BigDecimal.valueOf(filterNode.getOutputRows()).divide(tableSize, BIG_DECIMAL_DEFAULT_PRECISION);
                constraintChain.addNode(new ConstraintChainFilterNode(ratio, result));
            }
            lastNodeLineCount = filterNode.getOutputRows();
        } else {
            throw new TouchstoneException(String.format("底层节点 %s 只能为select或者scan", headNode.getId()));
        }
        inputNodes.add(headNode);
        for (ExecutionNode executionNode : path.subList(1, path.size())) {
            try {
                lastNodeLineCount = analyzeNode(executionNode, constraintChain, lastNodeLineCount);
                inputNodes.add(executionNode);
                if (lastNodeLineCount == STOP_CONSTRUCT) {
                    if (constraintChain.getNodes().isEmpty()) {
                        return null;
                    } else {
                        break;
                    }
                } else if (lastNodeLineCount < 0) {
                    throw new UnsupportedOperationException();
                }
            } catch (TouchstoneException e) {
                e.printStackTrace();
                // 小于设置的阈值以后略去后续的节点
                if (executionNode.getOutputRows() * 1.0 / TableManager.getInstance().getTableSize(executionNode.getTableName()) < skipNodeThreshold) {
                    logger.error("提取约束链失败", e);
                    logger.info(String.format("%s, 但节点行数与tableSize比值小于阈值，跳过节点%s", e.getMessage(), executionNode));
                    return constraintChain;
                }
            }
        }
        return constraintChain.getNodes().isEmpty() ? null : constraintChain;
    }


    /**
     * 将树结构根据叶子节点分割为不同的path
     *
     * @param currentNode 需要处理的查询树节点
     * @param paths       需要返回的路径
     */
    private void getPathsIterate(ExecutionNode currentNode, List<List<ExecutionNode>> paths, List<ExecutionNode> currentPath) {
        currentPath.add(0, currentNode);
        if (currentNode.getLeftNode() == null && currentNode.getRightNode() == null) {
            paths.add(new ArrayList<>(currentPath));
        }
        if (currentNode.getLeftNode() != null) {
            getPathsIterate(currentNode.getLeftNode(), paths, currentPath);
        }
        if (currentNode.getRightNode() != null) {
            getPathsIterate(currentNode.getRightNode(), paths, currentPath);
        }
        currentPath.remove(0);
    }

    /**
     * 获取查询树的约束链信息和表信息
     *
     * @param query 查询语句
     * @return 该查询树结构出的约束链信息和表信息
     */
    public List<List<ConstraintChain>> extractQuery(String query) throws SQLException {
        List<String[]> queryPlan = dbConnector.explainQuery(query);
        List<List<String[]>> queryPlans = abstractAnalyzer.splitQueryPlan(queryPlan);
        List<ExecutionNode> executionTrees = new LinkedList<>();
        try {
            for (List<String[]> plan : queryPlans) {
                executionTrees.add(abstractAnalyzer.getExecutionTree(plan));
                List<Map.Entry<String, String>> tableNameAndFilterInfos = abstractAnalyzer.splitQueryPlanForMultipleAggregate();
                if (tableNameAndFilterInfos != null) {
                    for (Map.Entry<String, String> tableNameAndFilterInfo : tableNameAndFilterInfos) {
                        executionTrees.add(abstractAnalyzer.getExecutionTree(dbConnector.explainQuery(tableNameAndFilterInfo)));
                    }
                }
            }
        } catch (TouchstoneException | IOException e) {
            if (queryPlan != null && !queryPlan.isEmpty()) {
                String queryPlanContent = queryPlan.stream().map(plan -> String.join("\t", plan))
                        .collect(Collectors.joining(System.lineSeparator()));
                logger.error("查询树抽取失败");
                logger.error(queryPlanContent, e);
            }
        }
        List<List<ConstraintChain>> constraintChains = new ArrayList<>();
        for (ExecutionNode executionTree : executionTrees) {
            //获取查询树的所有路径
            List<List<ExecutionNode>> paths = new ArrayList<>();
            getPathsIterate(executionTree, paths, new LinkedList<>());
            // 并发处理约束链
            HashSet<ExecutionNode> allNodes = new HashSet<>();
            paths.forEach(allNodes::addAll);
            Set<ExecutionNode> inputNodes = ConcurrentHashMap.newKeySet();
            ForkJoinPool forkJoinPool = new ForkJoinPool(paths.size());
            try {
                constraintChains.add(new ArrayList<>(forkJoinPool.submit(() -> paths.parallelStream().map(path -> {
                    try {
                        return extractConstraintChain(path, inputNodes);
                    } catch (TouchstoneException | SQLException e) {
                        logger.error(path.toString(), e);
                        return null;
                    }
                }).filter(Objects::nonNull).toList()).get()));
            } catch (InterruptedException | ExecutionException e) {
                logger.error("约束链构造失败", e);
                Thread.currentThread().interrupt();
            } finally {
                forkJoinPool.shutdown();
            }
            allNodes.removeAll(inputNodes);
            if (!allNodes.isEmpty()) {
                for (ExecutionNode node : allNodes) {
                    logger.error("can not input {}", node);
                }
            }
        }
        logger.info("Status:获取完成");
        return constraintChains;
    }

    /**
     * 分析select信息
     *
     * @param operatorInfo 需要分析的operator_info
     * @return 分析查询的逻辑树
     * @throws TouchstoneException 分析失败
     */
    private synchronized LogicNode analyzeSelectInfo(String operatorInfo) throws TouchstoneException {
        try {
            return abstractAnalyzer.analyzeSelectOperator(operatorInfo);
        } catch (Exception e) {
            throw new UnsupportedSelect(operatorInfo, e);
        }
    }

}
