package ecnu.db.analyzer.online.adapter.pg;

import ecnu.db.analyzer.online.AbstractAnalyzer;
import ecnu.db.analyzer.online.adapter.pg.parser.PgSelectOperatorInfoLexer;
import ecnu.db.analyzer.online.adapter.pg.parser.PgSelectOperatorInfoParser;
import ecnu.db.analyzer.online.node.*;
import ecnu.db.generator.constraintchain.filter.LogicNode;
import ecnu.db.schema.TableManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import ecnu.db.utils.exception.schema.CannotFindSchemaException;
import java_cup.runtime.ComplexSymbolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.matchPattern;

public class PgAnalyzer extends AbstractAnalyzer {

    private static final String NUMERIC = "'[0-9]+'::numeric";

    private static final String DATE1 = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}";
    private static final String DATE2 = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}";
    private static final String DATE3 = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
    private static final String DATE = String.format("'(%s|%s|%s)'::date", DATE1, DATE2, DATE3);

    private static final String TIMESTAMP1 = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}";
    private static final String TIMESTAMP2 = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}";
    private static final String TIMESTAMP3 = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
    public static final String  TIME_OR_DATE=String.format("(%s|%s|%s|%s|%s|%s)",DATE1,DATE2,DATE3,TIMESTAMP1,TIMESTAMP2,TIMESTAMP3);
    private static final String TIMESTAMP = String.format("'(%s|%s|%s)'::timestamp without time zone", TIMESTAMP1, TIMESTAMP2, TIMESTAMP3);
    private static final Pattern REDUNDANCY = Pattern.compile(NUMERIC + "|" + DATE + "|" + TIMESTAMP);

    private static final Pattern CanonicalColumnName = Pattern.compile("[a-zA-Z][a-zA-Z0-9$_]*\\.[a-zA-Z0-9_]+");
    private static final Pattern JOIN_EQ_OPERATOR = Pattern.compile("Cond: \\(.*\\)");
    private static final Pattern EQ_OPERATOR = Pattern.compile("\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+) = ([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+)\\)");


    private final PgSelectOperatorInfoParser parser = new PgSelectOperatorInfoParser(new PgSelectOperatorInfoLexer(new StringReader("")), new ComplexSymbolFactory());
    protected static final Logger logger = LoggerFactory.getLogger(PgAnalyzer.class);

    public StringBuilder pathForSplit = null;

    public PgAnalyzer() {
        super();
        this.nodeTypeRef = new PgNodeTypeInfo();
    }

    @Override
    public ExecutionNode getExecutionTree(List<String[]> queryPlans) throws TouchstoneException, IOException, SQLException {
        String queryPlan = queryPlans.stream().map(queryPlanLine -> queryPlanLine[0]).collect(Collectors.joining());
        PgJsonReader.setReadContext(queryPlan);
        return getExecutionTreeRes(PgJsonReader.skipNodes(new StringBuilder("$.[0]['Plan']")));
    }

    public ExecutionNode getExecutionTreeRes(StringBuilder currentNodePath) throws TouchstoneException, IOException, SQLException {
        ExecutionNode leftNode = null;
        ExecutionNode rightNode = null;
        int plansCount = PgJsonReader.readPlansCount(currentNodePath);
        if (plansCount >= 2) {
            StringBuilder leftChildPath = PgJsonReader.skipNodes(PgJsonReader.move2LeftChild(currentNodePath));
            leftNode = getExecutionTreeRes(leftChildPath);
            StringBuilder rightChildPath = PgJsonReader.skipNodes(PgJsonReader.move2RightChild(currentNodePath));
            rightNode = getExecutionTreeRes(rightChildPath);
        } else if (plansCount == 1) {
            //todo fix only for query 20
            if (canNotDeal(currentNodePath)) {
                pathForSplit = currentNodePath;
                String tableName = PgJsonReader.readTableName(currentNodePath.toString());
                int tableSize = TableManager.getInstance().getTableSize(tableName);
                aliasDic.put(PgJsonReader.readAlias(currentNodePath.toString()), tableName);
                ExecutionNode subNode = new FilterNode(currentNodePath.toString(), tableSize, null);
                subNode.setTableName(tableName);
                return subNode;
            }
            StringBuilder leftChildPath = PgJsonReader.skipNodes(PgJsonReader.move2LeftChild(currentNodePath));
            leftNode = getExecutionTreeRes(leftChildPath);
            rightNode = transferSubPlan2AntiJoin(currentNodePath);
        }
        ExecutionNode node = getExecutionNode(currentNodePath);
        if (node == null) {
            return null;
        }
        // todo 不一定是主键
        if (node.getType() == ExecutionNodeType.aggregate) {
            assert leftNode != null;
            if (leftNode.getType() == ExecutionNodeType.join &&
                    ((JoinNode) leftNode).getPkDistinctSize() > 0) {
                logger.debug("跳过外连接后的主键聚集");
                node.setInfo(null);
            }
        }
        if (node.getType() == ExecutionNodeType.join) {
            assert rightNode != null;
            if (rightNode.getType() == ExecutionNodeType.filter && rightNode.getInfo() != null && ((FilterNode) rightNode).isIndexScan()) {
                long rowsRemoveByFilterAfterJoin = PgJsonReader.readRowsRemoved(PgJsonReader.skipNodes(PgJsonReader.move2RightChild(currentNodePath)));
                ((JoinNode) node).setRowsRemoveByFilterAfterJoin(rowsRemoveByFilterAfterJoin);
                String indexJoinFilter = PgJsonReader.readFilterInfo(PgJsonReader.skipNodes(PgJsonReader.move2RightChild(currentNodePath)));
                ((JoinNode) node).setIndexJoinFilter(removeRedundancy(indexJoinFilter, true));
            }
        }
        node.setLeftNode(leftNode);
        node.setRightNode(rightNode);
        //create agg node
        if (plansCount == 3) {
            StringBuilder thirdChildPath = PgJsonReader.skipNodes(PgJsonReader.move3ThirdChild(currentNodePath));
            ExecutionNode parentAggNode = createParentAggNode(currentNodePath, thirdChildPath);
            int rowCount = PgJsonReader.readRowCount(currentNodePath) + PgJsonReader.readRowsRemovedByJoinFilter(currentNodePath);
            node.setOutputRows(rowCount);
            parentAggNode.setLeftNode(node);
            node = parentAggNode;
        }
        //todo fix only for query 20
        if (pathForSplit != null) {
            if (PgJsonReader.move2LeftChild(currentNodePath).toString().equals(pathForSplit.toString()) ||
                    PgJsonReader.move2RightChild(currentNodePath).toString().equals(pathForSplit.toString())) {
                node.setOutputRows(PgJsonReader.readActualLoops(PgJsonReader.move2LeftChild(pathForSplit)));
            }
        }
        return node;
    }


    private ExecutionNode transferSubPlan2AntiJoin(StringBuilder path) {
        //todo multiple subPlans
        if (nodeTypeRef.isFilterNode(PgJsonReader.readNodeType(path)) &&
                "(NOT (hashed SubPlan 1))".equals(PgJsonReader.readFilterInfo(path))) {
            String tableName = PgJsonReader.readTableName(path.toString());
            aliasDic.put(PgJsonReader.readAlias(path.toString()), tableName);
            int outPutCount = PgJsonReader.readRowCount(path);
            int removedCount = PgJsonReader.readRowsRemoved(path);
            int rowCount = outPutCount + removedCount;
            StringBuilder rightPath = PgJsonReader.move2RightChild(path);
            FilterNode currentRightNode = new FilterNode(rightPath.toString(), rowCount, null);
            currentRightNode.setTableName(tableName);
            currentRightNode.setAdd();
            return currentRightNode;
        }
        return null;
    }


    private ExecutionNode getFilterNode(StringBuilder path, int rowCount) throws CannotFindSchemaException {
        if (PgJsonReader.readJoinFilter(path) != null) {
            rowCount += PgJsonReader.readRowsRemovedByJoinFilter(path);
        }
        String planId = path.toString();
        String filterInfo = PgJsonReader.readFilterInfo(path);
        if (filterInfo != null) {
            if (filterInfo.equals("(NOT (hashed SubPlan 1))")) {
                return transferFilter2AntiJoin(path, rowCount);
            } else {
                String tableName = PgJsonReader.readTableName(path.toString());
                aliasDic.put(PgJsonReader.readAlias(path.toString()), tableName);
                FilterNode node = new FilterNode(planId, rowCount, transColumnName(filterInfo));
                node.setTableName(tableName);
                if (nodeTypeRef.isIndexScanNode(PgJsonReader.readNodeType(path))) {
                    node.setIndexScan(true);
                    node.setFilterInfoWithQuote(transColumnName(removeRedundancy(filterInfo, true)));
                }
                return node;
            }
        } else {
            String tableName = PgJsonReader.readTableName(path.toString());
            if (nodeTypeRef.isIndexScanNode(PgJsonReader.readNodeType(path))) {
                rowCount = TableManager.getInstance().getTableSize(tableName);
            }
            aliasDic.put(PgJsonReader.readAlias(path.toString()), tableName);
            ExecutionNode node = new FilterNode(planId, rowCount, null);
            node.setTableName(tableName);
            return node;
        }
    }

    private ExecutionNode transferFilter2AntiJoin(StringBuilder path, int rowCount) {
        StringBuilder leftNodePath = PgJsonReader.move2LeftChild(path);
        List<String> leftNodeResult = PgJsonReader.readOutput(leftNodePath);
        List<String> outPut = PgJsonReader.readOutput(path);
        String joinInfo = "";
        for (String s : leftNodeResult) {
            String antiJoinTable1 = s.split("\\.")[0];
            String antiJoinKey1 = s.split("\\.")[1];
            String joinColumn1 = antiJoinKey1.split("_")[1];
            for (String value : outPut) {
                String antiJoinKey2 = value.split("\\.")[1];
                String antiJoinTable2 = value.split("\\.")[0];
                String joinColumn2 = antiJoinKey2.split("_")[1];
                if (joinColumn1.equals(joinColumn2)) {
                    joinInfo = antiJoinTable2 + "." + antiJoinKey2 + " = " + antiJoinTable1 + "." + antiJoinKey1;
                }
            }
        }
        joinInfo = "Hash Cond: " + "(" + joinInfo + ")";
        return new JoinNode(path.toString(), rowCount, joinInfo, true, false, 0);
    }

    private ExecutionNode getJoinNode(StringBuilder path, int rowCount) {
        String joinInfo = switch (PgJsonReader.readNodeType(path)) {
            case "Hash Join" -> PgJsonReader.readHashJoin(path);
            case "Nested Loop" -> PgJsonReader.readIndexJoin(path);
            case "Merge Join" -> PgJsonReader.readMergeJoin(path);
            default -> throw new UnsupportedOperationException();
        };
        double pkDistinctProbability = 0;
        if (PgJsonReader.isOutJoin(path)) {
            StringBuilder leftChildPath = PgJsonReader.skipNodes(PgJsonReader.move2LeftChild(path));
            StringBuilder rightChildPath = PgJsonReader.skipNodes(PgJsonReader.move2RightChild(path));
            int joinRowCount = PgJsonReader.readRowCount(path);
            int pkRowCount = PgJsonReader.readRowCount(rightChildPath);
            int fkRowCount = PgJsonReader.readRowCount(leftChildPath);
            rowCount = fkRowCount;
            pkDistinctProbability = ((double) joinRowCount - (double) fkRowCount) / (double) pkRowCount;
        }
        boolean isSemiJoin = PgJsonReader.isSemiJoin(path);
        JoinNode joinNode = new JoinNode(path.toString(), rowCount, joinInfo, false, isSemiJoin, pkDistinctProbability);
        if(isSemiJoin) {
            joinNode.setPkDistinctSize(joinNode.getOutputRows());
        }
        return joinNode;
    }

    private ExecutionNode getAggregationNode(StringBuilder path, int rowCount) {
        FilterNode aggFilter = null;
        String aggFilterInfo = PgJsonReader.readFilterInfo(path);
        if (aggFilterInfo == null) {
            String subPlanIndex = PgJsonReader.readSubPlanIndex(path);
            if (subPlanIndex != null) {
                aggFilterInfo = "(" + removeRedundancy(PgJsonReader.readOutput(path).get(0), false) + "=" + subPlanIndex + ")";
                aggFilter = new FilterNode(path.toString(), 1, transColumnName(aggFilterInfo));
            }
        } else {
            aggFilter = new FilterNode(path.toString(), rowCount, transColumnName(aggFilterInfo));
            rowCount += PgJsonReader.readRowsRemoved(path);
        }

        List<String> groupKey = PgJsonReader.readGroupKey(path);
        String groupKeyInfo = null;
        String tableName = null;
        if (groupKey != null) {
            //todo multiple table name
            groupKeyInfo = groupKey.stream().map(this::transColumnName).collect(Collectors.joining(";"));
            tableName = aliasDic.get(groupKey.get(0).split("\\.")[0]);
        } else if (aggFilter == null) {
            logger.debug("跳过无Group Key和过滤条件的聚集算子");
        } else {
            tableName = getTableNameFromOutput(path);
        }
        AggNode node = new AggNode(path.toString(), rowCount, groupKeyInfo);
        node.setTableName(tableName);
        node.setAggFilter(aggFilter);
        return node;
    }

    private ExecutionNode createParentAggNode(StringBuilder parentPath, StringBuilder aggPath) throws TouchstoneException, IOException, SQLException {
        int rowCount;
        int rowsAfterFilter;
        String joinCond = PgJsonReader.readJoinCond(parentPath);
        String leftJoinCond = joinCond.split("=")[0];
        List<String> groupKey = new ArrayList<>();
        groupKey.add(leftJoinCond.substring(1));
        String[] outPut = PgJsonReader.readOutput(aggPath).get(0).split("\\.");
        String tableName = outPut[outPut.length - 2];
        tableName = tableName.replaceAll(".*\\(", "");
        tableName = tableName.split("_")[0];
        tableName = aliasDic.get(tableName);
        getExecutionTreeRes(aggPath);
        String aggFilterInfo = PgJsonReader.readJoinFilter(parentPath);
        if (aggFilterInfo != null) {
            String aggOutPut = PgJsonReader.readOutput(aggPath).get(0);
            aggFilterInfo = aggFilterInfo.replace("(SubPlan 1)", aggOutPut);
            rowsAfterFilter = PgJsonReader.readRowCount(parentPath);
            rowCount = rowsAfterFilter + PgJsonReader.readRowsRemovedByJoinFilter(parentPath);
        } else {
            throw new UnsupportedOperationException();
        }
        groupKey = groupKey.stream().map(this::transColumnName).toList();
        AggNode node = new AggNode(aggPath.toString(), rowCount, String.join(";", groupKey));
        node.setAggFilter(new FilterNode(aggPath.toString(), rowsAfterFilter, transColumnName(aggFilterInfo)));
        node.setTableName(tableName);
        return node;
    }

    private ExecutionNode getExecutionNode(StringBuilder path) throws TouchstoneException {
        String nodeType = PgJsonReader.readNodeType(path);
        if (nodeType == null) {
            return null;
        }
        int rowCount = PgJsonReader.readRowCount(path);
        if (nodeTypeRef.isFilterNode(nodeType)) {
            return getFilterNode(path, rowCount);
        } else if (nodeTypeRef.isJoinNode(nodeType)) {
            return getJoinNode(path, rowCount);
        } else if (nodeTypeRef.isAggregateNode(nodeType)) {
            return getAggregationNode(path, rowCount);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public String transColumnName(String filterInfo) {
        Matcher m = CanonicalColumnName.matcher(filterInfo);
        StringBuilder filter = new StringBuilder();
        while (m.find()) {
            String[] tableNameAndColName = m.group().split("\\.");
            m.appendReplacement(filter, aliasDic.get(tableNameAndColName[0]) + "." + tableNameAndColName[1]);
        }
        m.appendTail(filter);
        return removeRedundancy(filter.toString(), false);
    }

    public String removeRedundancy(String filterInfo, boolean keepQuotes) {
        int filterLocation = keepQuotes ? 0 : 1;
        Matcher m = REDUNDANCY.matcher(filterInfo);
        StringBuilder filter = new StringBuilder();
        while (m.find()) {
            String date = m.group().split("::")[0];
            m.appendReplacement(filter, date.substring(filterLocation, date.length() - filterLocation));
        }
        m.appendTail(filter);
        return filter.toString();
    }

    @Override
    public String[] analyzeJoinInfo(String joinInfo) throws TouchstoneException {
        if (joinInfo.contains("other cond:")) {
            throw new TouchstoneException("join中包含其他条件,暂不支持");
        }
        joinInfo = transColumnName(joinInfo);
        String[] result = new String[4];
        Matcher eqCondition = JOIN_EQ_OPERATOR.matcher(joinInfo);
        if (eqCondition.find()) {
            if (eqCondition.groupCount() > 1) {
                throw new UnsupportedOperationException();
            }
            List<List<String>> matches = matchPattern(EQ_OPERATOR, joinInfo);
            String[] leftJoinInfos = matches.get(0).get(1).split("\\.");
            String[] rightJoinInfos = matches.get(0).get(2).split("\\.");
            String leftTable = leftJoinInfos[0] + CommonUtils.CANONICAL_NAME_CONTACT_SYMBOL + leftJoinInfos[1];
            String rightTable = rightJoinInfos[0] + CommonUtils.CANONICAL_NAME_CONTACT_SYMBOL + rightJoinInfos[1];
            List<String> leftCols = new ArrayList<>();
            List<String> rightCols = new ArrayList<>();
            for (List<String> match : matches) {
                leftJoinInfos = match.get(1).split("\\.");
                rightJoinInfos = match.get(2).split("\\.");
                String currLeftTable = String.format("%s.%s", leftJoinInfos[0], leftJoinInfos[1]);
                String currLeftCol = leftJoinInfos[2];
                String currRightTable = String.format("%s.%s", rightJoinInfos[0], rightJoinInfos[1]);
                String currRightCol = rightJoinInfos[2];
                if (!leftTable.equals(currLeftTable) || !rightTable.equals(currRightTable)) {
                    logger.error("join中包含多个表的约束,暂不支持");
                    break;
                }
                leftCols.add(currLeftCol);
                rightCols.add(currRightCol);
            }
            String leftCol = String.join(",", leftCols);
            String rightCol = String.join(",", rightCols);
            result[0] = leftTable;
            result[1] = leftCol;
            result[2] = rightTable;
            result[3] = rightCol;
        }
        return result;
    }

    @Override
    public List<List<String[]>> splitQueryPlan(List<String[]> queryPlan) {
        String queryPlanString = queryPlan.stream().map(queryPlanLine -> queryPlanLine[0]).collect(Collectors.joining());
        PgJsonReader.setReadContext(queryPlanString);
        StringBuilder path = new StringBuilder("$.[0]['Plan']");
        if (PgJsonReader.hasInitPlan(path)) {
            List<List<String[]>> queryPlans = new LinkedList<>();
            for (int i = 0; i < PgJsonReader.readPlansCount(path); i++) {
                String[] subQueryPlan = new String[]{"[{Plan:" + PgJsonReader.readPlan(path, i) + "}]"};
                queryPlans.add(Collections.singletonList(subQueryPlan));
            }
            return queryPlans;
        } else {
            return Collections.singletonList(queryPlan);
        }
    }

    @Override
    public List<Map.Entry<String, String>> splitQueryPlanForMultipleAggregate() {
        if (pathForSplit == null) {
            return null;
        } else {
            List<Map.Entry<String, String>> tableNameAndFilterInfo = new LinkedList<>();
            StringBuilder path = PgJsonReader.move2LeftChild(PgJsonReader.move2LeftChild(pathForSplit));
            String tableName = PgJsonReader.readTableName(path.toString()).split("\\.")[1];
            String filterInfo = removeRedundancy(PgJsonReader.readFilterInfo(path), true);
            tableNameAndFilterInfo.add(new AbstractMap.SimpleEntry<>(tableName, filterInfo));
            pathForSplit = null;
            return tableNameAndFilterInfo;
        }
    }

    public boolean canNotDeal(StringBuilder path) throws SQLException, TouchstoneException, IOException {
        String nodeType = PgJsonReader.readNodeType(path);
        StringBuilder leftPath = PgJsonReader.move2LeftChild(path);
        String leftNodeType = PgJsonReader.readNodeType(leftPath);
        String tableName = PgJsonReader.readTableName(path.toString()).split("\\.")[1];
        if (nodeTypeRef.isAggregateNode(leftNodeType) && nodeTypeRef.isIndexScanNode(nodeType)) {
            logger.error("cannot deal with " + path);
            getExecutionTreeRes(PgJsonReader.move2LeftChild(leftPath));
            return !tableName.equals(getTableNameFromOutput(leftPath));
        } else {
            return false;
        }
    }

    private String getTableNameFromOutput(StringBuilder path) {
        String outPut = PgJsonReader.readOutput(path).get(0);
        Set<String> tableNames = aliasDic.keySet().stream().filter(outPut::contains)
                .map(alias -> aliasDic.get(alias)).collect(Collectors.toSet());
        if (tableNames.size() > 1) {
            logger.error("不能识别多表");
            return null;
        } else {
            return tableNames.iterator().next();
        }
    }

    @Override
    public LogicNode analyzeSelectOperator(String operatorInfo) throws Exception {
        return parser.parseSelectOperatorInfo(operatorInfo);
    }
}
