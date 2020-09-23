package ecnu.db.tidb;

import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;
import ecnu.db.analyzer.online.AbstractAnalyzer;
import ecnu.db.analyzer.online.ExecutionNode;
import ecnu.db.analyzer.online.ExecutionNode.ExecutionNodeType;
import ecnu.db.analyzer.online.RawNode;
import ecnu.db.constraintchain.filter.SelectResult;
import ecnu.db.dbconnector.DatabaseConnectorInterface;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.exception.UnsupportedDBTypeException;
import ecnu.db.exception.UnsupportedJoin;
import ecnu.db.exception.UnsupportedSelect;
import ecnu.db.schema.Schema;
import ecnu.db.tidb.parser.TidbSelectOperatorInfoLexer;
import ecnu.db.tidb.parser.TidbSelectOperatorInfoParser;
import ecnu.db.utils.AbstractDatabaseInfo;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.SystemConfig;
import java_cup.runtime.ComplexSymbolFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.io.StringReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.matchPattern;

/**
 * @author qingshuai.wang
 */
public class TidbAnalyzer extends AbstractAnalyzer {


    private static final Pattern ROW_COUNTS = Pattern.compile("rows:[0-9]+");
    private static final Pattern INNER_JOIN_OUTER_KEY = Pattern.compile("outer key:(.+),");
    private static final Pattern INNER_JOIN_INNER_KEY = Pattern.compile("inner key:(.+)");
    private static final Pattern JOIN_EQ_OPERATOR = Pattern.compile("equal:\\[.*]");
    private static final Pattern PLAN_ID = Pattern.compile("([a-zA-Z]+_[0-9]+)");
    private static final Pattern EQ_OPERATOR = Pattern.compile("eq\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), ([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+)\\)");
    private static final Pattern INNER_JOIN = Pattern.compile("inner join");
    private final TidbSelectOperatorInfoParser parser = new TidbSelectOperatorInfoParser(new TidbSelectOperatorInfoLexer(new StringReader("")), new ComplexSymbolFactory());


    public TidbAnalyzer(SystemConfig config, DatabaseConnectorInterface dbConnector, AbstractDatabaseInfo databaseInfo,
                        Map<String, Schema> schemas, Multimap<String, String> tblName2CanonicalTblName) {
        super(config, dbConnector, databaseInfo, schemas, tblName2CanonicalTblName);
        this.nodeTypeRef = new TidbNodeTypeTool();
        parser.setAnalyzer(this);
    }

    @Override
    public ExecutionNode getExecutionTree(List<String[]> queryPlan) throws TouchstoneToolChainException {
        RawNode rawNodeRoot = buildRawNodeTree(queryPlan);
        return buildExecutionTree(rawNodeRoot);
    }

    /**
     * TODO 支持 semi join, outer join
     * 合并节点，删除query plan中不需要或者不支持的节点，并根据节点类型提取对应信息
     * 关于join下推到tikv节点的处理:
     * 1. 有selection的下推
     * ***********************************************************************
     * *         IndexJoin                                       Filter      *
     * *         /       \                                         /         *
     * *    leftNode    IndexLookup              ===>>>          Join        *
     * *                  /         \                           /   \        *
     * *        IndexRangeScan     Selection              leftNode  Scan     *
     * *                            /                                        *
     * *                          Scan                                       *
     * ***********************************************************************
     * <p>
     * 2. 没有selection的下推(leftNode中有Selection节点)
     * ***********************************************************************
     * *         IndexJoin                                       Join        *
     * *         /       \                                      /    \       *
     * *    leftNode    IndexLookup              ===>>>   leftNode   Scan    *
     * *                /        \                                           *
     * *        IndexRangeScan  Scan                                         *
     * ***********************************************************************
     * <p>
     * 3. 没有selection的下推(leftNode中没有Selection节点，但右边扫描节点上有索引)
     * ***********************************************************************
     * *        IndexJoin                                        Join        *
     * *        /       \                                       /    \       *
     * *    leftNode   IndexReader              ===>>>    leftNode   Scan    *
     * *                /                                                    *
     * *          IndexRangeScan                                             *
     * ***********************************************************************
     *
     * @param rawNode 需要处理的query plan树
     * @return 处理好的树
     * @throws TouchstoneToolChainException 构建查询树失败
     */
    private ExecutionNode buildExecutionTree(RawNode rawNode) throws TouchstoneToolChainException {
        if (rawNode == null) {
            return null;
        }
        String nodeType = rawNode.nodeType;
        if (nodeTypeRef.isPassNode(nodeType)) {
            return rawNode.left == null ? null : buildExecutionTree(rawNode.left);
        }
        ExecutionNode node;
        // 处理底层的TableScan
        if (nodeTypeRef.isTableScanNode(nodeType)) {
            String tableName = extractTableName(rawNode.operatorInfo);
            String canonicalTableName = getCanonicalTblName(tableName);
            return new ExecutionNode(rawNode.id, ExecutionNodeType.scan, rawNode.rowCount, "table:" + canonicalTableName);
        } else if (nodeTypeRef.isFilterNode(nodeType)) {
            node = new ExecutionNode(rawNode.id, ExecutionNodeType.filter, rawNode.rowCount, rawNode.operatorInfo);
            // 跳过底部的TableScan
            if (rawNode.left != null && nodeTypeRef.isTableScanNode(rawNode.left.nodeType)) {
                return node;
            }
            node.leftNode = rawNode.left == null ? null : buildExecutionTree(rawNode.left);
            node.rightNode = rawNode.right == null ? null : buildExecutionTree(rawNode.right);
        } else if (nodeTypeRef.isJoinNode(nodeType)) {
            // 处理IndexJoin有selection的下推到tikv情况
            if (nodeTypeRef.isReaderNode(rawNode.right.nodeType)
                    && rawNode.right.right != null
                    && nodeTypeRef.isIndexScanNode(rawNode.right.left.nodeType)
                    && nodeTypeRef.isFilterNode(rawNode.right.right.nodeType)) {
                node = new ExecutionNode(rawNode.right.right.id, ExecutionNodeType.filter, rawNode.rowCount, rawNode.right.right.operatorInfo);
                node.leftNode = new ExecutionNode(rawNode.right.left.id, ExecutionNodeType.join, rawNode.right.left.rowCount, rawNode.operatorInfo);
                String tableName = extractTableName(rawNode.right.right.left.operatorInfo);
                String canonicalTblName = getCanonicalTblName(tableName);
                node.leftNode.rightNode = new ExecutionNode(rawNode.right.right.left.id, ExecutionNodeType.scan, getSchema(canonicalTblName).getTableSize(), "table:" + canonicalTblName);
                node.leftNode.leftNode = buildExecutionTree(rawNode.left);
                return node;
            }
            node = new ExecutionNode(rawNode.id, ExecutionNodeType.join, rawNode.rowCount, rawNode.operatorInfo);
            node.leftNode = rawNode.left == null ? null : buildExecutionTree(rawNode.left);
            node.rightNode = rawNode.right == null ? null : buildExecutionTree(rawNode.right);
        } else if (nodeTypeRef.isReaderNode(nodeType)) {
            if (rawNode.right != null) {
                List<List<String>> matches = matchPattern(EQ_OPERATOR, rawNode.left.operatorInfo);
                // 处理IndexJoin没有selection的下推到tikv情况
                if (!matches.isEmpty() && nodeTypeRef.isTableScanNode(rawNode.right.nodeType)) {
                    String tableName = extractTableName(rawNode.right.operatorInfo);
                    String canonicalTblName = getCanonicalTblName(tableName);
                    node = new ExecutionNode(rawNode.id, ExecutionNodeType.scan, getSchema(canonicalTblName).getTableSize(), "table:" + canonicalTblName);
                    // 其他情况跳过左侧节点
                } else {
                    node = buildExecutionTree(rawNode.right);
                }
            }
            // 处理IndexReader后接一个IndexScan的情况
            else if (nodeTypeRef.isIndexScanNode(rawNode.left.nodeType)) {
                String tableName = extractTableName(rawNode.left.operatorInfo);
                String canonicalTblName = getCanonicalTblName(tableName);
                int tableSize = getSchema(canonicalTblName).getTableSize();
                // 处理IndexJoin没有selection的下推到tikv情况
                if (rawNode.left.rowCount != tableSize) {
                    node = new ExecutionNode(rawNode.left.id, ExecutionNodeType.scan, tableSize, "table:" + canonicalTblName);
                    // 正常情况
                } else {
                    node = new ExecutionNode(rawNode.left.id, ExecutionNodeType.scan, rawNode.left.rowCount, "table:" + canonicalTblName);
                }
            } else {
                node = buildExecutionTree(rawNode.left);
            }
        } else {
            throw new TouchstoneToolChainException("未支持的查询树Node，类型为" + nodeType);
        }
        return node;
    }

    private String getCanonicalTblName(String tableName) throws TouchstoneToolChainException {
        if (CommonUtils.isCanonicalTableName(tableName)) {
            return tableName;
        }
        List<String> canonicalTblNames = new ArrayList<>(tblName2CanonicalTblName.get(tableName));
        if (canonicalTblNames.size() > 1) {
            throw new TouchstoneToolChainException(String.format("'%s'的表名有冲突，请使用别名",
                    canonicalTblNames
                            .stream()
                            .map((name) -> String.format("%s", name))
                            .collect(Collectors.joining(","))));
        }
        return canonicalTblNames.get(0);
    }

    /**
     * 根据explain analyze的结果生成query plan树
     *
     * @param queryPlan explain analyze的结果
     * @return 生成好的树
     */
    private RawNode buildRawNodeTree(List<String[]> queryPlan) throws TouchstoneToolChainException {
        Deque<Pair<Integer, RawNode>> pStack = new ArrayDeque<>();
        List<List<String>> matches = matchPattern(PLAN_ID, queryPlan.get(0)[0]);
        String nodeType = matches.get(0).get(0).split("_")[0];
        String[] subQueryPlanInfo = extractSubQueryPlanInfo(queryPlan.get(0));
        String planId = matches.get(0).get(0), operatorInfo = subQueryPlanInfo[1], executionInfo = subQueryPlanInfo[2];
        Matcher matcher;
        int rowCount = (matcher = ROW_COUNTS.matcher(executionInfo)).find() ?
                Integer.parseInt(matcher.group(0).split(":")[1]) : 0;
        RawNode rawNodeRoot = new RawNode(planId, null, null, nodeType, operatorInfo, rowCount), rawNode;
        pStack.push(Pair.of(0, rawNodeRoot));
        for (String[] subQueryPlan : queryPlan.subList(1, queryPlan.size())) {
            subQueryPlanInfo = extractSubQueryPlanInfo(subQueryPlan);
            matches = matchPattern(PLAN_ID, subQueryPlanInfo[0]);
            planId = matches.get(0).get(0);
            operatorInfo = subQueryPlanInfo[1];
            executionInfo = subQueryPlanInfo[2];
            nodeType = matches.get(0).get(0).split("_")[0];
            try {
                rowCount = (matcher = ROW_COUNTS.matcher(executionInfo)).find() ?
                        Integer.parseInt(matcher.group(0).split(":")[1]) : 0;
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("hello");
            }
            rawNode = new RawNode(planId, null, null, nodeType, operatorInfo, rowCount);
            int level = (subQueryPlan[0].split("─")[0].length() + 1) / 2;
            while (!pStack.isEmpty() && pStack.peek().getKey() > level) {
                pStack.pop(); // pop直到找到同一个层级的节点
            }
            if (pStack.isEmpty()) {
                throw new TouchstoneToolChainException("pStack不应为空");
            }
            if (pStack.peek().getKey().equals(level)) {
                pStack.pop();
                if (pStack.isEmpty()) {
                    throw new TouchstoneToolChainException("pStack不应为空");
                }
                pStack.peek().getValue().right = rawNode;
            } else {
                pStack.peek().getValue().left = rawNode;
            }
            pStack.push(Pair.of(level, rawNode));
        }
        return rawNodeRoot;
    }

    /**
     * 获取节点上查询计划的信息
     *
     * @param data 需要处理的数据
     * @return 返回plan_id, operator_info, execution_info
     * @throws TouchstoneToolChainException 不支持的版本
     */
    private String[] extractSubQueryPlanInfo(String[] data) throws TouchstoneToolChainException {
        switch (analyzerSupportedDatabaseVersion) {
            case TiDB3:
                return data;
            case TiDB4:
                String[] ret = new String[3];
                ret[0] = data[0];
                ret[1] = data[3].isEmpty() ? data[1] : String.format("%s,%s", data[3], data[1]);
                ret[2] = "rows:" + data[2];
                return ret;
            default:
                throw new UnsupportedDBTypeException(analyzerSupportedDatabaseVersion);
        }
    }

    /**
     * 分析join信息
     * TODO support other valid schema object names listed in https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
     *
     * @param joinInfo join字符串
     * @return 长度为4的字符串数组，0，1为join info左侧的表名和列名，2，3为join右侧的表明和列名
     * @throws TouchstoneToolChainException 无法分析的join条件
     */
    @Override
    public String[] analyzeJoinInfo(String joinInfo) throws TouchstoneToolChainException {
        if (joinInfo.contains("other cond:")) {
            throw new TouchstoneToolChainException("join中包含其他条件,暂不支持");
        }
        if (matchPattern(INNER_JOIN, joinInfo).isEmpty()) {
            throw new UnsupportedJoin(joinInfo);
        }
        String[] result = new String[4];
        String leftTable, leftCol, rightTable, rightCol;
        Matcher eqCondition = JOIN_EQ_OPERATOR.matcher(joinInfo);
        if (eqCondition.find()) {
            if (eqCondition.groupCount() > 1) {
                throw new UnsupportedOperationException();
            }
            List<List<String>> matches = matchPattern(EQ_OPERATOR, joinInfo);
            String[] leftJoinInfos = matches.get(0).get(1).split("\\."), rightJoinInfos = matches.get(0).get(2).split("\\.");
            leftTable = String.format("%s.%s", leftJoinInfos[0], leftJoinInfos[1]);
            rightTable = String.format("%s.%s", rightJoinInfos[0], rightJoinInfos[1]);
            List<String> leftCols = new ArrayList<>(), rightCols = new ArrayList<>();
            for (List<String> match : matches) {
                leftJoinInfos = match.get(1).split("\\.");
                rightJoinInfos = match.get(2).split("\\.");
                String currLeftTable = String.format("%s.%s", leftJoinInfos[0], leftJoinInfos[1]),
                        currLeftCol = leftJoinInfos[2],
                        currRightTable = String.format("%s.%s", rightJoinInfos[0], rightJoinInfos[1]),
                        currRightCol = rightJoinInfos[2];
                if (!leftTable.equals(currLeftTable) || !rightTable.equals(currRightTable)) {
                    throw new TouchstoneToolChainException("join中包含多个表的约束,暂不支持");
                }
                leftCols.add(currLeftCol);
                rightCols.add(currRightCol);
            }
            leftCol = String.join(",", leftCols);
            rightCol = String.join(",", rightCols);
            result[0] = leftTable;
            result[1] = leftCol;
            result[2] = rightTable;
            result[3] = rightCol;
        } else {
            Matcher innerInfo = INNER_JOIN_INNER_KEY.matcher(joinInfo);
            if (innerInfo.find()) {
                String[] innerInfos = innerInfo.group(1).split("\\.");
                result[0] = String.join(".", Arrays.asList(innerInfos[0], innerInfos[1]));
                result[1] = innerInfos[2];
            } else {
                throw new TouchstoneToolChainException("无法匹配的join格式" + joinInfo);
            }
            Matcher outerInfo = INNER_JOIN_OUTER_KEY.matcher(joinInfo);
            if (outerInfo.find()) {
                String[] outerInfos = outerInfo.group(1).split("\\.");
                result[2] = String.join(".", Arrays.asList(outerInfos[0], outerInfos[1]));
                result[3] = outerInfos[2].substring(0, outerInfos[2].length() - 1);
            } else {
                throw new TouchstoneToolChainException("无法匹配的join格式" + joinInfo);
            }
        }
        if (result[1].contains(")")) {
            result[1] = result[1].substring(0, result[1].indexOf(')'));
        }
        if (result[3].contains(")")) {
            result[3] = result[3].substring(0, result[3].indexOf(')'));
        }
        return convertToDbTableName(result);
    }

    private String[] convertToDbTableName(String[] result) {
        if (aliasDic.containsKey(result[0])) {
            result[0] = aliasDic.get(result[0]);
        }
        if (aliasDic.containsKey(result[2])) {
            result[2] = aliasDic.get(result[2]);
        }
        return result;
    }

    @Override
    protected String extractTableName(String operatorInfo) {
        String tableName = operatorInfo.split(",")[0].substring(6).toLowerCase();
        if (aliasDic.containsKey(tableName)) {
            tableName = aliasDic.get(tableName);
        }
        return tableName;
    }

    @Override
    public SelectResult analyzeSelectInfo(String operatorInfo) throws UnsupportedSelect {
        SelectResult result;
        try {
            result = parser.parseSelectOperatorInfo(operatorInfo);
            return result;
        } catch (Exception e) {
            String stackTrace = Throwables.getStackTraceAsString(e);
            throw new UnsupportedSelect(operatorInfo, stackTrace);
        }
    }
}
