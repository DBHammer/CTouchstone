package ecnu.db.analyzer.online.adapter.pg;

import ecnu.db.analyzer.online.AbstractAnalyzer;
import ecnu.db.analyzer.online.ExecutionNode;
import ecnu.db.analyzer.online.ExecutionNode.ExecutionNodeType;
import ecnu.db.analyzer.online.adapter.pg.parser.PgSelectOperatorInfoLexer;
import ecnu.db.analyzer.online.adapter.pg.parser.PgSelectOperatorInfoParser;
import ecnu.db.generator.constraintchain.filter.LogicNode;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import java_cup.runtime.ComplexSymbolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.matchPattern;


public class PgAnalyzer extends AbstractAnalyzer {

    private static final String NUMERIC = "'[0-9]+'::numeric";

    private static final String DATE1 = "(([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6})";
    private static final String DATE2 = "([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})";
    private static final String DATE3 = "([0-9]{4}-[0-9]{2}-[0-9]{2}))";
    private static final String DATE = String.format("'%s|%s|%s'::date", DATE1, DATE2, DATE3);

    private static final String TIMESTAMP1 = "(([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6})";
    private static final String TIMESTAMP2 = "([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})";
    private static final String TIMESTAMP3 = "([0-9]{4}-[0-9]{2}-[0-9]{2}))";
    private static final String TIMESTAMP = String.format("'%s|%s|%s'::timestamp without time zone", TIMESTAMP1, TIMESTAMP2, TIMESTAMP3);
    private static final Pattern REDUNDANCY = Pattern.compile(NUMERIC + "|" + DATE + "|" + TIMESTAMP);

    private static final Pattern CanonicalColumnName = Pattern.compile("[a-zA-Z][a-zA-Z0-9$_]*\\.[a-zA-Z0-9_]+");
    private static final Pattern JOIN_EQ_OPERATOR = Pattern.compile("Cond: \\(.*\\)");
    private static final String EQ_COLUMN = "([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+)";
    private static final Pattern EQ_OPERATOR = Pattern.compile(String.format("\\( %1$s = %1$s \\)", EQ_COLUMN));


    private final PgSelectOperatorInfoParser parser = new PgSelectOperatorInfoParser(new PgSelectOperatorInfoLexer(new StringReader("")), new ComplexSymbolFactory());
    protected static final Logger logger = LoggerFactory.getLogger(PgAnalyzer.class);

    public PgAnalyzer() {
        super();
        this.nodeTypeRef = new PgNodeTypeInfo();
    }


    @Override
    public ExecutionNode getExecutionTree(List<String[]> queryPlans) {
        String queryPlan = queryPlans.stream().map(queryPlanLine -> queryPlanLine[0]).collect(Collectors.joining());
        PgJsonReader.setReadContext(queryPlan);
        return getExecutionTreeRes(PgJsonReader.skipNodes(new StringBuilder("$.[0]['Plan']")));
    }

    public ExecutionNode getExecutionTreeRes(StringBuilder currentNodePath) {
        ExecutionNode leftNode = null;
        ExecutionNode rightNode = null;
        int plansCount = PgJsonReader.readPlansCount(currentNodePath);
        if (plansCount == 2) {
            StringBuilder rightChildPath = PgJsonReader.skipNodes(PgJsonReader.move2RightChild(currentNodePath));
            rightNode = getExecutionTreeRes(rightChildPath);
        }
        if (plansCount == 1) {
            StringBuilder leftChildPath = PgJsonReader.skipNodes(PgJsonReader.move2LeftChild(currentNodePath));
            rightNode = transferSubPlan2AntiJoin(leftChildPath);
            leftNode = getExecutionTreeRes(leftChildPath);
        }
        ExecutionNode node = getExecutionNode(currentNodePath);
        node.leftNode = leftNode;
        node.rightNode = rightNode;
        return node;
    }


    private ExecutionNode transferSubPlan2AntiJoin(StringBuilder path) {
        if (nodeTypeRef.isFilterNode(PgJsonReader.readNodeType(path))) {
            String filterInfo = PgJsonReader.readFilterInfo(path);
            //todo multiple subPlans
            if ("(NOT (hashed SubPlan 1))".equals(filterInfo)) {
                String tableName = PgJsonReader.readTableName(path.toString());
                aliasDic.put(PgJsonReader.readAlias(path.toString()), tableName);
                int outPutCount = PgJsonReader.readRowCount(path);
                int removedCount = PgJsonReader.readRowsRemoved(path);
                int rowCount = outPutCount + removedCount;
                StringBuilder rightPath = PgJsonReader.move2RightChild(path);
                ExecutionNode currentRightNode = new ExecutionNode(rightPath.toString(), ExecutionNode.ExecutionNodeType.scan, rowCount, "table:" + tableName);
                currentRightNode.setTableName(tableName);
                currentRightNode.isAdd = true;
                return currentRightNode;
            }
        }
        return null;
    }


    private ExecutionNode getFilterNode(StringBuilder path, int rowCount) {
        String planId = path.toString();
        String filterInfo = PgJsonReader.readFilterInfo(path);
        if (filterInfo != null) {
            if (filterInfo.equals("(NOT (hashed SubPlan 1))")) {
                return transferFilter2AntiJoin(path, rowCount);
            } else {
                String tableName = PgJsonReader.readTableName(path.toString());
                aliasDic.put(PgJsonReader.readAlias(path.toString()), tableName);
                ExecutionNode node = new ExecutionNode(planId, ExecutionNodeType.filter, rowCount, transColumnName(filterInfo));
                node.setTableName(tableName);
                return node;
            }
        } else {
            String tableName = PgJsonReader.readTableName(path.toString());
            aliasDic.put(PgJsonReader.readAlias(path.toString()), tableName);
            ExecutionNode node = new ExecutionNode(planId, ExecutionNodeType.scan, rowCount, "table:" + tableName);
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
        return new ExecutionNode(path.toString(), ExecutionNodeType.antiJoin, rowCount, joinInfo);
    }

    private ExecutionNode getJoinNode(StringBuilder path, int rowCount) {
        String joinInfo = switch (PgJsonReader.readNodeType(path)) {
            case "Hash Join" -> PgJsonReader.readHashJoin(path);
            case "Nested Loop" -> PgJsonReader.readIndexJoin(path);
            default -> throw new UnsupportedOperationException();
        };
        return new ExecutionNode(path.toString(), ExecutionNodeType.join, rowCount, joinInfo);
    }

    private ExecutionNode getAggregationNode(StringBuilder path, int rowCount) {
        int rowsAfterFilter = 0;
        List<String> groupKey = PgJsonReader.readGroupKey(path);
        String aggFilterInfo = PgJsonReader.readFilterInfo(path);
        if (aggFilterInfo != null) {
            int inputRows = PgJsonReader.readRowCount(PgJsonReader.move2LeftChild(path));
            rowsAfterFilter = inputRows - PgJsonReader.readRowsRemoved(path);
        } else {
            aggFilterInfo = "";
        }
        aggFilterInfo += PgJsonReader.readOutput(path);
        ExecutionNode node = new ExecutionNode(path.toString(), ExecutionNode.ExecutionNodeType.aggregate,
                rowCount, rowsAfterFilter, transColumnName(aggFilterInfo), groupKey);
        if (groupKey != null) {
            node.setTableName(groupKey.get(0).split("\\.")[0]);
        }
        return node;
    }

    private ExecutionNode getExecutionNode(StringBuilder path) {
        PgNodeTypeInfo nodeTypeRef = new PgNodeTypeInfo();
        String nodeType = PgJsonReader.readNodeType(path);
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
        return removeRedundancy(filter.toString());
    }

    public String removeRedundancy(String filterInfo) {
        Matcher m = REDUNDANCY.matcher(filterInfo);
        StringBuilder filter = new StringBuilder();
        while (m.find()) {
            String date = m.group().split("::")[0];
            m.appendReplacement(filter, date.substring(1, date.length() - 1));
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
                    logger.info("join中包含多个表的约束,暂不支持");
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
    public LogicNode analyzeSelectOperator(String operatorInfo) throws Exception {
        return parser.parseSelectOperatorInfo(operatorInfo);
    }
}
