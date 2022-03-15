package ecnu.db.analyzer.statical;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.Token;
import ecnu.db.LanguageManager;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.utils.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ecnu.db.analyzer.online.adapter.pg.PgAnalyzer.TIME_OR_DATE;
import static ecnu.db.utils.CommonUtils.matchPattern;

/**
 * @author alan
 */
public class QueryWriter {
    private static final Logger logger = LoggerFactory.getLogger(QueryWriter.class);
    private static final Pattern PATTERN = Pattern.compile("'([0-9]+)'");
    private static final Pattern DATECompute = Pattern.compile("(?i)'*" + TIME_OR_DATE + "'* ([+\\-]) interval '[0-9]+' (month|year|day)");
    private static final Pattern NumberCompute = Pattern.compile("[0-9]+\\.*[0-9]* (([+\\-]) [0-9]+\\.*[0-9]*)+");
    private final String resultDir;
    private DbType dbType;
    private static final String WORKLOAD_DIR = "/workload";
    private final ResourceBundle rb = LanguageManager.getInstance().getRb();


    public QueryWriter(String resultDir) {
        this.resultDir = resultDir;
        if (new File(resultDir).mkdir()) {
            logger.info("create query dir for output");
        }
    }

    public void setDbType(DbType dbType) {
        this.dbType = dbType;
    }

    /**
     * 模板化SQL语句
     *
     * @param queryCanonicalName query标准名
     * @param query              需要处理的SQL语句
     * @param parameters         需要模板化的参数
     * @return 模板化的SQL语句
     */
    public String templatizeSql(String queryCanonicalName, String query, List<Parameter> parameters) throws SQLException {
        Matcher matcher = DATECompute.matcher(query);
        while (matcher.find()) {
            String dateCompute = matcher.group();
            query = query.replace(dateCompute, evaluate(dateCompute, true));
        }
        matcher = NumberCompute.matcher(query);
        while (matcher.find()) {
            String dateCompute = matcher.group();
            query = query.replace(dateCompute, evaluate(dateCompute, false));
        }
        for (Parameter parameter : parameters) {
            List<String> cols = parameter.hasOnlyOneColumn();
            if (cols.size() > 1) {
                String pattern = cols.stream().map(col -> col.split("\\.")[2]).collect(Collectors.joining(" .{1,2} "));
                matcher = Pattern.compile(pattern).matcher(query);
                if (matcher.find()) {
                    query = query.replace(matcher.group(), matcher.group() + " +'" + parameter.getId() + "' ");
                }
            }
        }
        Lexer lexer = new Lexer(query, null, dbType);
        // paramter data, column name -> start location and end location
        Map<String, List<parameterColumnName2Location>> literalMap = new HashMap<>();
        int lastPos = 0;
        int pos;
        String lastColumn = "";
        while (!lexer.isEOF()) {
            lexer.nextToken();
            // 读进一个关键字
            Token token = lexer.token();
            // 更新到最新的位置
            pos = lexer.pos();
            // 如果可能是列名
            if (token == Token.IDENTIFIER) {
                while (query.length() > pos && query.charAt(pos) == '.') {
                    lexer.nextToken();
                    lexer.nextToken();
                    pos = lexer.pos();
                }
                String col = query.substring(lastPos, pos).trim();
                if (!col.equals("DATE")) {
                    lastColumn = col;
                }
            } else if (token == Token.LITERAL_INT || token == Token.LITERAL_FLOAT || token == Token.LITERAL_CHARS) {
                String str = query.substring(lastPos, pos).trim();
                if (!literalMap.containsKey(str)) {
                    literalMap.put(str, new ArrayList<>());
                }
                parameterColumnName2Location currentLocations = null;
                for (parameterColumnName2Location parameterColumnName2Location : literalMap.get(str)) {
                    if (parameterColumnName2Location.columnName.equals(lastColumn)) {
                        currentLocations = parameterColumnName2Location;
                        break;
                    }
                }
                if (currentLocations != null) {
                    currentLocations.range.add(new AbstractMap.SimpleEntry<>(pos - str.length(), pos));
                } else {
                    currentLocations = new parameterColumnName2Location();
                    currentLocations.columnName = lastColumn;
                    currentLocations.range.add(new AbstractMap.SimpleEntry<>(pos - str.length(), pos));
                    literalMap.get(str).add(currentLocations);
                }
            }
            lastPos = pos;
        }

        // replacement
        List<Parameter> cannotFindArgs = new ArrayList<>(), conflictArgs = new ArrayList<>();
        TreeMap<Integer, Map.Entry<Parameter, Map.Entry<Integer, Integer>>> replaceParams = new TreeMap<>();
        List<Parameter> subPlanParameters = parameters.stream().filter(Parameter::isSubPlan).toList();
        parameters.removeAll(subPlanParameters);
        parameters.addAll(subPlanParameters);
        for (Parameter parameter : parameters) {
            if (parameter.hasOnlyOneColumn().size() > 1) {
                continue;
            }

            String data = parameter.getRealDataValue();
            List<parameterColumnName2Location> matches = literalMap.getOrDefault(data, new ArrayList<>());
            if (literalMap.containsKey("'" + data + "'")) {
                matches.addAll(literalMap.get("'" + data + "'"));
            }

            if (matches.isEmpty()) {
                cannotFindArgs.add(parameter);
            } else if (matches.size() > 1) {
                String col = parameter.getOperand();
                parameterColumnName2Location location = null;
                for (parameterColumnName2Location match : matches) {
                    if (col.contains(match.columnName)) {
                        location = match;
                        break;
                    }
                }
                if (location != null) {
                    var pair = location.range;
                    for (Map.Entry<Integer, Integer> range : pair) {
                        replaceParams.put(range.getKey(), new AbstractMap.SimpleEntry<>(parameter, range));
                    }
                } else {
                    conflictArgs.add(parameter);
                }
            } else {
                if (parameter.isSubPlan()) {
                    var subplanranges = matches.stream().findFirst().get().range;
                    var subplanrange = subplanranges.get(subplanranges.size() - 1);
                    replaceParams.put(subplanrange.getKey(), new AbstractMap.SimpleEntry<>(parameter, subplanrange));
                } else {
                    var pair = matches.stream().findFirst().get().range;
                    for (Map.Entry<Integer, Integer> range : pair) {
                        replaceParams.put(range.getKey(), new AbstractMap.SimpleEntry<>(parameter, range));
                    }
                }
            }
        }
        StringBuilder fragments = new StringBuilder();
        int currentPos = 0;
        while (!replaceParams.isEmpty()) {
            Map.Entry<Integer, Map.Entry<Parameter, Map.Entry<Integer, Integer>>> entry = replaceParams.pollFirstEntry();
            Map.Entry<Parameter, Map.Entry<Integer, Integer>> pair = entry.getValue();
            Parameter parameter = pair.getKey();
            int startPos = pair.getValue().getKey();
            int endPos = pair.getValue().getValue();
            fragments.append(query, currentPos, startPos).append(String.format("'%s'", parameter.getId()));
            currentPos = endPos;
        }
        fragments.append(query.substring(currentPos));
        query = fragments.toString();
        if (!cannotFindArgs.isEmpty()) {
            logger.warn(rb.getString("CannotReplace1"), queryCanonicalName);
            query = appendArgs("cannotFindArgs", cannotFindArgs) + query;
        }
        if (!conflictArgs.isEmpty()) {
            logger.warn(rb.getString("CannotReplace2"), queryCanonicalName);
            query = appendArgs("conflictArgs", conflictArgs) + query;
        }

        return query;
    }


    public String evaluate(String str, boolean isDate) throws SQLException {
        String h2ConnUrl = "jdbc:h2:mem:h2DB;MODE=MYSQL;";
        try (Connection conn = DriverManager.getConnection(h2ConnUrl, "root", "root")) {
            try (Statement statement = conn.createStatement();) {
                String date = isDate ? "DATE " : " ";
                ResultSet resultSet = statement.executeQuery("SELECT " + date + str);
                return resultSet.next() ? "'" + resultSet.getString(1) + "'" : "";
            }
        }
    }


    /**
     * 为模板化后的SQL语句添加conflictArgs和cannotFindArgs参数
     *
     * @param title  标题
     * @param params 需要添加的
     * @return 添加的参数部分
     */
    public String appendArgs(String title, List<Parameter> params) {
        String argsString = params.stream().map(parameter -> String.format("{id:%s,data:%s,operand:%s}",
                        parameter.getId(),
                        "'" + parameter.getDataValue() + "'",
                        parameter.getOperand()))
                .collect(Collectors.joining(","));
        return String.format("-- %s:%s%s", title, argsString, System.lineSeparator());
    }

    public void writeQuery(Map<String, String> queryName2QueryTemplates, Map<Integer, Parameter> id2Parameter) throws IOException {
        for (Map.Entry<String, String> queryName2QueryTemplate : queryName2QueryTemplates.entrySet()) {
            String query = queryName2QueryTemplate.getValue();
            String path = resultDir + WORKLOAD_DIR + "/" + queryName2QueryTemplate.getKey().split("\\.")[0] + "/" + queryName2QueryTemplate.getKey();
            String pathOfTemplate = resultDir + WORKLOAD_DIR + "/" + queryName2QueryTemplate.getKey().split("\\.")[0] + "/" + queryName2QueryTemplate.getKey().split("\\.")[0] + "Template.sql";
            String templateSql = SQLUtils.format(query, dbType, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION) + System.lineSeparator();
            CommonUtils.writeFile(pathOfTemplate, templateSql);
            List<List<String>> matches = matchPattern(PATTERN, query);
            if (matches.isEmpty()) {
                String formatQuery = SQLUtils.format(query, dbType, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION) + System.lineSeparator();
                CommonUtils.writeFile(path, formatQuery);
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
                    String formatQuery = SQLUtils.format(query, dbType, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION) + System.lineSeparator();
                    CommonUtils.writeFile(path, formatQuery);
                }
            }
        }
    }

    private static class parameterColumnName2Location {
        public String columnName;
        public List<Map.Entry<Integer, Integer>> range = new ArrayList<>();
    }
}
