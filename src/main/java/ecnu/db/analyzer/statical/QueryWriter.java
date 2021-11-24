package ecnu.db.analyzer.statical;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.Token;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.utils.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.matchPattern;

/**
 * @author alan
 */
public class QueryWriter {
    private static final Logger logger = LoggerFactory.getLogger(QueryWriter.class);
    private static final Pattern PATTERN = Pattern.compile("'([0-9]+)'");
    public static final String QUERY_DIR = "/queries/";
    private final String queryDir;
    private DbType dbType;

    public QueryWriter(String resultDir) {
        this.queryDir = resultDir + QUERY_DIR;
        if(new File(queryDir).mkdir()){
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
    public String templatizeSql(String queryCanonicalName, String query, List<Parameter> parameters) {
        Lexer lexer = new Lexer(query, null, dbType);
        Map<String, Collection<Map.Entry<Integer, Integer>>> literalMap = new HashMap<>();
        int lastPos = 0, pos;
        while (!lexer.isEOF()) {
            lexer.nextToken();
            Token token = lexer.token();
            pos = lexer.pos();
            if (token == Token.LITERAL_INT || token == Token.LITERAL_FLOAT || token == Token.LITERAL_CHARS) {
                String str = query.substring(lastPos, pos).trim();
                if (!literalMap.containsKey(str)) {
                    literalMap.put(str, new ArrayList<>());
                }
                literalMap.get(str).add(new AbstractMap.SimpleEntry<>(pos - str.length(), pos));
            }
            lastPos = pos;
        }

        // replacement
        List<Parameter> cannotFindArgs = new ArrayList<>(), conflictArgs = new ArrayList<>();
        TreeMap<Integer, Map.Entry<Parameter, Map.Entry<Integer, Integer>>> replaceParams = new TreeMap<>();
        for (Parameter parameter : parameters) {
            String data = parameter.getDataValue();
            Collection<Map.Entry<Integer, Integer>> matches = literalMap.getOrDefault(data, new ArrayList<>());
            if (literalMap.containsKey("'" + data + "'")) {
                matches.addAll(literalMap.get("'" + data + "'"));
            }

            if (matches.isEmpty()) {
                cannotFindArgs.add(parameter);
            } else if (matches.size() > 1) {
                conflictArgs.add(parameter);
            } else {
                var pair = matches.stream().findFirst().get();
                int startPos = pair.getKey();
                replaceParams.put(startPos, new AbstractMap.SimpleEntry<>(parameter, pair));
            }
        }
        StringBuilder fragments = new StringBuilder();
        int currentPos = 0;
        while (!replaceParams.isEmpty()) {
            Map.Entry<Integer, Map.Entry<Parameter, Map.Entry<Integer, Integer>>> entry = replaceParams.pollFirstEntry();
            Map.Entry<Parameter, Map.Entry<Integer, Integer>> pair = entry.getValue();
            Parameter parameter = pair.getKey();
            int startPos = pair.getValue().getKey(), endPos = pair.getValue().getValue();
            fragments.append(query, currentPos, startPos).append(String.format("'%s'", parameter.getId()));
            currentPos = endPos;
        }
        fragments.append(query.substring(currentPos));
        query = fragments.toString();
        if (!cannotFindArgs.isEmpty()) {
            logger.warn("请注意{}中有参数无法完成替换，请查看该sql输出，手动替换;", queryCanonicalName);
            query = appendArgs("cannotFindArgs", cannotFindArgs) + query;
        }
        if (!conflictArgs.isEmpty()) {
            logger.warn("请注意{}中有参数出现多次，无法智能替换，请查看该sql输出，手动替换;", queryCanonicalName);
            query = appendArgs("conflictArgs", conflictArgs) + query;
        }

        return query;
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
            List<List<String>> matches = matchPattern(PATTERN, query);
            if(matches.isEmpty()){
                String formatQuery = SQLUtils.format(query, dbType, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION) + System.lineSeparator();
                CommonUtils.writeFile(queryDir + queryName2QueryTemplate.getKey(), formatQuery);
            }else {
                for (List<String> group : matches) {
                    int parameterId = Integer.parseInt(group.get(1));
                    Parameter parameter = id2Parameter.remove(parameterId);
                    if (parameter != null) {
                        String parameterData = parameter.getDataValue();
                        try {
                            query = query.replaceAll(group.get(0), String.format("'%s'", parameterData));
                        } catch (IllegalArgumentException e) {
                            logger.error("query is " + query + "; group is " + group + "; parameter data is " + parameterData, e);
                        }
                    }
                    String formatQuery = SQLUtils.format(query, dbType, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION) + System.lineSeparator();
                    CommonUtils.writeFile(queryDir + queryName2QueryTemplate.getKey(), formatQuery);
                }
           }
        }
    }
}
