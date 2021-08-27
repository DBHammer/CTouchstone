package ecnu.db.analyzer.statical;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.Token;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.generator.constraintchain.filter.Parameter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author alan
 */
public class QueryWriter {
    private static final Logger logger = LoggerFactory.getLogger(QueryWriter.class);
    private final String queryDir;
    private DbType dbType;

    public QueryWriter(String queryDir) {
        this.queryDir = queryDir;
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
        Multimap<String, Pair<Integer, Integer>> literalMap = ArrayListMultimap.create();
        int lastPos = 0, pos;
        while (!lexer.isEOF()) {
            lexer.nextToken();
            Token token = lexer.token();
            pos = lexer.pos();
            if (token == Token.LITERAL_INT || token == Token.LITERAL_FLOAT || token == Token.LITERAL_CHARS) {
                String str = query.substring(lastPos, pos).trim();
                literalMap.put(str, Pair.of(pos - str.length(), pos));
            }
            lastPos = pos;
        }

        // replacement
        List<Parameter> cannotFindArgs = new ArrayList<>(), conflictArgs = new ArrayList<>();
        TreeMap<Integer, Pair<Parameter, Pair<Integer, Integer>>> replaceParams = new TreeMap<>();
        for (Parameter parameter : parameters) {
            String data = parameter.getDataValue();
            Collection<Pair<Integer, Integer>> matches = literalMap.get(data);
            matches.addAll(literalMap.get("'" + data + "'"));
            if (matches.size() == 0) {
                cannotFindArgs.add(parameter);
            } else if (matches.size() > 1) {
                conflictArgs.add(parameter);
            } else {
                Pair<Integer, Integer> pair = matches.stream().findFirst().get();
                int startPos = pair.getLeft();
                replaceParams.put(startPos, Pair.of(parameter, pair));
            }
        }
        StringBuilder fragments = new StringBuilder();
        int currentPos = 0;
        while (!replaceParams.isEmpty()) {
            Map.Entry<Integer, Pair<Parameter, Pair<Integer, Integer>>> entry = replaceParams.pollFirstEntry();
            Pair<Parameter, Pair<Integer, Integer>> pair = entry.getValue();
            Parameter parameter = pair.getKey();
            int startPos = pair.getValue().getLeft(), endPos = pair.getValue().getRight();
            fragments.append(query, currentPos, startPos).append(String.format("'%s'", parameter.getId()));
            currentPos = endPos;
        }
        fragments.append(query.substring(currentPos));
        query = fragments.toString();
        if (cannotFindArgs.size() > 0) {
            logger.warn(String.format("请注意%s中有参数无法完成替换，请查看该sql输出，手动替换;", queryCanonicalName));
            query = appendArgs("cannotFindArgs", cannotFindArgs) + query;
        }
        if (conflictArgs.size() > 0) {
            logger.warn(String.format("请注意%s中有参数出现多次，无法智能，替换请查看该sql输出，手动替换;", queryCanonicalName));
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
        String argsString = params.stream().map(
                        (parameter) ->
                                String.format("{id:%s,data:%s,operand:%s}",
                                        parameter.getId(),
                                        "'" + parameter.getDataValue() + "'",
                                        parameter.getOperand()
                                ))
                .collect(Collectors.joining(","));
        return String.format("-- %s:%s%s", title, argsString, System.lineSeparator());
    }

    public void writeQuery(String queryFileName, String query) throws IOException {
        String formatQuery = SQLUtils.format(query, dbType, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION) + System.lineSeparator();
        FileUtils.writeStringToFile(new File(queryDir + queryFileName), formatQuery, UTF_8);
    }
}
