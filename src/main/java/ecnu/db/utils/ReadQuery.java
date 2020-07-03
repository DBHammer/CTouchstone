package ecnu.db.utils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author qingshuai.wang
 */
public class ReadQuery {
    public static List<String> getQueriesFromFile(String file, String dbType) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        StringBuilder fileContents = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.length() > 0) {
                if (!line.startsWith("--")) {
                    fileContents.append(line).append('\n');
                }
            }
        }
        List<SQLStatement> statementList = SQLUtils.parseStatements(fileContents.toString(), dbType, false);
        List<String> sqls = new ArrayList<>();
        for (SQLStatement sqlStatement : statementList) {
            String sql = SQLUtils.format(sqlStatement.toString(), dbType, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION);
            sql = sql.replace('\n', ' ');
            sql = sql.replace('\t', ' ');
            sql = sql.replaceAll(" +", " ");
            sqls.add(sql);
        }
        return sqls;
    }
}
