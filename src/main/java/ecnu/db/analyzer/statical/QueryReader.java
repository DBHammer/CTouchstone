package ecnu.db.analyzer.statical;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import ecnu.db.utils.TouchstoneSupportedDatabaseVersion;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author qingshuai.wang
 */
public class QueryReader {
    public static List<String> getQueriesFromFile(String file, String dbType) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        StringBuilder fileContents = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.length() > 0) {
                if (!line.startsWith("--")) {
                    fileContents.append(line).append(System.lineSeparator());
                }
            }
        }
        List<SQLStatement> statementList = SQLUtils.parseStatements(fileContents.toString(), dbType, false);
        List<String> sqls = new ArrayList<>();
        for (SQLStatement sqlStatement : statementList) {
            String sql = SQLUtils.format(sqlStatement.toString(), dbType, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION);
            sql = sql.replace(System.lineSeparator(), " ");
            sql = sql.replace('\t', ' ');
            sql = sql.replaceAll(" +", " ");
            sqls.add(sql);
        }
        return sqls;
    }
}
