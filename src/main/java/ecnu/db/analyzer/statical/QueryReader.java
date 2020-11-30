package ecnu.db.analyzer.statical;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.utils.CommonUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * @author qingshuai.wang
 */
public class QueryReader {
    private static final ExportTableAliasVisitor statVisitor = new ExportTableAliasVisitor();

    public static List<String> getQueriesFromFile(String file, DbType dbType) throws IOException {
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

    public static HashSet<String> getTableName(String sql, DbType dbType) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        SQLStatement stmt = stmtList.get(0);

        SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
        stmt.accept(statVisitor);
        HashSet<String> tableName = new HashSet<>();
        for (TableStat.Name name : statVisitor.getTables().keySet()) {
            tableName.add(CommonUtils.addDatabaseNamePrefix(name.getName().toLowerCase()));
        }
        return tableName;
    }

    public static Map<String, String> getTableAlias(String sql, DbType dbType) throws TouchstoneException {
        SQLStatement sqlStatement = SQLUtils.parseStatements(sql, dbType).get(0);
        if (!(sqlStatement instanceof SQLSelectStatement)) {
            throw new TouchstoneException("Only support select statement");
        }
        SQLSelectStatement statement = (SQLSelectStatement) sqlStatement;
        statement.accept(statVisitor);
        return statVisitor.getAliasMap();
    }

    private static class ExportTableAliasVisitor extends MySqlASTVisitorAdapter {
        private final Map<String, String> aliasMap = new HashMap<>();

        @Override
        public boolean visit(SQLExprTableSource x) {
            if (x.getAlias() != null) {
                aliasMap.put(x.getAlias().toLowerCase(), CommonUtils.addDatabaseNamePrefix(x.getName().toString().toLowerCase()));
            }
            return true;
        }

        public Map<String, String> getAliasMap() {
            return aliasMap;
        }
    }
}
