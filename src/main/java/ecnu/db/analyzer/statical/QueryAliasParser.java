package ecnu.db.analyzer.statical;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.utils.CommonUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangqingshuai
 * <p>
 * 使用阿里巴巴druid库 获取table信息
 */
public class QueryAliasParser {

    public Map<String, String> getTableAlias(boolean isCrossMultiDatabase, String databaseName, String sql, String dbType) throws TouchstoneToolChainException {
        ExportTableAliasVisitor statVisitor = new ExportTableAliasVisitor(isCrossMultiDatabase, databaseName);
        SQLStatement sqlStatement = SQLUtils.parseStatements(sql, dbType).get(0);
        if (!(sqlStatement instanceof SQLSelectStatement)) {
            throw new TouchstoneToolChainException("Only support select statement");
        }
        SQLSelectStatement statement = (SQLSelectStatement) sqlStatement;
        statement.accept(statVisitor);
        return statVisitor.getAliasMap();
    }

    private static class ExportTableAliasVisitor extends MySqlASTVisitorAdapter {
        private final boolean isCrossMultiDatabase;
        private final String databaseName;
        private final Map<String, String> aliasMap = new HashMap<>();

        ExportTableAliasVisitor(boolean isCrossMultiDatabase, String databaseName) {
            this.isCrossMultiDatabase = isCrossMultiDatabase;
            this.databaseName = databaseName;
        }

        @Override
        public boolean visit(SQLExprTableSource x) {
            if (x.getAlias() != null) {
                String tableName = x.getName().toString().toLowerCase();
                if (!isCrossMultiDatabase) {
                    aliasMap.put(x.getAlias().toLowerCase(), CommonUtils.addDatabaseNamePrefix(databaseName, tableName));
                } else {
                    aliasMap.put(x.getAlias().toLowerCase(), tableName);
                }
            }
            return true;
        }

        public Map<String, String> getAliasMap() {
            return aliasMap;
        }
    }
}
