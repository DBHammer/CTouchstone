package ecnu.db.analyzer.statical;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.SystemConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangqingshuai
 * <p>
 * 使用阿里巴巴druid库 获取table信息
 */
public class QueryAliasParser {

    public Map<String, String> getTableAlias(boolean isCrossMultiDatabase, String databaseName, String sql, String dbType) {
        ExportTableAliasVisitor statVisitor = new ExportTableAliasVisitor(isCrossMultiDatabase, databaseName);
        SQLSelectStatement statement = (SQLSelectStatement) SQLUtils.parseStatements(sql, dbType).get(0);
        statement.accept(statVisitor);
        return statVisitor.getAliasMap();
    }

    private static class ExportTableAliasVisitor extends MySqlASTVisitorAdapter {
        private final boolean isCrossMultiDatabase;
        private final String databaseName;
        ExportTableAliasVisitor(boolean isCrossMultiDatabase, String databaseName) {
            this.isCrossMultiDatabase = isCrossMultiDatabase;
            this.databaseName = databaseName;
        }

        private final Map<String, String> aliasMap = new HashMap<>();

        @Override
        public boolean visit(SQLExprTableSource x) {
            if (x.getAlias() != null) {
                String tableName = x.getName().toString().toLowerCase();
                if (!isCrossMultiDatabase) {
                    aliasMap.put(x.getAlias().toLowerCase(), CommonUtils.addDBNamePrefix(databaseName, tableName));
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
