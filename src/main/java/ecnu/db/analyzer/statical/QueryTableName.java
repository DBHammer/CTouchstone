package ecnu.db.analyzer.statical;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.TouchstoneToolChainException;

import java.util.HashSet;
import java.util.List;

/**
 * @author wangqingshuai
 */
public class QueryTableName {
    public static HashSet<String> getTableName(String filePath, String sql, String dbType, boolean isCrossMultiDatabase) throws TouchstoneToolChainException {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        SQLStatement stmt = stmtList.get(0);

        SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
        stmt.accept(statVisitor);
        HashSet<String> tableName = new HashSet<>();
        for (TableStat.Name name : statVisitor.getTables().keySet()) {
            if (!CommonUtils.isCanonicalTableName(name.getName()) && isCrossMultiDatabase) {
                throw new TouchstoneToolChainException(
                        String.format("'%s'文件的'%s'query中跨数据库的表'%s'形式必须为'<database>.<table>'",
                                filePath, sql, name.getName()));
            }
            tableName.add(name.getName().toLowerCase());
        }
        return tableName;
    }
}
