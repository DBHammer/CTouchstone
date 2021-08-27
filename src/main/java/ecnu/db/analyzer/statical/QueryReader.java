package ecnu.db.analyzer.statical;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import ecnu.db.utils.exception.TouchstoneException;
import ecnu.db.utils.exception.analyze.IllegalQueryTableNameException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.convertTableName2CanonicalTableName;
import static ecnu.db.utils.CommonUtils.matchPattern;

/**
 * @author qingshuai.wang
 */
public class QueryReader {
    private static final Pattern CANONICAL_TBL_NAME = Pattern.compile("[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+");
    private final ExportTableAliasVisitor aliasVisitor;
    private final String queriesDir;
    private DbType dbType;

    public QueryReader(String defaultDatabaseName, String queriesDir) {
        if (defaultDatabaseName == null) {
            this.aliasVisitor = new ExportTableAliasVisitor();
        } else {
            this.aliasVisitor = new ExportTableAliasVisitor(defaultDatabaseName);
        }
        this.queriesDir = queriesDir;
    }

    public void setDbType(DbType dbType) {
        this.dbType = dbType;
    }

    public List<File> loadQueryFiles() {
        return Optional.ofNullable(new File(queriesDir).listFiles())
                .map(Arrays::asList)
                .orElse(new ArrayList<>())
                .stream()
                .filter((file) -> file.isFile() && file.getName().endsWith(".sql"))
                .collect(Collectors.toList());
    }

    public List<String> getQueriesFromFile(String file) throws IOException {
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

    public HashSet<String> getTableName(String sql) throws IllegalQueryTableNameException {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        SQLStatement stmt = stmtList.get(0);
        SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
        stmt.accept(statVisitor);
        HashSet<String> tableName = new HashSet<>();
        for (TableStat.Name name : statVisitor.getTables().keySet()) {
            tableName.add(aliasVisitor.addDatabaseNamePrefix(name.getName().toLowerCase()));
        }
        return tableName;
    }

    public Map<String, String> getTableAlias(String sql) throws TouchstoneException {
        SQLStatement sqlStatement = SQLUtils.parseStatements(sql, dbType).get(0);
        if (!(sqlStatement instanceof SQLSelectStatement)) {
            throw new TouchstoneException("Only support select statement");
        }
        SQLSelectStatement statement = (SQLSelectStatement) sqlStatement;
        statement.accept(aliasVisitor);
        return aliasVisitor.getAliasMap();
    }

    /**
     * @param files SQL文件
     * @return 所有查询中涉及到的表名
     * @throws IOException 从SQL文件中获取Query失败
     */
    public List<String> fetchTableNames(List<File> files) throws IOException, IllegalQueryTableNameException {
        List<String> tableNames = new ArrayList<>();
        for (File sqlFile : files) {
            List<String> queries = getQueriesFromFile(sqlFile.getPath());
            for (String query : queries) {
                Set<String> tableNameRefs = getTableName(query);
                tableNames.addAll(tableNameRefs);
            }
        }
        tableNames = tableNames.stream().distinct().collect(Collectors.toList());
        return tableNames;
    }

    private static class ExportTableAliasVisitor extends MySqlASTVisitorAdapter {
        private final Map<String, String> aliasMap = new HashMap<>();
        private final String defaultDatabaseName;

        public ExportTableAliasVisitor(String defaultDatabaseName) {
            this.defaultDatabaseName = defaultDatabaseName;
        }

        public ExportTableAliasVisitor() {
            this.defaultDatabaseName = null;
        }


        @Override
        public boolean visit(SQLExprTableSource x) {
            if (x.getAlias() != null) {
                try {
                    aliasMap.put(x.getAlias().toLowerCase(), addDatabaseNamePrefix(x.getName().toString().toLowerCase()));
                } catch (IllegalQueryTableNameException e) {
                    e.printStackTrace();
                }
            }
            return true;
        }

        public Map<String, String> getAliasMap() {
            return aliasMap;
        }

        /**
         * 单个数据库时把表转换为<database>.<table>的形式
         *
         * @param tableName 表名
         * @return 转换后的表名
         */
        public String addDatabaseNamePrefix(String tableName) throws IllegalQueryTableNameException {
            return convertTableName2CanonicalTableName(tableName, CANONICAL_TBL_NAME, defaultDatabaseName);
        }
    }


}
