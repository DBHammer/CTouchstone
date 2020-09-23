package ecnu.db.utils;

import ecnu.db.exception.UnsupportedDBTypeException;

import java.util.Set;

/**
 * @author wangqingshuai
 */
public abstract class AbstractDatabaseInfo {
    protected TouchstoneSupportedDatabaseVersion touchstoneSupportedDatabaseVersion;

    public AbstractDatabaseInfo(TouchstoneSupportedDatabaseVersion touchstoneSupportedDatabaseVersion) {
        this.touchstoneSupportedDatabaseVersion = touchstoneSupportedDatabaseVersion;
    }

    /**
     * 获取sql的查询计划中，需要使用查询计划的列名
     *
     * @return sql的查询计划中，需要使用查询计划的列名
     * @throws UnsupportedDBTypeException 为对当前analyzer实例化的数据库版本实现该方法
     */
    public abstract String[] getSqlInfoColumns() throws UnsupportedDBTypeException;

    /**
     * 获取数据库使用的静态解析器的数据类型
     *
     * @return 静态解析器使用的数据库类型
     */
    public abstract String getStaticalDbVersion();

    /**
     * 该Analyzer支持的数据库版本
     *
     * @return 该分析器支持的数据库版本
     */
    public abstract Set<TouchstoneSupportedDatabaseVersion> getSupportedDatabaseVersions();

    /**
     * jdbc 连接器需要的类型
     *
     * @return jdbc连接器类型
     */
    public abstract String getJdbcType();

    /**
     * jdbc 连接的properties
     *
     * @return jdbc properties
     */
    public abstract String getJdbcProperties();
}
