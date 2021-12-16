package ecnu.db.utils;

import com.alibaba.druid.util.JdbcConstants;
import ecnu.db.analyzer.statical.QueryWriter;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.schema.Column;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.ColumnType;
import ecnu.db.utils.exception.TouchstoneException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryWriterTest {
    final static QueryWriter queryWriter = new QueryWriter("");

    @BeforeAll
    static void setUp() {
        queryWriter.setDbType(JdbcConstants.MYSQL);
    }

    @Test
    public void testTemplatizeSqlInt() throws SQLException, ClassNotFoundException {
        String sql = "select * from test where a=5";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, null, "5"));
        String modified = queryWriter.templatizeSql("q1", sql, parameters);
        assertEquals("select * from test where a='0'", modified);
    }

    @Test
    void testTemplatizeSqlFloat() throws SQLException, ClassNotFoundException {
        String sql = "select * from test where a=1.5";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, null, "1.5"));
        String modified = queryWriter.templatizeSql("q2", sql, parameters);
        assertEquals("select * from test where a='0'", modified);
    }

    @Test
    void testTemplatizeSqlStr() throws SQLException, ClassNotFoundException {
        String sql = "select * from test where a='5'";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, null, "5"));
        String modified = queryWriter.templatizeSql("q3", sql, parameters);
        assertEquals("select * from test where a='0'", modified);
    }

    @Test
    void testTemplatizeSqlDate() throws SQLException, ClassNotFoundException {
        String sql = "select * from test where a='1998-12-12 12:00:00.000000'";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, null, "1998-12-12 12:00:00.000000"));
        String modified = queryWriter.templatizeSql("q4", sql, parameters);
        assertEquals("select * from test where a='0'", modified);
    }

    @Test
    void testTemplatizeSqlConflicts() throws SQLException, TouchstoneException {
        String sql = "select * from test where a='5' or b='5'";
        List<Parameter> parameters = new ArrayList<>();
        ColumnManager.getInstance().addColumn("db.test.a", new Column(ColumnType.INTEGER));
        ColumnManager.getInstance().addColumn("db.test.b", new Column(ColumnType.INTEGER));
        parameters.add(new Parameter(0, "db.test.a", "5"));
        parameters.add(new Parameter(1, "db.test.b", "5"));
        String modified = queryWriter.templatizeSql("q5", sql, parameters);
        assertEquals("select * from test where a='0' or b='1'", modified);
    }

    @Test
    void testTemplatizeSqlCannotFind() throws SQLException {
        String sql = "select * from test where a='5' or b='5'";
        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter(0, "db.test.b", "6");
        parameters.add(parameter);
        String modified = queryWriter.templatizeSql("q6", sql, parameters);
        assertEquals("-- cannotFindArgs:{id:0,data:'6',operand:db.test.b}" + System.lineSeparator() + "select * from test where a='5' or b='5'", modified);
    }
}