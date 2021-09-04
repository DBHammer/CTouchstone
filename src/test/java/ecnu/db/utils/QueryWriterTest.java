package ecnu.db.utils;

import com.alibaba.druid.util.JdbcConstants;
import ecnu.db.analyzer.statical.QueryWriter;
import ecnu.db.generator.constraintchain.filter.Parameter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
    public void testTemplatizeSqlInt() {
        String sql = "select * from test where a=5";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, null, "5"));
        String modified = queryWriter.templatizeSql("q1", sql, parameters);
        assertEquals("select * from test where a='0'", modified);
    }

    @Test
    void testTemplatizeSqlFloat() {
        String sql = "select * from test where a=1.5";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, null, "1.5"));
        String modified = queryWriter.templatizeSql("q2", sql, parameters);
        assertEquals("select * from test where a='0'", modified);
    }

    @Test
    void testTemplatizeSqlStr() {
        String sql = "select * from test where a='5'";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, null, "5"));
        String modified = queryWriter.templatizeSql("q3", sql, parameters);
        assertEquals("select * from test where a='0'", modified);
    }

    @Test
    void testTemplatizeSqlDate() {
        String sql = "select * from test where a='1998-12-12 12:00:00.000000'";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, null, "1998-12-12 12:00:00.000000"));
        String modified = queryWriter.templatizeSql("q4", sql, parameters);
        assertEquals("select * from test where a='0'", modified);
    }

    @Test
    void testTemplatizeSqlConflicts() {
        String sql = "select * from test where a='5' or b='5'";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, "db.test.a", "5"));
        parameters.add(new Parameter(1, "db.test.b", "5"));
        String modified = queryWriter.templatizeSql("q5", sql, parameters);
        assertEquals("-- conflictArgs:{id:0,data:'5',operand:db.test.a},{id:1,data:'5',operand:db.test.b}" + System.lineSeparator() + "select * from test where a='5' or b='5'", modified);
    }

    @Test
    void testTemplatizeSqlCannotFind() {
        String sql = "select * from test where a='5' or b='5'";
        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter(0, "db.test.b", "6");
        parameters.add(parameter);
        String modified = queryWriter.templatizeSql("q6", sql, parameters);
        assertEquals("-- cannotFindArgs:{id:0,data:'6',operand:db.test.b}" + System.lineSeparator() + "select * from test where a='5' or b='5'", modified);
    }
}