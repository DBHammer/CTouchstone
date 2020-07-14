package ecnu.db.analyzer.online.select.tidb;

import ecnu.db.analyzer.online.select.SelectNode;
import java_cup.runtime.ComplexSymbolFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TidbSelectOperatorInfoParserTest {
    private TidbSelectOperatorInfoLexer lexer = new TidbSelectOperatorInfoLexer(new StringReader(""));
    private TidbSelectOperatorInfoParser parser = new TidbSelectOperatorInfoParser(lexer, new ComplexSymbolFactory());

    @DisplayName("test TidbSelectOperatorInfoParser.parse method")
    @Test
    void testParse() throws Exception {
        lexer.yyreset(new StringReader("ge(db.table.col, 2)"));
        parser.parse();
        SelectNode node = parser.getRoot();
        assertEquals(node.toString(), "and(ge(canonical_column_name(db.table.col), integer(2)))");
    }
    @DisplayName("test TidbSelectOperatorInfoParser.parse method with arithmetic ops")
    @Test
    void testParseWithArithmeticOps() throws Exception {
        lexer.yyreset(new StringReader("ge(mul(db.table.col1, plus(db.table.col2, 3)), 2)"));
        parser.parse();
        SelectNode node = parser.getRoot();
        assertEquals(node.toString(), "and(ge(mul(canonical_column_name(db.table.col1), plus(canonical_column_name(db.table.col2), integer(3))), integer(2)))");
    }
    @DisplayName("test TidbSelectOperatorInfoParser.parse method with logical ops")
    @Test
    void testParseWithLogicalOps() throws Exception {
        lexer.yyreset(new StringReader("or(ge(db.table.col1, 2), lt(db.table.col2, 3.0))"));
        parser.parse();
        SelectNode node = parser.getRoot();
        assertEquals(node.toString(), "and(or(ge(canonical_column_name(db.table.col1), integer(2)), lt(canonical_column_name(db.table.col2), float(3.0))))");
    }
    @DisplayName("test TidbSelectOperatorInfoParser.parse method with erroneous grammar")
    @Test()
    void testParseWithLogicalOpsFailed() {
        assertThrows(Exception.class, () -> {
            lexer.yyreset(new StringReader("or(ge((db.table.col1), 2), mul(db.table.col2, 3))"));
            parser.parse();
        });
    }
    @DisplayName("test TidbSelectOperatorInfoParser.parse method with not")
    @Test()
    void testParseWithNot() throws Exception {
        lexer.yyreset(new StringReader("or(ge(db.table.col1, 2), not(in(db.table.col2, \"3\", \"2\")))"));
        parser.parse();
        SelectNode node = parser.getRoot();
        assertEquals(node.toString(), "and(or(ge(canonical_column_name(db.table.col1), integer(2)), not(in(canonical_column_name(db.table.col2), string(3), string(2)))))");
    }
    @DisplayName("test TidbSelectOperatorInfoParser.parse method with isnull")
    @Test()
    void testParseWithIsnull() throws Exception {
        lexer.yyreset(new StringReader("or(ge(db.table.col1, 2), not(isnull(db.table.col2)))"));
        parser.parse();
        SelectNode node = parser.getRoot();
        assertEquals(node.toString(), "and(or(ge(canonical_column_name(db.table.col1), integer(2)), not(isnull(canonical_column_name(db.table.col2)))))");
    }
}