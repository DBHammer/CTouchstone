package ecnu.db.analyzer.online.select;

import ecnu.db.analyzer.online.select.tidb.TidbSelectOperatorInfoParser;
import ecnu.db.utils.TouchstoneToolChainException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SelectOperatorInfoParserTest {
    @DisplayName("test SelectOperatorInfoParser.parse method")
    @Test
    void testParse() throws Exception {
        SelectNode node = TidbSelectOperatorInfoParser.parse("ge(db.table.col, 2)");
        assertEquals(node.toString(), "and(ge(db.table.col, 2))");
    }
    @DisplayName("test SelectOperatorInfoParser.parse method with arithmetic ops")
    @Test
    void testParseWithArithmeticOps() throws Exception {
        SelectNode node = TidbSelectOperatorInfoParser.parse("ge(mul(db.table.col1, plus(db.table.col2, 3)), 2)");
        assertEquals(node.toString(), "and(ge(mul(db.table.col1, plus(db.table.col2, 3)), 2))");
    }
    @DisplayName("test SelectOperatorInfoParser.parse method with logical ops")
    @Test
    void testParseWithLogicalOps() throws Exception {
        SelectNode node = TidbSelectOperatorInfoParser.parse("or(ge(db.table.col1, 2), lt(db.table.col2, 3))");
        assertEquals(node.toString(), "and(or(ge(db.table.col1, 2), lt(db.table.col2, 3)))");
    }
    @DisplayName("test SelectOperatorInfoParser.parse method with erroneous grammar")
    @Test()
    void testParseWithLogicalOpsFailed() {
        assertThrows(TouchstoneToolChainException.class, () -> {
            TidbSelectOperatorInfoParser.parse("or(ge(db.table.col1, 2), mul(db.table.col2, 3))");
        });
    }
}