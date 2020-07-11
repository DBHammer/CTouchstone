package ecnu.db.analyzer.online.select.tidb;

import ecnu.db.analyzer.online.select.*;
import ecnu.db.utils.TouchstoneToolChainException;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.StringReader;

/**
 * @author alan
 */
public class TidbSelectOperatorInfoParser {
    public static SelectNode parse(@NonNull String operatorInfo) throws TouchstoneToolChainException, IOException {
        StringReader stringReader = new StringReader(operatorInfo);
        Token andToken = new Token(TokenType.LOGICAL_OPERATOR, "and");
        SelectNode root = new SelectNode(andToken);
        TidbSelectOperatorInfoLexer lexer = new TidbSelectOperatorInfoLexer(stringReader);
        BaseState state = new LogicalState(null, root);
        do {
            Token yytoken = lexer.yylex();
            if (yytoken == null) {
                break;
            }
            state = state.handle(yytoken);
        } while(!lexer.yyatEOF());

        return root;
    }
}
