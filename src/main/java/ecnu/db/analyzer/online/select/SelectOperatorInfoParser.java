package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.StringReader;

/**
 * @author alan
 */
public class SelectOperatorInfoParser {
    public static SelectNode parse(@NonNull String operatorInfo) throws TouchstoneToolChainException, IOException {
        StringReader stringReader = new StringReader(operatorInfo);
        Yytoken andToken = new Yytoken(TokenType.LOGICAL_OPERATOR, "and");
        SelectNode root = new SelectNode(andToken);
        SelectOperatorInfoLexer lexer = new SelectOperatorInfoLexer(stringReader);
        State state = new LogicalState(null, root);
        do {
            Yytoken yytoken = lexer.yylex();
            if (yytoken == null) {
                break;
            }
            state = state.handle(yytoken);
        } while(!lexer.yyatEOF());

        return root;
    }
}
