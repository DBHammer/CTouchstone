package ecnu.db.schema.column;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author qingshuai.wang
 */
public class StringColumn extends AbstractColumn {
    private final Set<String> likeCandidates = new HashSet<>();
    private int minLength;
    private int maxLength;
    private int ndv;

    public StringColumn() {
        super(null, ColumnType.VARCHAR);
    }

    public StringColumn(String columnName) {
        super(columnName, ColumnType.VARCHAR);
    }

    public int getMinLength() {
        return minLength;
    }

    public void setMinLength(int minLength) {
        this.minLength = minLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    @Override
    public int getNdv() {
        return this.ndv;
    }

    public void setNdv(int ndv) {
        this.ndv = ndv;
    }

    @Override
    protected String generateEqData(BigDecimal minProbability, BigDecimal maxProbability) {
        String eqCandidate;
        do {
            byte[] array = new byte[(maxLength > minLength ? new Random().nextInt(maxLength - minLength) : 0) + minLength];
            new Random().nextBytes(array);
            eqCandidate = new String(array, UTF_8);
        } while (eqCandidates.contains(eqCandidate));
        eqCandidates.add(eqCandidate);
        return eqCandidate;
    }

    public String generateLikeData(String likeStr) {
        String prefix = "", postfix = "";
        if (likeStr.startsWith("%")) {
            prefix = "%";
        }
        if (likeStr.endsWith("%")) {
            postfix = "%";
        }
        String likeCandidate;
        do {
            byte[] array = new byte[new Random().nextInt(maxLength - minLength) + minLength];
            new Random().nextBytes(array);
            likeCandidate = String.format("%s%s%s", prefix, new String(array, UTF_8), postfix);
        } while (likeCandidates.contains(likeCandidate));
        likeCandidates.add(likeCandidate);
        return likeCandidate;
    }
}
