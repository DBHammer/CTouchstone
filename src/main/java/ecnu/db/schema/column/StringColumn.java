package ecnu.db.schema.column;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.constraintchain.filter.operation.CompareOperator;
import ecnu.db.schema.column.bucket.EqBucket;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static ecnu.db.utils.CommonUtils.shuffle;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author qingshuai.wang
 */
public class StringColumn extends AbstractColumn {
    private final Map<String, BigDecimal> likeCandidates = new HashMap<>();
    private int minLength;
    private int maxLength;
    private int ndv;

    private int[] intCopyOfTupleData;
    private String[] tupleData;
    private BiMap<String, Integer> eqCandidateMap;
    private BiMap<String, Pair<Integer, Integer>> likeCandidateMap;
    private BiMap<Integer, String> strings;

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
    protected String generateEqParamData(BigDecimal minProbability, BigDecimal maxProbability) {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        String eqCandidate;
        do {
            eqCandidate = randomString(rand);
        } while (eqCandidates.contains(eqCandidate));
        eqCandidates.add(eqCandidate);
        return eqCandidate;
    }

    private String randomString(ThreadLocalRandom rand) {
        byte[] array = new byte[rand.nextInt( 0, maxLength - minLength + 1) + minLength];
        rand.nextBytes(array);
        return new String(array, UTF_8);
    }

    @Override
    public String generateNonEqParamData(BigDecimal probability) {
        throw new UnsupportedOperationException();
    }

    public String generateLikeParamData(BigDecimal probability, String likeStr) {
        String prefix = "", postfix = "";
        if (likeStr.startsWith("%")) {
            prefix = "%";
        }
        if (likeStr.endsWith("%")) {
            postfix = "%";
        }
        String likeCandidate;
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        do {
            byte[] array = new byte[rand.nextInt(maxLength - minLength + 1) + minLength];
            rand.nextBytes(array);
            likeCandidate = String.format("%s%s%s", prefix, new String(array, UTF_8), postfix);
        } while (likeCandidates.containsKey(likeCandidate));
        likeCandidates.put(likeCandidate, probability);
        return likeCandidate;
    }

    // todo 暂时不考虑like和eq，like内部之间的影响
    @Override
    public void prepareTupleData(int size) {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        eqBuckets.sort(Comparator.comparing(o -> o.leftBorder));
        BigDecimal cumBorder = BigDecimal.ZERO, sizeVal = BigDecimal.valueOf(size);
        if (eqCandidateMap == null && likeCandidateMap == null && strings == null) {
            initDistinctStrings(size, rand);
        }
        if (intCopyOfTupleData == null || this.intCopyOfTupleData.length != size) {
            intCopyOfTupleData = new int[size];
        }
        for (EqBucket eqBucket : eqBuckets) {
            for (Map.Entry<BigDecimal, Parameter> entry : eqBucket.eqConditions.entries()) {
                BigDecimal newCum = cumBorder.add(entry.getKey().multiply(sizeVal));
                int eqValue = convertString2Tag(entry.getValue().getData());
                for (int j = cumBorder.intValue(); j < newCum.intValue() && j < size; j++) {
                    intCopyOfTupleData[j] = eqValue;
                }
                cumBorder = newCum;
            }
        }
        int likeSize = 0;
        for (String likeCandidate : likeCandidates.keySet()) {
            BigDecimal probability = likeCandidates.get(likeCandidate);
            BigDecimal newCum = cumBorder.add(probability.multiply(sizeVal));
            Pair<Integer, Integer> pair = likeCandidateMap.get(likeCandidate);
            int likeMinIdx = pair.getLeft(), likeMaxIdx = pair.getRight(), start = cumBorder.intValue(), end = newCum.intValue();
            int likeNdv = likeMaxIdx - likeMinIdx + 1;
            for (int j = start; j < end && j < size; j++) {
                intCopyOfTupleData[j] = likeMinIdx + ((j - start) % likeNdv);
            }
            likeSize += likeNdv;
            cumBorder = newCum;
        }
        int bound = ndv - eqCandidates.size() - likeSize;
        if (bound < 0) {
            throw new UnsupportedOperationException();
        }
        if (bound > 0) {
            for (int i = cumBorder.intValue(); i < size; i++) {
                intCopyOfTupleData[i] = rand.nextInt(bound) + eqCandidates.size() + likeSize;
            }
        }
        if (cumBorder.compareTo(BigDecimal.ZERO) > 0) {
            shuffle(size, rand, intCopyOfTupleData);
        }
        if (tupleData == null || tupleData.length != size) {
            tupleData = new String[size];
        }
        for (int i = 0; i < size; i++) {
            int tag = intCopyOfTupleData[i];
            if (strings.containsKey(tag)) {
                tupleData[i] = strings.get(tag);
            } else {
                tupleData[i] = randomString(rand);
            }
        }
    }

    private void initDistinctStrings(int size, ThreadLocalRandom rand) {
        eqCandidateMap = HashBiMap.create();
        likeCandidateMap = HashBiMap.create();
        strings = HashBiMap.create();
        int idx = 0;
        for (String candidate : eqCandidates) {
            strings.put(idx, candidate);
            eqCandidateMap.put(candidate, idx);
            idx++;
        }
        for (String candidate : likeCandidates.keySet()) {
            int likeNdv = likeCandidates.get(candidate).multiply(BigDecimal.valueOf(ndv)).intValue();
            likeCandidateMap.put(candidate, Pair.of(idx, idx + likeNdv - 1));
            int newIdx = idx + likeNdv;
            for (; idx < newIdx; idx++) {
                String generatedStr;
                do {
                    generatedStr = generateLikeTupleData(rand, candidate);
                } while (strings.inverse().containsKey(generatedStr));
                strings.put(idx, generatedStr);
            }
            idx = newIdx;
        }
    }

    private String generateLikeTupleData(ThreadLocalRandom rand, String candidate) {
        boolean startTag = false, endTag = false;
        if (candidate.startsWith("%")) {
            startTag = true;
        }
        if (candidate.endsWith("%")) {
            endTag = true;
        }
        String generatedStr;
        if (!startTag && !endTag) {
            generatedStr = candidate;
        }
        else if (!startTag) {
            int postLen = rand.nextInt(0, maxLength - (candidate.length() - 1) + 1);
            byte[] array = new byte[postLen];
            rand.nextBytes(array);
            String postStr = new String(array, UTF_8);
            generatedStr = candidate.substring(0, candidate.length() - 1) + postStr;
        }
        else if (!endTag) {
            int preLen =  rand.nextInt(0, maxLength - (candidate.length() - 1) + 1);
            byte[] array = new byte[preLen];
            rand.nextBytes(array);
            String preStr = new String(array, UTF_8);
            generatedStr = preStr + candidate.substring(1);
        }
        else {
            int preLen = rand.nextInt(0, maxLength - (candidate.length() - 2) + 1), postLen = maxLength - preLen - (candidate.length() - 2);
            String preStr = "", postStr = "";
            if (preLen > 0) {
                byte[] preArray;
                preArray = new byte[preLen];
                rand.nextBytes(preArray);
                preStr = new String(preArray, UTF_8);
            }
            if (postLen > 0) {
                byte[] postArray;
                postArray = new byte[postLen];
                postStr = new String(postArray, UTF_8);
            }
            generatedStr = preStr + candidate.substring(0, candidate.length() - 2) + postStr;
        }
        return generatedStr;
    }

    public int[] evaluate() {
        return intCopyOfTupleData;
    }

    @Override
    public boolean[] evaluate(CompareOperator operator, List<Parameter> parameters, boolean hasNot) {
        boolean[] ret = new boolean[intCopyOfTupleData.length];
        switch (operator) {
            case EQ:
                for (int i = 0; i < intCopyOfTupleData.length; i++) {
                    ret[i] = (!hasNot & (intCopyOfTupleData[i] == eqCandidateMap.get(parameters.get(0).getData())));
                }
                break;
            case NE:
                for (int i = 0; i < intCopyOfTupleData.length; i++) {
                    ret[i] = (!hasNot & (intCopyOfTupleData[i] != eqCandidateMap.get(parameters.get(0).getData())));
                }
                break;
            case IN:
                int[] parameterData = new int[parameters.size()];
                for (int i = 0; i < parameterData.length; i++) {
                    parameterData[i] = eqCandidateMap.get(parameters.get(i).getData());
                }
                for (int i = 0; i < intCopyOfTupleData.length; i++) {
                    ret[i] = false;
                    for (double paramDatum : parameterData) {
                        ret[i] = (ret[i] | (!hasNot & (intCopyOfTupleData[i] == paramDatum)));
                    }
                }
                break;
            case LIKE:
                Pair<Integer, Integer> pair = likeCandidateMap.get(parameters.get(0).getData());
                int min = pair.getLeft(), max = pair.getRight();
                for (int i = 0; i < intCopyOfTupleData.length; i++) {
                    ret[i] = (!hasNot & (intCopyOfTupleData[i] >= min && intCopyOfTupleData[i] <= max));
                }
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return ret;
    }

    @JsonIgnore
    public String[] getTupleData() {
        return tupleData;
    }


    public int convertString2Tag(String str) {
        return eqCandidateMap.get(str);
    }
}
