package ecnu.db.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

class StringTemplate {
    private static final char[] randomCharSet = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray();
    private static final char[] likeRandomCharSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ-".toCharArray();
    int minLength;
    int rangeLength;
    long specialValue;

    private boolean hasNonEqConstraint;
    Map<Long, boolean[]> likeIndex2Status = new HashMap<>();

    public StringTemplate(int minLength, int rangeLength, long specialValue) {
        this.minLength = minLength;
        this.rangeLength = rangeLength;
        this.specialValue = specialValue;
    }

    public String getParameterValue(long dataId) {
        return new String(getParameterBuilder(dataId));
    }

    private char[] getParameterBuilder(long dataId) {
        Random random = new Random(specialValue * dataId);
        char[] values = new char[minLength + random.nextInt(rangeLength + 1)];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomCharSet[random.nextInt(randomCharSet.length)];
        }
        return values;
    }

    public String getLikeValue(long dataId, String originValue) {
        char[] likeValue = getParameterBuilder(dataId);
        likeIndex2Status.put(dataId, new boolean[3]);
        int firstChangeIndex = 0;
        if (originValue.startsWith("%")) {
            likeValue[0] = '%';
            originValue = originValue.substring(1);
            likeIndex2Status.get(dataId)[0] = true;
            firstChangeIndex = 1;
        }
        if (originValue.endsWith("%")) {
            likeValue[likeValue.length - 1] = '%';
            originValue = originValue.substring(0, originValue.length() - 1);
            likeIndex2Status.get(dataId)[1] = true;
        }
        if (originValue.contains("%")) {
            likeValue[likeValue.length / 2] = '%';
            likeIndex2Status.get(dataId)[2] = true;
        }
        likeValue[firstChangeIndex] = likeRandomCharSet[likeValue[firstChangeIndex] % likeRandomCharSet.length];
        return new String(likeValue);
    }

    public Map<Long, boolean[]> getLikeIndex2Status() {
        return likeIndex2Status;
    }

    public void setLikeIndex2Status(Map<Long, boolean[]> likeIndex2Status) {
        this.likeIndex2Status = likeIndex2Status;
    }

    //todo deal with the like operator and the compare operator at the same time
    public String transferColumnData2Value(long data, long range) {
        if (likeIndex2Status.size() > 0 && likeIndex2Status.containsKey(data)) {
            char[] value = getParameterBuilder(data);
            boolean[] status = likeIndex2Status.get(data);
            int firstChangeIndex = 0;
            if (status[0]) {
                value[0] = randomCharSet[ThreadLocalRandom.current().nextInt(randomCharSet.length)];
                firstChangeIndex = 1;
            }
            if (status[1]) {
                value[value.length - 1] = randomCharSet[ThreadLocalRandom.current().nextInt(randomCharSet.length)];
            }
            if (status[2]) {
                value[value.length / 2] = randomCharSet[ThreadLocalRandom.current().nextInt(randomCharSet.length)];
            }
            value[firstChangeIndex] = likeRandomCharSet[value[firstChangeIndex] % likeRandomCharSet.length];
            return new String(value);
        } else if (hasNonEqConstraint) {
            char[] value = getParameterBuilder(data);
            int length = Long.toString(range).length();
            String currentData = Long.toString(data);
            int lengthCurrent = currentData.length();
            int diff = length - lengthCurrent;
            for (int i = 0; i < diff; i++) {
                value[i] = '0';
            }
            for (int i = 0; i < lengthCurrent; i++) {
                value[i + diff] = currentData.charAt(i);
            }
            return new String(value);
        } else {
            return getParameterValue(data);
        }
    }
}