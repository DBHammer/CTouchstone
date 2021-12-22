package ecnu.db.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

class StringTemplate {
    int minLength;
    int rangeLength;
    long specialValue;
    Map<Long, boolean[]> likeIndex2Status = new HashMap<>();
    private static final char[] randomCharSet = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray();

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
        if (originValue.startsWith("%")) {
            likeValue[0] = '%';
            originValue = originValue.substring(1);
            likeIndex2Status.get(dataId)[0] = true;
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
        return new String(likeValue);
    }

    public Map<Long, boolean[]> getLikeIndex2Status() {
        return likeIndex2Status;
    }

    public void setLikeIndex2Status(Map<Long, boolean[]> likeIndex2Status) {
        this.likeIndex2Status = likeIndex2Status;
    }

    public String transferColumnData2Value(long data) {
        if (likeIndex2Status.size() > 0 && likeIndex2Status.containsKey(data)) {
            char[] value = getParameterBuilder(data);
            boolean[] status = likeIndex2Status.get(data);
            if (status[0]) {
                value[0] = randomCharSet[ThreadLocalRandom.current().nextInt(randomCharSet.length)];
            }
            if (status[1]) {
                value[value.length - 1] = randomCharSet[ThreadLocalRandom.current().nextInt(randomCharSet.length)];
            }
            if (status[2]) {
                value[value.length / 2] = randomCharSet[ThreadLocalRandom.current().nextInt(randomCharSet.length)];
            }
            return new String(value);
        } else {
            return getParameterValue(data);
        }
    }
}