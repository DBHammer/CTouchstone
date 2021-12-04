package ecnu.db.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

class StringTemplate {
    int minLength;
    int rangeLength;
    long specialValue;
    Map<Long, boolean[]> likeIndex2Status = new HashMap<>();

    public StringTemplate(int minLength, int rangeLength, long specialValue) {
        this.minLength = minLength;
        this.rangeLength = rangeLength;
        this.specialValue = specialValue;
    }

    public String getParameterValue(long dataId) {
        return getParameterBuilder(dataId).toString();
    }

    private StringBuilder getParameterBuilder(long dataId) {
        Random random = new Random(specialValue * dataId);
        return random.ints(65, 123)
                .limit(minLength + random.nextInt(rangeLength + 1)).
                collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append);
    }

    public String getLikeValue(long dataId, String originValue) {
        StringBuilder likeValue = getParameterBuilder(dataId);
        likeIndex2Status.put(dataId, new boolean[3]);
        if (originValue.startsWith("%")) {
            likeValue.setCharAt(0, '%');
            originValue = originValue.substring(1);
            likeIndex2Status.get(dataId)[0] = true;
        }
        if (originValue.endsWith("%")) {
            likeValue.setCharAt(likeValue.length() - 1, '%');
            originValue = originValue.substring(0, originValue.length() - 1);
            likeIndex2Status.get(dataId)[1] = true;
        }
        if (originValue.contains("%")) {
            likeValue.setCharAt(likeValue.length() / 2, '%');
            likeIndex2Status.get(dataId)[2] = true;
        }
        return likeValue.toString();
    }

    public Map<Long, boolean[]> getLikeIndex2Status() {
        return likeIndex2Status;
    }

    public void setLikeIndex2Status(Map<Long, boolean[]> likeIndex2Status) {
        this.likeIndex2Status = likeIndex2Status;
    }

    public String transferColumnData2Value(long data) {
        if (likeIndex2Status.containsKey(data)) {
            StringBuilder value = getParameterBuilder(data);
            boolean[] status = likeIndex2Status.get(data);
            if (status[0]) {
                value.setCharAt(0, (char) ((int) (Math.random() * 1000) % 52 + 65));
            }
            if (status[1]) {
                value.setCharAt(value.length() - 1, (char) ((int) (Math.random() * 1000) % 52 + 65));
            }
            if (status[2]) {
                value.setCharAt(value.length() / 2, (char) ((int) (Math.random() * 1000) % 52 + 65));
            }
            return value.toString();
        } else {
            return getParameterValue(data);
        }
    }
}