package ecnu.db.schema;

import java.util.HashMap;
import java.util.Random;

class StringTemplate {
    int minLength;
    int rangeLength;

    HashMap<Long, boolean[]> likeIndex2Status = new HashMap<>();

    public String getParameterValue(long specialValue, long dataId) {
        return getParameterBuilder(specialValue, dataId).toString();
    }

    private StringBuilder getParameterBuilder(long specialValue, long dataId) {
        Random random = new Random(specialValue * dataId);
        return random.ints(65, 123)
                .limit(minLength + random.nextInt(rangeLength + 1)).
                        collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append);
    }

    public String getLikeValue(long specialValue, long dataId, String originValue) {
        StringBuilder likeValue = getParameterBuilder(specialValue, dataId);
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

    public int getMinLength() {
        return minLength;
    }

    public void setMinLength(int minLength) {
        this.minLength = minLength;
    }

    public int getRangeLength() {
        return rangeLength;
    }

    public void setRangeLength(int rangeLength) {
        this.rangeLength = rangeLength;
    }

    public HashMap<Long, boolean[]> getLikeIndex2Status() {
        return likeIndex2Status;
    }

    public void setLikeIndex2Status(HashMap<Long, boolean[]> likeIndex2Status) {
        this.likeIndex2Status = likeIndex2Status;
    }

    public String transferColumnData2Value(long specialValue, long data) {
        if (likeIndex2Status.containsKey(data)) {
            StringBuilder value = getParameterBuilder(specialValue, data);
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
            return getParameterValue(specialValue, data);
        }
    }
}