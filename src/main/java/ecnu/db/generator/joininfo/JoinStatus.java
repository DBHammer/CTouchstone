package ecnu.db.generator.joininfo;

import java.util.Arrays;

public class JoinStatus implements Comparable<JoinStatus> {

    boolean[] status;
    private final int hashCode;

    private final long[] longStatus;

    public boolean[] status() {
        return status;
    }


    public JoinStatus(boolean[] status) {
        this.status = status;
        hashCode = Arrays.hashCode(status);
        int longSize = status().length / 63;
        if (longSize * 63 < status.length) {
            longSize++;
        }
        longStatus = new long[longSize];
        Arrays.fill(longStatus, 0);
        long start = 1;
        int j = 0;
        for (int i = 0; i < status.length; i++) {
            if (status[i]) {
                longStatus[j] += start;
            }
            start *= 2;
            if ((i + 1) % 63 == 0) {
                j++;
                start = 1;
            }
        }
    }

    @Override
    public String toString() {
        return "JoinStatus{" +
                "status=" + Arrays.toString(status) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinStatus that = (JoinStatus) o;
        for (int i = 0; i < longStatus.length; i++) {
            if (longStatus[i] != that.longStatus[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public int compareTo(JoinStatus o) {
        return this.hashCode() - o.hashCode();
    }
}
