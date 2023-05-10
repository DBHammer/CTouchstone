package ecnu.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class getTest {

    public static void main(String[] args) {
        List<Integer> a = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            a.add(i);
        }
        int[] b = a.stream().mapToInt(Integer::intValue).toArray();
        System.out.println(Arrays.toString(b));
    }
}
