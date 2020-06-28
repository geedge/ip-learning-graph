package cn.ac.iie;

import java.util.Arrays;

public class ArrayTest {
    public static void main(String[] args) {
        long[] longs = {1, 2, 3, 4, 5, 6, 7};
        long[] longs1 = new long[7];
        System.arraycopy(longs, 0, longs1, 1, longs.length - 1);
        longs1[0] = 8;
        System.out.println(Arrays.toString(longs1));
    }
}
