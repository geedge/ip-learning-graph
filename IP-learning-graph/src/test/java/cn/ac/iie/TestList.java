package cn.ac.iie;

import java.util.ArrayList;
import java.util.Collections;

public class TestList {
    public static void main(String[] args) {
        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(10);
        integers.add(8);
        integers.add(11);
        integers.add(5);
        integers.add(5);
        integers.add(4);
        integers.add(4);
        integers.add(12);
        Collections.sort(integers);
        System.out.println(integers);
        integers.add(5);
        Collections.sort(integers);
        System.out.println(integers);
        System.out.println(integers.indexOf(5));
    }
}
