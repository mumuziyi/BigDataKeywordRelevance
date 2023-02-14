package uk.ac.gla.dcs.bigdata;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        List<String> str = new ArrayList<>();
        str.add("hello");
        str.add("world");

        System.out.println(str.toString());
        String str2 = str.toString();
        System.out.println(str2.length());
    }
}
