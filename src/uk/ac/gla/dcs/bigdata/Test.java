package uk.ac.gla.dcs.bigdata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Test {
    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        list.add("hello");
        list.add("world");
        list.add("hello");

        Set<String> set = new HashSet<>(list);

        for (String str: set){
            System.out.println(str);
        }
    }
}
