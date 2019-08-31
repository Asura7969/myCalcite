package com.myCalcite;

import java.util.LinkedHashMap;
import java.util.Map;

public class MapTest {

    public static void main(String[] args) {

        LinkedHashMap<String,String> map = new LinkedHashMap<>();
        map.put("1","1");
        map.put("2","1");
        map.put("3","1");
        map.put("4","1");
        map.put("5","1");
        map.put("6","1");

        for (Map.Entry<String, String> entry : map.entrySet()) {
            //System.out.println(entry.getKey());
        }
        for (String s : map.keySet()) {
            System.out.println(s);
        }


    }
}
