package com.learnreactiveprogramming.imperative;

import java.util.ArrayList;
import java.util.List;


public class ImperativeExample {
    public static void main(String[] args) {
        var nameList = List.of("Alex", "Ben", "Chloe", "Adam", "Adam");
        var newNameList = nameGreaterThanSize(nameList, 3);
        System.out.println("Names: "+ newNameList);
    }

    private static ArrayList<String> nameGreaterThanSize(List<String> nameList, int size) {
        var nameListSize = new ArrayList<String>();
        for (String name : nameList) {
            if (name.length() > size && !nameListSize.contains(name)) {
                nameListSize.add(name);
            }
        }
        return nameListSize;
    }
}
