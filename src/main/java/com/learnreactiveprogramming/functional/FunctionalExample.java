package com.learnreactiveprogramming.functional;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FunctionalExample {

    public static void main(String[] args) {
        var nameList = List.of("Alex", "Ben", "Chloe", "Adam", "Adam");
        var newNameList = nameGreaterThanSize(nameList, 3);
        System.out.println("Names: "+ newNameList);
    }

    private static List<String> nameGreaterThanSize(List<String> nameList, int size) {
        return nameList.stream()
                .filter(name -> name.length() > size)
                .distinct()
                .map(String::toUpperCase)
                .collect(Collectors.toList());
    }
}
