package com.kuaishou.kcode.check.demo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author KCODE
 * Created on 2020-05-28
 */
public class Utils {

    public static Set<Q1Result> createQ1CheckResult(String filePath) throws IOException {
        Stream<String> checkResultStream = Files.lines(Paths.get(filePath));
        return checkResultStream.map(line -> new Q1Result(line)).collect(Collectors.toSet());
    }

    public static Map<Q2Input, Set<Q2Result>> createQ2Result(String filePath) throws Exception {
        Map<Q2Input, Set<Q2Result>> result = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath)))) {
            Set<Q2Result> q2Results = null;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("**")) {
                    q2Results = result.computeIfAbsent(new Q2Input(line), key -> new HashSet<>());
                    continue;
                }
                q2Results.add(new Q2Result(line));
            }
        } catch (IOException e) {
            throw e;
        }
        return result;
    }
}