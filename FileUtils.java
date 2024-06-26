package com.camel.firstCamelProgram.Utils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class FileUtils {

    /**
     * Creates a File if the file does not exist, or returns a
     * reference to the File if it already exists.
     */
    public static File createOrRetrieve(final String target) throws IOException {
        final File answer;
        Path path = Paths.get(target);
        Path parent = path.getParent();
        if (parent != null && Files.notExists(parent)) {
            Files.createDirectories(path);
        }
        if (Files.notExists(path)) {
            System.out.println("Target file \"" + target + "\" will be created.");
            answer = Files.createFile(path).toFile();
        } else {
            System.out.println("Target file \"" + target + "\" will be retrieved.");
            answer = path.toFile();
        }
        return answer;
    }

    public static void writeToFile(File file, Object data) throws IOException {
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(String.valueOf(data));
        bw.close();
    }

    public static String readFile(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        return br.readLine();
    }

    public static void readFile(File file, int batchSize, File offsetFile) throws IOException {
        AtomicInteger offset = new AtomicInteger();
        if(!Objects.isNull(readFile(offsetFile)))
            offset.set(Integer.parseInt(readFile(offsetFile)));
        Stream<String> lines = Files.lines(file.toPath()).skip(offset.get());
        if(Files.lines(file.toPath()).skip(offset.get()).count()==0){
            System.out.println("No records to read in "+file.toPath().toString());
        }
        Stream<List<String>> chunked = fetchBatch(lines, batchSize,new AtomicInteger(0));
        chunked.forEach(chunk -> {
            System.out.println("Chunk Execution Started: " );
            chunk.forEach(record -> {
/*                if(record.equals("line5")){
                    throw new RuntimeException();
                }*/
                System.out.println(record);
                offset.getAndIncrement();
                try {
                    writeToFile(offsetFile, offset);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

        });
    }

    private static <T> Stream<List<String>> fetchBatch(Stream<String> stream, int batchSize, AtomicInteger offset) throws IOException {
        AtomicInteger index = new AtomicInteger(0);
        return stream.collect(Collectors.groupingBy(x -> index.getAndIncrement() / batchSize))
                .entrySet().stream()
                .map(Map.Entry::getValue);

    }

}
