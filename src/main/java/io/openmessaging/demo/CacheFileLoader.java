package io.openmessaging.demo;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author Yuchen Chiang
 */
public class CacheFileLoader {

    public static Map<String, List<String>> load(String str) throws IOException{

        Map<String, List<String>> map = new HashMap<>();

        DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(str));
        for (Path p : directoryStream) {
            if (!Files.isDirectory(p)) {
                continue;
            }
            List<String> list = new ArrayList<>();
            map.put(p.getFileName().toString(), list);
            DirectoryStream<Path> pathDirectoryStream = Files.newDirectoryStream(p);
            for (Path pp : pathDirectoryStream) {
                list.add(pp.toString());
            }
            pathDirectoryStream.close();
            //to avoid file sequence error
            Collections.sort(list);
        }
        directoryStream.close();

        return map;
    }
}
