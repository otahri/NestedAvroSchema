package org.avro;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class NestedSchemas {

    private Map<String, Schema> schemas = new HashMap<>();

    private Queue<String> waitingQueue = new LinkedList<>();

    private static NestedSchemas nestedSchemas = new NestedSchemas();

    private NestedSchemas() {
    }

    public static NestedSchemas getInstance() {
        return nestedSchemas;
    }

    public void load(String inputDirectory) throws IOException {

        Files
                .list(Paths.get(inputDirectory))
                .filter(Files::isRegularFile)
                .map(Path::toAbsolutePath)
                .map(Path::toFile)
                .map(this::convertFileToString)
                .forEach(jsonFile -> waitingQueue.add(jsonFile));

    }


    public void compile() {

        while (waitingQueue.size() != 0) {
            String item = waitingQueue.poll();
            try {
                String completeSchema = compileSchema(item);
                Schema schema = new Schema.Parser().parse(completeSchema);
                String name = schema.getFullName();
                schemas.put(name, schema);
            } catch (RuntimeException e) {
                waitingQueue.offer(item);
            }
        }
    }


    public void save(String outputDirectory, String schemaName, String fileName) throws IOException {

        try (FileWriter file = new FileWriter(outputDirectory + "/" + fileName)) {
            file.write(schemas.get(schemaName).toString(true));
        }

    }


    private String convertFileToString (File file) {
        String str = null;
        try {
            str = FileUtils.readFileToString(file, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return str;
    }

    private String compileSchema(String schema) {

        for (Map.Entry<String, Schema> entry : schemas.entrySet()) {
            schema = schema.replaceAll("\"" + entry.getKey() + "\"", entry.getValue().toString());
        }
        return schema;
    }


}
