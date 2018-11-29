package org.avro;

import org.apache.avro.Schema;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class NestedSchemas {

    private Map<String, Schema> schemas = new HashMap<>();

    private Queue<String> waitingQueue = new LinkedList<>();

    private static NestedSchemas nestedSchemas = new NestedSchemas();

    private NestedSchemas(){}

    public static NestedSchemas getInstance(){
        return nestedSchemas;
    }

    public void load(String inputDirectory) throws IOException {

        List<String> files = IOUtils.readLines(this.getClass().getResourceAsStream(inputDirectory), Charsets.UTF_8);

        files.stream().forEach(f -> {

            String filePath = inputDirectory + "/" + f;

            String json = fileToString(filePath);

            waitingQueue.add(json);

        });
    }


    public void compile() {

        while (waitingQueue.size() != 0) {
            String item = waitingQueue.poll();
            try {
                String completeSchema = resolveSchema(item);
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


    private String fileToString(String filePath) {

        try (InputStream inputStream = this.getClass().getResourceAsStream(filePath)) {
            return IOUtils.toString(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String resolveSchema(String schema) {

        for (Map.Entry<String, Schema> entry : schemas.entrySet()) {
            schema = schema.replaceAll("\"" + entry.getKey() + "\"", entry.getValue().toString());
        }
        return schema;
    }


}
