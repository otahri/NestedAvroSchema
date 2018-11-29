package org.avro;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        String inputDir = "/inputSchemas";
        String outputDir = "/OutputSchemas";
        String schemaName = "kafka101.mydev.Main";
        String fileName = "output.avsc";

        NestedSchemas nestedSchemas = NestedSchemas.getInstance();

        nestedSchemas.load(inputDir);
        nestedSchemas.compile();
        nestedSchemas.save(outputDir, schemaName, fileName);
    }
}
