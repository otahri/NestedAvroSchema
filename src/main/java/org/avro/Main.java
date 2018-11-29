package org.avro;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        //Absolute path of the input Directory
        String inputDir = "/inputSchemas";
        //Absolute path of the output Directory
        String outputDir = "/OutputSchemas";
        String schemaName = "kafka101.mydev.Main";
        String fileName = "output.avsc";

        NestedSchemas nestedSchemas = NestedSchemas.getInstance();

        nestedSchemas.load(inputDir);
        nestedSchemas.compile();
        nestedSchemas.save(outputDir, schemaName, fileName);
    }
}
