package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class OutputToFileWriter {

    String fileContent;
    String filePath;

    public OutputToFileWriter(String fileContent, String filePath) {
        this.fileContent = fileContent;
        this.filePath = filePath;
    }

    public boolean writeOutputToFile() {
        deleteExistingFile();
        try (PrintWriter printWriter = new PrintWriter(filePath)) {
            printWriter.println(fileContent);
            return true;
        } catch (FileNotFoundException ex) {
            System.err.println(ex.getMessage());
            return false;
        }
    }

    private void deleteExistingFile() {
        new File(filePath).delete();
    }


}
