import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Scanner;

public class Main {
    private static final String url = "";
    private static final String user = "";
    private static final String pass = "";

    public static void main(String[] args) {

        Scanner input = new Scanner(System.in);
        System.out.println("Enter Query:");
        String query = input.nextLine();
        System.out.println("Enter filename:");
        String filename = input.nextLine();
        System.out.println("Enter output file type(csv/sql):");
        String fileType = input.nextLine();
        System.out.println("Enter Query Table(leave empty for csv):");
        String tableName = input.nextLine();

        long start = System.currentTimeMillis();
        File f = new File(filename + "." + fileType);
        if (f.exists()) {
            f.delete();
        }

        extractData(tableName, query, filename, fileType);
        System.out.printf("File generated! Time elapsed: %dms",
                (System.currentTimeMillis() - start));
        input.close();
    }

    public static void writeOutputToFile(String output, String filename) {
        try (PrintWriter printWriter = new PrintWriter(filename)) {
            printWriter.println(output);
        } catch (FileNotFoundException ex) {
            System.err.println(ex.getMessage());
        }
    }

    public static void extractData(String tableName, String query, String fileName, String fileType) {
        StringBuilder output = new StringBuilder();
        String joiner = ",";
        Scanner scanner = new Scanner(System.in);

        try (Connection connection = DriverManager.getConnection(url, user, pass)) {
            Statement statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = statement.executeQuery(query);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            boolean isSql = false;

            if (fileType.equals("csv")) {
                for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                    output.append(rsMetaData.getColumnName(i)).append(joiner);
                }
                output.append(System.lineSeparator());
            } else {
                isSql = true;
            }

            String clobFileExtension = null;
            String blobFileExtension = null;
            long rowLimiter = -1;
            rs.last();
            if (rs.getRow() > 3000) {
                System.out.println("Query contains" +rs.getRow() + "rows!!" + System.lineSeparator() +
                        "Do you wish to limit the output ? (enter number of rows as limiter, -1 for no limiter)");
                rowLimiter = scanner.nextInt();
            }
            rs.first();
            while (rs.next() && rs.getRow() != rowLimiter) {
                if (isSql) {
                    output.append("INSERT INTO ").append(tableName).append(" VALUES (");
                }
                for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                    if (rsMetaData.getColumnClassName(i).contains("Clob")) {
                        if (clobFileExtension == null) {
                            System.out.println("The table contains CLOB files. CLOB File extension ? :");
                            clobFileExtension = new Scanner(System.in).nextLine();
                            clobFileExtension = clobFileExtension.trim().isEmpty() ? "txt": clobFileExtension;
                        }
                        String directoryName = "clob_objs_for_" + fileName;
                        if (!Files.exists(Paths.get(directoryName))) {
                            new File(directoryName).mkdir();
                        }
                        writeOutputToFile(rs.getString(rsMetaData.getColumnName(i)),
                                directoryName + "/clobobj" + rs.getRow() + "." + clobFileExtension);
                        output.append(directoryName).append("/clobobj").append(rs.getRow())
                                .append(".").append(clobFileExtension).append(",");
                        continue;
                    }

                    if (rsMetaData.getColumnClassName(i).contains("Blob")) {
                        if (blobFileExtension == null) {
                            System.out.println("The table contains BLOB files. BLOB File extension ? :");
                            blobFileExtension = new Scanner(System.in).nextLine();
                            blobFileExtension = blobFileExtension.trim().isEmpty() ? "jpg": blobFileExtension;
                        }
                        String directoryName = "blob_objs_for_" + fileName;
                        if (!Files.exists(Paths.get(directoryName))) {
                            new File(directoryName).mkdir();
                        }
                        writeOutputToFile(rs.getString(rsMetaData.getColumnName(i)),
                                directoryName + "/blobobj" + rs.getRow() + ".jpg");
                        output.append(directoryName).append("/blobobj").append(rs.getRow()).append(".jpg,");
                        continue;
                    }

                    output.append(formatData(rsMetaData.getColumnClassName(i),
                            rs.getString(rsMetaData.getColumnName(i)), isSql));
                }
                output.deleteCharAt(output.length() - 1);
                if (isSql) {
                    output.append(")");
                }
                output.append(System.lineSeparator());
            }
            statement.close();
            rs.close();
        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
        }
        writeOutputToFile(output.toString(), fileName + "." + fileType);
    }

    public static String formatData(String columnType, String data, boolean isSql) {
        StringBuilder output = new StringBuilder();
        if (data == null) {
            return isSql ? "NULL," : ",";
        }
        if (columnType.contains("String")) {
            output.append("'").append(data).append("'");
        } else if (isSql && columnType.contains("Timestamp")) {
            output.append("TO_DATE('").append(data, 0, 10).append("', 'yyyy-mm-dd')");
        } else {
            output.append(data);
        }
        output.append(",");
        return output.toString();
    }
}
