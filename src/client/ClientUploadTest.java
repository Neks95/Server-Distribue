package client;

import java.io.File;

public class ClientUploadTest {
    public static void main(String[] args) throws Exception {
        ClientSocket client = new ClientSocket("localhost", 5000);
        File f = new File("testfile.dat"); // assurez-vous qu'il existe
        String fileId = client.uploadFile(f, (sent, total) -> {
            System.out.printf("Progress: %d/%d bytes\r", sent, total);
        });
        System.out.println("\nFileId renvoye: " + fileId);
    }
}