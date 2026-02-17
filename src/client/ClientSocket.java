

package client;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;


public class ClientSocket {
    private final String masterHost;
    private final int masterPort;
    private static final int BUFFER_SIZE = 1024 * 1024;
    private final Gson gson = new Gson();


    public interface ProgressListener {
        void onProgress(long bytesTransferred, long totalBytes);
    }


    public static class FileInfo {
        private String fileId;
        private String name;
        private long size;
        private int fragments;
        private String date;

        public FileInfo(String fileId, String name, long size, int fragments, String date) {
            this.fileId = fileId;
            this.name = name;
            this.size = size;
            this.fragments = fragments;
            this.date = date;
        }

        public String getFileId() { return fileId; }
        public String getName() { return name; }
        public long getSize() { return size; }
        public int getFragments() { return fragments; }
        public String getDate() { return date; }

        @Override
        public String toString() {
            return name + " (" + formatSize(size) + ") - " + date;
        }

        private String formatSize(long bytes) {
            if (bytes < 1024) return bytes + " B";
            if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
            if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
            return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
        }
    }

    public ClientSocket(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    // ======================== UPLOAD ========================


    public String uploadFile(File file, ProgressListener listener) throws IOException {
        if (!file.exists() || !file.isFile()) {
            throw new FileNotFoundException("Fichier introuvable: " + file.getAbsolutePath());
        }

        try (Socket socket = new Socket(masterHost, masterPort)) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            // 1. Envoyer la commande UPLOAD
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
            pw.println("UPLOAD");
            pw.flush();

            // 2. Envoyer le nom et la taille du fichier
            out.writeUTF(file.getName());
            out.writeLong(file.length());
            out.flush();

            // 3. Attendre READY
            String response = in.readUTF();
            if (!"READY".equals(response)) {
                throw new IOException("Serveur non pret: " + response);
            }

            // 4. Lire et envoyer le fichier par fragments de 1MB
            long totalSize = file.length();
            long bytesSent = 0;

            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] buffer = new byte[BUFFER_SIZE];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                    out.flush();
                    bytesSent += bytesRead;

                    if (listener != null) {
                        listener.onProgress(bytesSent, totalSize);
                    }
                }
            }

            // 5. Recevoir confirmation et fileId
            String status = in.readUTF();
            if ("OK".equals(status)) {
                String fileId = in.readUTF();
                System.out.println("Upload reussi! FileId: " + fileId);
                return fileId;
            } else {
                throw new IOException("Erreur upload: " + status);
            }
        }
    }

    /**
     * Upload sans listener de progression.
     */
    public String uploadFile(File file) throws IOException {
        return uploadFile(file, null);
    }

    // ======================== DOWNLOAD ========================

    /**
     * Télécharger un fichier depuis le Master Server.
     *
     * @param fileId   l'identifiant du fichier
     * @param savePath le chemin de sauvegarde local
     * @param listener listener de progression (peut être null)
     * @throws IOException en cas d'erreur réseau
     */
    public void downloadFile(String fileId, String savePath, ProgressListener listener) throws IOException {
        try (Socket socket = new Socket(masterHost, masterPort)) {
            DataInputStream in = new DataInputStream(socket.getInputStream());

            // 1. Envoyer la commande DOWNLOAD
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
            pw.println("DOWNLOAD " + fileId);
            pw.flush();

            // 2. Lire la réponse
            String status = in.readUTF();
            if (!"OK".equals(status)) {
                String errorMsg = in.readUTF();
                throw new IOException("Erreur download: " + errorMsg);
            }

            // 3. Lire les infos du fichier
            String originalName = in.readUTF();
            long totalSize = in.readLong();
            int fragmentCount = in.readInt();

            System.out.println("Download: " + originalName + " (" + totalSize + " bytes, " + fragmentCount + " fragments)");

            // 4. Recevoir et écrire les fragments
            File outputFile = new File(savePath);
            // Créer le dossier parent si nécessaire
            if (outputFile.getParentFile() != null) {
                outputFile.getParentFile().mkdirs();
            }

            long bytesReceived = 0;

            try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                for (int i = 0; i < fragmentCount; i++) {
                    int fragmentSize = in.readInt();

                    if (fragmentSize == -1) {
                        throw new IOException("Erreur lors de la recuperation du fragment #" + i);
                    }

                    byte[] fragmentData = new byte[fragmentSize];
                    in.readFully(fragmentData);
                    fos.write(fragmentData);

                    bytesReceived += fragmentSize;
                    if (listener != null) {
                        listener.onProgress(bytesReceived, totalSize);
                    }

                    System.out.println("Fragment #" + i + " recu (" + fragmentSize + " bytes)");
                }
                fos.flush();
            }

            System.out.println("Download termine! Sauvegarde: " + savePath);
        }
    }

    /**
     * Download sans listener de progression.
     */
    public void downloadFile(String fileId, String savePath) throws IOException {
        downloadFile(fileId, savePath, null);
    }

    // ======================== LIST ========================

    /**
     * Lister tous les fichiers disponibles sur le serveur.
     *
     * @return liste des informations de fichiers
     * @throws IOException en cas d'erreur réseau
     */
    public List<FileInfo> listFiles() throws IOException {
        List<FileInfo> files = new ArrayList<>();

        try (Socket socket = new Socket(masterHost, masterPort)) {
            DataInputStream in = new DataInputStream(socket.getInputStream());

            // Envoyer la commande LIST
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
            pw.println("LIST");
            pw.flush();

            // Lire la réponse
            String status = in.readUTF();
            if (!"OK".equals(status)) {
                throw new IOException("Erreur LIST: " + status);
            }

            int fileCount = in.readInt();
            String jsonList = in.readUTF();

            // Parser le JSON
            JsonArray array = gson.fromJson(jsonList, JsonArray.class);
            for (int i = 0; i < array.size(); i++) {
                JsonObject obj = array.get(i).getAsJsonObject();
                FileInfo info = new FileInfo(
                        obj.get("fileId").getAsString(),
                        obj.get("name").getAsString(),
                        obj.get("size").getAsLong(),
                        obj.get("fragments").getAsInt(),
                        obj.has("date") && !obj.get("date").isJsonNull() ? obj.get("date").getAsString() : "N/A"
                );
                files.add(info);
            }

            System.out.println(fileCount + " fichier(s) disponible(s)");
        }

        return files;
    }

    // ======================== DELETE ========================

    /**
     * Supprimer un fichier sur le serveur.
     *
     * @param fileId l'identifiant du fichier à supprimer
     * @return message de confirmation
     * @throws IOException en cas d'erreur réseau
     */
    public String deleteFile(String fileId) throws IOException {
        try (Socket socket = new Socket(masterHost, masterPort)) {
            DataInputStream in = new DataInputStream(socket.getInputStream());

            // Envoyer la commande DELETE
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
            pw.println("DELETE " + fileId);
            pw.flush();

            String status = in.readUTF();
            String message = in.readUTF();

            if ("OK".equals(status)) {
                System.out.println("Fichier supprime: " + message);
                return message;
            } else {
                throw new IOException("Erreur DELETE: " + message);
            }
        }
    }

    // ======================== STATS ========================

    /**
     * Recuperer les statistiques du systeme.
     *
     * @return JSON string contenant les statistiques
     * @throws IOException en cas d'erreur reseau
     */
    public String getStats() throws IOException {
        try (Socket socket = new Socket(masterHost, masterPort)) {
            DataInputStream in = new DataInputStream(socket.getInputStream());

            PrintWriter pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
            pw.println("STATS");
            pw.flush();

            String status = in.readUTF();
            if (!"OK".equals(status)) {
                throw new IOException("Erreur STATS: " + status);
            }

            return in.readUTF();
        }
    }


    public boolean testConnection() {
        try (Socket socket = new Socket(masterHost, masterPort)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public String getMasterHost() {
        return masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }
}

