package slaves;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Slave {

    private final String slaveId;
    private final String masterHost;
    private final int masterPort;
    private final int listenPort;
    private final Path storageDir;
    private final Set<String> fragmentIndex = ConcurrentHashMap.newKeySet();
    private final Gson gson = new Gson();
    private final ExecutorService pool = Executors.newFixedThreadPool(8);

    public Slave(String slaveId, String masterHost, int masterPort, int listenPort, String storageDirPath) throws IOException {
        this.slaveId = slaveId;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.listenPort = listenPort;
        this.storageDir = Paths.get(storageDirPath == null ? "./slave_storage" : storageDirPath);
        Files.createDirectories(this.storageDir);
        loadIndexFromStorage();
    }

    private void loadIndexFromStorage() throws IOException {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(storageDir, "*_fragment_*.dat")) {
            for (Path p : ds) fragmentIndex.add(p.getFileName().toString());
        }
    }

    public void start() {
        try {
            registerWithMaster();
        } catch (Exception e) {
            System.err.println("Impossible d'enregistrer auprès du master: " + e.getMessage());
        }

        try (ServerSocket serverSocket = new ServerSocket(listenPort)) {
            System.out.println("Slave ecoute sur le port " + listenPort);
            while (true) {
                Socket client = serverSocket.accept();
                pool.submit(() -> handleClient(client));
            }
        } catch (IOException e) {
            throw new RuntimeException("Erreur ServerSocket: " + e.getMessage(), e);
        } finally {
            pool.shutdown();
        }
    }

    private void registerWithMaster() {
        try (Socket s = new Socket(masterHost, masterPort);
             PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {
            JsonObject data = new JsonObject();
            data.addProperty("slave_id", slaveId);
            data.addProperty("capacity", getFreeSpace());
            data.addProperty("port", listenPort);

            JsonObject msg = new JsonObject();
            msg.addProperty("type", "REGISTER");
            msg.add("data", data);
            out.println(gson.toJson(msg));
            out.flush();
            System.out.println("REGISTER envoye au master: " + msg.toString());
        } catch (Exception e) {
            System.err.println("registerWithMaster échoué: " + e.getMessage());
        }
    }

    private void handleClient(Socket socket) {
        String remote = socket.getRemoteSocketAddress().toString();
        System.out.println("Connexion entrante de " + remote);
        try (InputStream in = socket.getInputStream();
             OutputStream out = socket.getOutputStream()) {

            String line = readJsonLine(in);
            if (line == null) {
                System.err.println("Aucune donnée JSON reçue de " + remote);
                return;
            }

            JsonObject msg;
            try {
                msg = gson.fromJson(line, JsonObject.class);
            } catch (JsonSyntaxException ex) {
                sendJson(out, error("invalid_json", "JSON non valide"));
                return;
            }

            String type = msg.has("type") ? msg.get("type").getAsString() : "";
            JsonObject data = msg.has("data") ? msg.getAsJsonObject("data") : null;

            switch (type) {
                case "STORE_FRAGMENT":
                    handleStoreFragment(in, out, data);
                    break;
                case "GET_FRAGMENT":
                    handleGetFragment(out, data);
                    break;
                case "PING":
                    System.out.println("PING RECU");
                    sendJson(out, pong());
                    break;
                default:
                    sendJson(out, error("unsupported", "Type non supporté: " + type));
            }
        } catch (Exception e) {
            System.err.println("Erreur sur connexion " + remote + " : " + e.getMessage());
        }
    }

    private String readJsonLine(InputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') break;
            baos.write(b);
            if (baos.size() > 10_000) { // 10KB header limit
                throw new IOException("Ligne JSON trop longue");
            }
        }
        if (baos.size() == 0 && b == -1) return null;
        return baos.toString(StandardCharsets.UTF_8.name()).trim();
    }

    private void handleStoreFragment(InputStream in, OutputStream out, JsonObject data) throws IOException {
        if (data == null || !data.has("file_id") || !data.has("fragment_id") || !data.has("length")) {
            sendJson(out, ackError(-1, "missing fields"));
            return;
        }
        String fileId = sanitizeFileId(data.get("file_id").getAsString());
        int fragmentId = data.get("fragment_id").getAsInt();
        long length = data.get("length").getAsLong();

        if (length < 0 || length > 500L * 1024L * 1024L) { // limit 500MB example
            sendJson(out, ackError(fragmentId, "invalid length"));
            return;
        }

        String filename = fileId + "_fragment_" + fragmentId + ".dat";
        Path tmp = storageDir.resolve(filename + ".tmp");
        Path dest = storageDir.resolve(filename);

        try (OutputStream fos = Files.newOutputStream(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            long remaining = length;
            byte[] buffer = new byte[8192];
            while (remaining > 0) {
                int toRead = (int) Math.min(buffer.length, remaining);
                int read = in.read(buffer, 0, toRead);
                if (read == -1) throw new EOFException("Flux terminé prématurément");
                fos.write(buffer, 0, read);
                remaining -= read;
            }
            fos.flush();
        } catch (IOException e) {
            try { Files.deleteIfExists(tmp); } catch (IOException ignored) {}
            sendJson(out, ackError(fragmentId, "io_error: " + e.getMessage()));
            return;
        }

        try {
            Files.move(tmp, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            fragmentIndex.add(dest.getFileName().toString());
            sendJson(out, ackOk(fragmentId));
            System.out.println("Fragment stocké: " + dest + " (" + length + " bytes)");
        } catch (IOException e) {
            sendJson(out, ackError(fragmentId, "move_error: " + e.getMessage()));
        }
    }

    private void handleGetFragment(OutputStream out, JsonObject data) throws IOException {
        if (data == null || !data.has("file_id") || !data.has("fragment_id")) {
            sendJson(out, error("missing_fields", "file_id or fragment_id missing"));
            return;
        }
        String fileId = sanitizeFileId(data.get("file_id").getAsString());
        int fragmentId = data.get("fragment_id").getAsInt();
        String filename = fileId + "_fragment_" + fragmentId + ".dat";
        Path f = storageDir.resolve(filename);
        if (!Files.exists(f)) {
            sendJson(out, error("not_found", "fragment not found"));
            return;
        }

        long length = Files.size(f);
        JsonObject resp = new JsonObject();
        resp.addProperty("file_id", fileId);
        resp.addProperty("fragment_id", fragmentId);
        resp.addProperty("length", length);

        sendJsonRaw(out, "GET_FRAGMENT_RESPONSE", resp);

        try (InputStream fis = Files.newInputStream(f, StandardOpenOption.READ)) {
            byte[] buffer = new byte[8192];
            int r;
            while ((r = fis.read(buffer)) != -1) {
                out.write(buffer, 0, r);
            }
            out.flush();
        }
        System.out.println("Fragment envoyé: " + f + " (" + length + " bytes)");
    }

    private void sendJsonRaw(OutputStream out, String type, JsonObject data) throws IOException {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", type);
        msg.add("data", data);
        sendJsonRaw(out, msg);
    }

    private void sendJsonRaw(OutputStream out, JsonObject msg) throws IOException {
        byte[] bytes = (gson.toJson(msg) + "\n").getBytes(StandardCharsets.UTF_8);
        out.write(bytes);
        out.flush();
    }

    private void sendJson(OutputStream out, JsonObject msg) throws IOException {
        System.out.println("PONG ENVOYE");
        sendJsonRaw(out, msg);
    }

    private JsonObject ackOk(int fragmentId) {
        JsonObject data = new JsonObject();
        data.addProperty("fragment_id", fragmentId);
        data.addProperty("status", "OK");

        JsonObject msg = new JsonObject();
        msg.addProperty("type", "ACK");
        msg.add("data", data);

        return msg;
    }

    private JsonObject ackError(int fragmentId, String message) {
        JsonObject data = new JsonObject();
        data.addProperty("fragment_id", fragmentId);
        data.addProperty("status", "ERROR: " + message);

        JsonObject msg = new JsonObject();
        msg.addProperty("type", "ACK");
        msg.add("data", data);

        return msg;
    }


    private JsonObject pong() {
        JsonObject d = new JsonObject();
        d.addProperty("slave_id", slaveId);
        JsonObject m = new JsonObject();
        m.addProperty("type", "PONG");
        m.add("data", d);
        return m;
    }

    private JsonObject error(String code, String message) {
        JsonObject d = new JsonObject();
        d.addProperty("code", code);
        d.addProperty("message", message);
        JsonObject m = new JsonObject();
        m.addProperty("type", "ERROR");
        m.add("data", d);
        return m;
    }

    private void sendAckWrapper(OutputStream out, int fragmentId, String status) throws IOException {
        JsonObject d = new JsonObject();
        d.addProperty("fragment_id", fragmentId);
        d.addProperty("status", status);
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "ACK");
        msg.add("data", d);
        sendJson(out, msg);
    }

    private String sanitizeFileId(String fileId) {
        return fileId.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private long getFreeSpace() {
        try {
            return Files.getFileStore(storageDir).getUsableSpace();
        } catch (IOException e) {
            return -1;
        }
    }

}