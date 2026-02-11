package slaves;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class Slave {

    private final String slaveId;
    private final String masterHost;
    private final int masterPort;
    private volatile boolean running = true;

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    private final Path storageDir;
    private final Set<String> fragmentIndex = ConcurrentHashMap.newKeySet();

    private final Gson gson = new Gson();

    public Slave(String slaveId, String masterHost, int masterPort, String storageDirPath) throws IOException {
        this.slaveId = slaveId;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.storageDir = Paths.get(storageDirPath == null ? "./slave-storage" : storageDirPath);
        Files.createDirectories(this.storageDir);
        loadIndexFromStorage();
    }

    public Slave(String slaveId, String masterHost, int masterPort) throws IOException {
        this(slaveId, masterHost, masterPort, "./slave-storage");
    }

    private void loadIndexFromStorage() throws IOException {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(storageDir, "*_fragment_*.dat")) {
            for (Path p : ds) fragmentIndex.add(p.getFileName().toString());
        }
    }

    public void start() {
        Thread t = new Thread(this::connectWithRetry, "slave-connect-thread");
        t.setDaemon(false);
        t.start();
    }

    private void connectWithRetry() {
        while (running) {
            try {
                System.out.println("Tentative de connexion au " + masterHost + ":" + masterPort + " ...");
                socket = new Socket(masterHost, masterPort);
                out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                System.out.println("Connecté au " + masterHost + ":" + masterPort);
                sendRegister();
                listenLoop(); 
            } catch (Exception e) {
                System.err.println("Connexion échouée: " + e.getMessage());
            } finally {
                closeSocket();
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void sendRegister() {
        JsonObject data = new JsonObject();
        data.addProperty("slave_id", slaveId);
        data.addProperty("capacity", getFreeSpace());
        sendMessage("REGISTER", data);
        System.out.println("REGISTER envoyé");
    }

    private void listenLoop() {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                JsonObject msg;
                try {
                    msg = gson.fromJson(line, JsonObject.class);
                } catch (Exception ex) {
                    System.err.println("JSON non valide reçu : " + line);
                    continue;
                }
                String type = msg.has("type") ? msg.get("type").getAsString() : "";
                JsonObject data = msg.has("data") ? msg.getAsJsonObject("data") : null;
                System.out.println("Reçu du Master : " + type);
                switch (type) {
                    case "PING":
                        sendMessage("PONG", createSimpleData("slave_id", slaveId));
                        break;
                    case "STORE_FRAGMENT":
                        if (data != null) handleStoreFragment(data);
                        break;
                    default:
                        System.out.println("Type non géré pour l'instant: " + type);
                }
            }
            System.out.println("Lecture terminée (Master fermé la connexion)");
        } catch (IOException e) {
            System.err.println("Erreur d'écoute: " + e.getMessage());
        }
    }

    private void handleStoreFragment(JsonObject data) {
        String fileId = null;
        int fragmentId = -1;
        try {
            fileId = data.get("file_id").getAsString();
            fragmentId = data.get("fragment_id").getAsInt();
            String b64 = data.get("data").getAsString();

            byte[] bytes = Base64.getDecoder().decode(b64);

            long maxBytes = 50L * 1024L * 1024L;
            if (bytes.length > maxBytes) {
                sendAck(fragmentId, "ERROR: fragment too large (" + bytes.length + " bytes)");
                System.err.println("Refusé fragment trop gros: " + bytes.length);
                return;
            }

            String filename = fileId + "_fragment_" + fragmentId + ".dat";
            Path tmp = storageDir.resolve(filename + ".tmp");
            Path dest = storageDir.resolve(filename);

            Files.write(tmp, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmp, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

            fragmentIndex.add(dest.getFileName().toString());
            sendAck(fragmentId, "OK");
            System.out.println("Fragment stocké: " + dest.toString() + " (" + bytes.length + " bytes)");
        } catch (Exception e) {
            System.err.println("Erreur stockage fragment (fileId=" + fileId + ", fragmentId=" + fragmentId + "): " + e.getMessage());
            sendAck(fragmentId, "ERROR: " + e.getMessage());
        }
    }

    private void sendAck(int fragmentId, String status) {
        JsonObject data = new JsonObject();
        data.addProperty("fragment_id", fragmentId);
        data.addProperty("status", status);
        sendMessage("ACK", data);
    }

    private synchronized void sendMessage(String type, JsonObject dataObj) {
        if (out == null) return;
        JsonObject msg = new JsonObject();
        msg.addProperty("type", type);
        msg.add("data", dataObj);
        out.println(gson.toJson(msg));
        out.flush();
    }

    private JsonObject createSimpleData(String key, String value) {
        JsonObject d = new JsonObject();
        d.addProperty(key, value);
        return d;
    }

    private long getFreeSpace() {
        try {
            return Files.getFileStore(storageDir).getUsableSpace();
        } catch (IOException e) {
            return -1;
        }
    }

    private synchronized void closeSocket() {
        try {
            if (socket != null && !socket.isClosed()) socket.close();
        } catch (IOException ignored) {
        } finally {
            socket = null;
            out = null;
            in = null;
        }
    }

    public void stop() {
        running = false;
        closeSocket();
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java -cp <classpath> slaves.Slave <slaveId> <masterHost> <masterPort> [storageDir]");
            System.exit(1);
        }
        String slaveId = args[0];
        String masterHost = args[1];
        int masterPort = Integer.parseInt(args[2]);
        String storageDir = args.length >= 4 ? args[3] : "./slave-storage";

        try {
            Slave s = new Slave(slaveId, masterHost, masterPort, storageDir);
            s.start();
            Thread.currentThread().join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}