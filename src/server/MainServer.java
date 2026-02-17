package server;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import slaves.Slave;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import static java.lang.Thread.sleep;

public class MainServer {
    private static final int PORT = 5000;
    private static final int FRAGMENT_SIZE = 1024 * 1024;
    private static final Gson gson = new Gson();
    private static java.util.List<SlaveInfo> slaves = new java.util.ArrayList<>();

    public static void main(String[] args) throws IOException {
        System.out.println("démarrage du serveur sur le port " + PORT);

        FileCatalogue.ensureLogsDir();
        System.out.println("Chargement des slaves ....");
        loadSlaveFromFile();
        System.out.println(slaves.size() + " slave(s) enregistré(s)");

        Thread ping = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10000);
                    sendPing();
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "ping-thread");
        ping.setDaemon(true);
        ping.start();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (true) {
                Socket client = serverSocket.accept();
                new Thread(() -> {
                    try {
                        handle(client);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }, "client-thread").start();
            }
        }
    }

    private static void handle(Socket client) throws IOException {
        InputStream rawIn = client.getInputStream();
        String line = readJsonLine(rawIn);
        if (line == null) { client.close(); return; }

        if (line.contains("REGISTER")) {
            registerSlave(client, line);
        } else if (line.startsWith("UPLOAD")) {
            handleClientUpload(client, rawIn);
        } else if (line.equals("LIST")) {
            handleList(client);
        } else if (line.startsWith("DOWNLOAD")) {
            String[] parts = line.split("\\s+", 2);
            String fileId = parts.length >= 2 ? parts[1].trim() : "";
            handleClientDownload(client, fileId);
        } else {
            client.close();
        }
    }
    private static void handleList(Socket client) throws IOException {
        List<FileCatalogue.FileMetadata> files = FileCatalogue.listAllFiles();

        JsonArray array = new JsonArray();
        for (FileCatalogue.FileMetadata f : files) {
            JsonObject obj = new JsonObject();
            obj.addProperty("fileId", f.fileId);
            obj.addProperty("name", f.name);
            obj.addProperty("size", f.size);
            obj.addProperty("fragments", f.fragments);
            obj.addProperty("date", f.date);
            array.add(obj);
        }

        try (DataOutputStream out = new DataOutputStream(client.getOutputStream())) {
            out.writeUTF("OK");
            out.writeInt(array.size());
            out.writeUTF(gson.toJson(array));
            out.flush();
        } catch (IOException e) {
            System.err.println("Erreur envoi LIST response: " + e.getMessage());
        }
    }

    private static void handleClientUpload(Socket client, InputStream rawIn) throws IOException {
        try (DataInputStream in = new DataInputStream(rawIn);
             DataOutputStream out = new DataOutputStream(client.getOutputStream())) {

            String fileName = in.readUTF();
            long fileSize = in.readLong();

            String fileId = UUID.randomUUID().toString();
            int fragmentCount = (int) Math.ceil((double) fileSize / FRAGMENT_SIZE);

            out.writeUTF("READY"); out.flush();

            for (int i = 0; i < fragmentCount; i++) {
                int currSize = (int) Math.min(FRAGMENT_SIZE, fileSize - (i * FRAGMENT_SIZE));
                byte[] fragment = new byte[currSize];
                in.readFully(fragment);

                int slaveId = getBestCapacityFromSlave();
                if (slaveId == -1) { out.writeUTF("ERROR: Aucun slave disponible"); return; }
                SlaveInfo slave = getSlaveById(slaveId);

                sendFragmentToSlave(slave, fileId, i, fragment);

                // enregistre mapping et met à jour capacité
                FileCatalogue.appendFragmentMapping(fileId, i, slave, currSize);
                slave.setCapacity(slave.getCapacity() - currSize);
            }

            // metadata
            FileCatalogue.appendFileMetadata(fileId, fileName, fileSize, fragmentCount);

            out.writeUTF("OK");
            out.writeUTF(fileId);
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void handleClientDownload(Socket client, String fileId) throws IOException {
        try (DataOutputStream out = new DataOutputStream(client.getOutputStream())) {
            FileCatalogue.FileMetadata meta = FileCatalogue.readFileMetadata(fileId);
            if (meta == null) {
                out.writeUTF("ERROR"); out.writeUTF("File non trouve: " + fileId); out.flush(); return;
            }

            List<FileCatalogue.FragmentMapping> mappings = FileCatalogue.readFragmentMappings(fileId);
            if (mappings.size() != meta.fragments) {
                out.writeUTF("ERROR"); out.writeUTF("Fragments manquants"); out.flush(); return;
            }

            out.writeUTF("OK");
            out.writeUTF(meta.name);
            out.writeLong(meta.size);
            out.writeInt(meta.fragments);
            out.flush();

            mappings.sort(Comparator.comparingInt(m -> m.fragmentIndex));
            byte[] buffer = new byte[8192];

            for (FileCatalogue.FragmentMapping m : mappings) {
                try (Socket slaveSocket = new Socket(m.slaveHost, m.slavePort);
                     OutputStream rawOut = slaveSocket.getOutputStream();
                     InputStream rawIn = slaveSocket.getInputStream()) {

                    JsonObject req = new JsonObject();
                    req.addProperty("type", "GET_FRAGMENT");
                    JsonObject data = new JsonObject();
                    data.addProperty("file_id", fileId);
                    data.addProperty("fragment_id", m.fragmentIndex);
                    req.add("data", data);
                    rawOut.write((gson.toJson(req) + "\n").getBytes(StandardCharsets.UTF_8));
                    rawOut.flush();

                    String respLine = readJsonLine(rawIn);
                    if (respLine == null) throw new IOException("Slave non reponse");
                    JsonObject resp = gson.fromJson(respLine, JsonObject.class);
                    long length = resp.getAsJsonObject("data").get("length").getAsLong();

                    out.writeInt((int) length);
                    long remaining = length;
                    while (remaining > 0) {
                        int toRead = (int) Math.min(buffer.length, remaining);
                        int r = rawIn.read(buffer, 0, toRead);
                        if (r == -1) throw new IOException("EOF slave");
                        out.write(buffer, 0, r);
                        remaining -= r;
                    }
                    out.flush();
                } catch (Exception e) {
                    System.err.println("Erreur fragment " + m.fragmentIndex + ": " + e.getMessage());
                    out.writeInt(-1); out.flush();
                    return;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String readJsonLine(InputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') break;
            baos.write(b);
            if (baos.size() > 10_000) throw new IOException("Ligne JSON trop longue");
        }
        if (baos.size() == 0 && b == -1) return null;
        return baos.toString(StandardCharsets.UTF_8.name()).trim();
    }

    private static void sendPing() {
        for (SlaveInfo s : slaves) {
            try (Socket socket = new Socket(s.getHost(), s.getPort());
                 OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {
                JsonObject msg = new JsonObject();
                msg.addProperty("type", "PING");
                out.write((gson.toJson(msg) + "\n").getBytes(StandardCharsets.UTF_8));
                out.flush();

                String resp = readJsonLine(in);
                if (resp == null) { s.setActif(false); continue; }
                JsonObject pong = gson.fromJson(resp, JsonObject.class);
                s.setActif("PONG".equals(pong.get("type").getAsString()));
            } catch (IOException e) {
                s.setActif(false);
            }
        }
    }

    private static void sendFragmentToSlave(SlaveInfo slave, String fileId, int fragmentIndex, byte[] fragmentData) throws IOException {
        try (Socket slaveSocket = new Socket(slave.getHost(), slave.getPort());
             OutputStream rawOut = slaveSocket.getOutputStream();
             InputStream rawIn = slaveSocket.getInputStream()) {

            JsonObject msg = new JsonObject();
            msg.addProperty("type", "STORE_FRAGMENT");
            JsonObject data = new JsonObject();
            data.addProperty("file_id", fileId);
            data.addProperty("fragment_id", fragmentIndex);
            data.addProperty("length", fragmentData.length);
            msg.add("data", data);

            rawOut.write((gson.toJson(msg) + "\n").getBytes(StandardCharsets.UTF_8));
            rawOut.flush();
            rawOut.write(fragmentData);
            rawOut.flush();

            String ackLine = readJsonLine(rawIn);
            if (ackLine == null) throw new IOException("Slave ne répond pas");
            JsonObject ackMsg = gson.fromJson(ackLine, JsonObject.class);
            String status = ackMsg.getAsJsonObject("data").get("status").getAsString();
            if (!"OK".equals(status)) throw new IOException("Slave a refusé: " + status);
        }
    }

    public static void registerSlave(Socket client, String line) throws IOException {
        try {
            JsonObject msg = gson.fromJson(line, JsonObject.class);
            JsonObject data = msg.getAsJsonObject("data");
            int id = Integer.parseInt(data.get("slave_id").getAsString());
            long capacity = data.get("capacity").getAsLong();
            int port = data.get("port").getAsInt();
            String host_str = client.getInetAddress().getHostAddress();

            SlaveInfo slaveInfo = new SlaveInfo(id, capacity, port, host_str);
            for (SlaveInfo s : slaves) if (s.getId() == id) return;
            slaves.add(slaveInfo);
            writeSlaveToFile(slaveInfo);
        } catch (Exception e) {
            System.err.println(" Erreur parsing REGISTER: " + e.getMessage());
        }
    }

    public static void writeSlaveToFile(SlaveInfo slaveInfo) throws IOException {
        String path = FileCatalogue.SLAVES_FILE;
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path, true))) {
            String line = slaveInfo.getId() + "," + slaveInfo.getCapacity() + "," + slaveInfo.getPort() + "," + slaveInfo.getHost();
            bw.write(line); bw.newLine();
        }
    }

    public static void loadSlaveFromFile() throws IOException {
        String path = FileCatalogue.SLAVES_FILE;
        try (BufferedReader bf = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = bf.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split(",");
                int id = Integer.parseInt(parts[0]);
                long capacity = Long.parseLong(parts[1]);
                int port = Integer.parseInt(parts[2]);
                String str_ip = parts[3];
                SlaveInfo slaveInfo = new SlaveInfo(id, capacity, port, str_ip);
                slaves.add(slaveInfo);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static int getBestCapacityFromSlave() {
        if (slaves.isEmpty()) return -1;
        SlaveInfo best = slaves.get(0);
        for (SlaveInfo s : slaves) if (s.getCapacity() > best.getCapacity()) best = s;
        return best.getId();
    }

    public static SlaveInfo getSlaveById(int id) {
        for (SlaveInfo s : slaves) if (s.getId() == id) return s;
        return null;
    }
}