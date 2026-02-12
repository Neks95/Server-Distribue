package server;

import client.ClientSocket;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import slaves.Slave;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.lang.Thread.sleep;


public class MainServer {
    private static final int PORT = 5000;
    private static final int fragment_SIZE = 1024 * 1024;
    private static final Gson gson = new Gson();
    private static List<SlaveInfo> slaves = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        System.out.println("demmarage du serveur sur le port " + PORT);
        System.out.println("Chargement des slaves ....");
        loadSlaveFromFile();
        System.out.println(slaves.size() + " slave(s) enregistré(s)");

        Thread listen_ping_pong = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10000);
                    System.out.println("\n Verification des slaves...");
                    sendPing();
                } catch (InterruptedException e) {
                    System.err.println("Heartbeat interrompu");
                    break;
                }
            }
        });
        listen_ping_pong.setName("ping_pong-thread");
        listen_ping_pong.setDaemon(true);
        listen_ping_pong.start();
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (true) {
                Socket client = serverSocket.accept();
                //multithread
                Thread t = new Thread(() -> {
                    try {
                        handle(client);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                t.setName("c-thread");
                t.setDaemon(false);
                t.start();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void sendPing() {
        for(SlaveInfo slave : slaves){
            try (Socket socket = new Socket(slave.getHost(), slave.getPort());
                 OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                JsonObject msg = new JsonObject();
                msg.addProperty("type", "PING");

                String jsonString = gson.toJson(msg) + "\n";
                out.write(jsonString.getBytes(StandardCharsets.UTF_8));
                out.flush();

                System.out.println("PING envoye à Slave #" + slave.getId());

                String response = readJsonLine(in);
                if (response == null) {
                    System.err.println(" Slave #" + slave.getId() + " ne repond pas");
                    slave.setActif(false);
                }

                JsonObject pongMsg = gson.fromJson(response, JsonObject.class);

                if (pongMsg.has("type") && "PONG".equals(pongMsg.get("type").getAsString())) {
                    System.out.println("PONG reçu de Slave #" + slave.getId());
                    slave.setActif(true);
                } else {
                    slave.setActif(false);
                    System.err.println("Reponse invalide du Slave #" + slave.getId());
                }
            } catch (IOException e) {
                slave.setActif(false);
                System.err.println("Slave #" + slave.getId() + " inaccessible: " + e.getMessage());
            }
        }
    }

    private static String readJsonLine(InputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') break;
            baos.write(b);
            if (baos.size() > 10_000) {
                throw new IOException("Ligne JSON trop longue");
            }
        }
        if (baos.size() == 0 && b == -1) return null;
        return baos.toString(StandardCharsets.UTF_8.name()).trim();
    }

    public static void handle(Socket client) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
        String line = in.readLine();
        if (line != null) {
            if (line.contains("REGISTER")) {
                registerSlave(client, line);
            } else {
                handleClient(client);
            }
        }

    }

    public static void handleClient(Socket client) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
        String commandLine = in.readLine();
        if (commandLine != null) {
            if (commandLine.startsWith("UPLOAD")) {
                handleClientUpload(client);
            } else {
                System.out.println("Commande inconnue : " + commandLine);
            }
        }
    }


    public static void handleClientUpload(Socket client) throws IOException {
        System.out.println("\nTraitement UPLOAD");

        try (DataInputStream in = new DataInputStream(client.getInputStream()); DataOutputStream out = new DataOutputStream(client.getOutputStream())) {
            String fileName = in.readUTF();
            long fileSize = in.readLong();

            //otran'servlet
            String fileId = UUID.randomUUID().toString();
            int fragmentCount = (int) Math.ceil((double) fileSize / fragment_SIZE);
            System.out.println(" fragments : " + fragmentCount);

            // Envoyer confirmation READY
            out.writeUTF("READY");
            out.flush();

            System.out.println(" Reception du fichier...");

            for (int i = 0; i < fragmentCount; i++) {
                int currfragmentSize = (int) Math.min(fragment_SIZE, fileSize - (i * fragment_SIZE));
                // Lire le fragment
                byte[] fragmentData = new byte[currfragmentSize];
                in.readFully(fragmentData);

                // Trouver le meilleur slave
                int slaveId = getBestCapacityFromSlave();

                if (slaveId == -1) {
                    System.err.println("\n Aucun slave disponible !");
                    out.writeUTF("ERROR: Aucun slave disponible");
                    return;
                }

                SlaveInfo slave = getSlaveById(slaveId);

                sendfragmentToSlave(slave, fileId, i, fragmentData);

                // Pour l'instant, on simule
                System.out.print(" → Slave #" + slaveId);

                slave.setCapacity(slave.getCapacity() - currfragmentSize);
            }

            System.out.println("\n Fichier uploadé avec succès !");
            System.out.println(" File ID : " + fileId);

            out.writeUTF("OK");
            out.writeUTF(fileId);
            out.flush();

        } catch (Exception e) {
            System.err.println("\n Erreur upload : " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void sendfragmentToSlave(SlaveInfo slave, String fileId,
                                            int fragmentIndex, byte[] fragmentData)
            throws IOException {

        System.out.println("\n Envoi fragment #" + fragmentIndex + " au Slave #" + slave.getId());

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

            String jsonLine = gson.toJson(msg) + "\n";
            rawOut.write(jsonLine.getBytes(StandardCharsets.UTF_8));
            rawOut.flush();

            System.out.println("  Commande envoyee");

            rawOut.write(fragmentData);
            rawOut.flush();

            System.out.println("  ✅ Données envoyées : " + fragmentData.length + " bytes");

            String ackLine = readJsonLine(rawIn);

            if (ackLine == null) {
                throw new IOException("Slave ne répond pas");
            }

            JsonObject ackMsg = gson.fromJson(ackLine, JsonObject.class);

            if (!ackMsg.has("type") || !ackMsg.get("type").getAsString().equals("ACK")) {
                throw new IOException("Réponse invalide: " + ackLine);
            }

            JsonObject ackData = ackMsg.getAsJsonObject("data");
            String status = ackData.get("status").getAsString();

            if (status.equals("OK")) {
                System.out.println("  Fragment stocké avec succès!");
            } else {
                System.err.println("  Erreur slave : " + status);
                throw new IOException("Slave a refusé le fragment: " + status);
            }

        } catch (Exception e) {
            System.err.println(" Erreur connexion slave : " + e.getMessage());
            throw new IOException("Echec envoi fragment au slave", e);
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

            SlaveInfo slaveInfo = new SlaveInfo(id, capacity, port,host_str);
            for (SlaveInfo s : slaves) {
                if (s.getId() == id) {
                    System.out.println("Slave deja enregistre: " + id + " - envoi PING");
                    return;
                }
            }
            slaves.add(slaveInfo);
            writeToFile(slaveInfo);
            System.out.println("Nouveau slave enregistre: " + id);

        } catch (Exception e) {
            System.err.println(" Erreur parsing REGISTER: " + e.getMessage());
        }
    }

    public static void writeToFile(SlaveInfo slaveInfo) throws IOException {
        String path = "logs/slave.txt";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path, true))) {
            String line = slaveInfo.getId() + "," + slaveInfo.getCapacity() + "," + slaveInfo.getPort() + "," + slaveInfo.getHost();
            bw.write(line);
            bw.newLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //format csv
    public static void loadSlaveFromFile() throws IOException {
        String path = "logs/slave.txt";
        try (BufferedReader bf = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = bf.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split(",");
                String str_id = parts[0];
                String str_capacity = parts[1];
                String str_port = parts[2];
                String str_ip = parts[3];
                int id = Integer.parseInt(str_id);
                long capacity = Long.parseLong(str_capacity);
                int port = Integer.parseInt(str_port);
                SlaveInfo slaveInfo = new SlaveInfo(id, capacity,port,str_ip);
                slaves.add(slaveInfo);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    //mbola essaie fotsiny fa tsy optimal
    public static int getBestCapacityFromSlave() {
        if (slaves.isEmpty()) return -1;
        SlaveInfo best = slaves.get(0);
        for (SlaveInfo s : slaves) {
            if (s.getCapacity() > best.getCapacity()) {
                best = s;
            }
        }
        return best.getId();
    }

    public static SlaveInfo getSlaveById(int id) {
        for (SlaveInfo s : slaves) {
            if (s.getId() == id) return s;
        }
        return null;
    }

    public static void ListenPingPong() throws InterruptedException {
        try{
            while (true) {
                //maijery hoe mbola velon asa tsia
                sleep(10000);
                sendPing();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }


}
