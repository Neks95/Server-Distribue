package server;

import client.ClientSocket;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import slaves.Slave;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class MainServer {
    private static final int PORT = 5000;
    private static final int fragment_SIZE = 1024 * 1024;
    private static final Gson gson = new Gson();

    private static List<SlaveInfo> slaves = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        System.out.println("demmarage du serveur sur le port " + PORT);
        System.out.println("Chargement des slaves ....");
        loadSlaveFromFile();
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
        }
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

            System.out.println(" Réception du fichier...");

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

        System.out.println("\nEnvoi fragment au Slave " + slave.getId());

        String slaveIp = "localhost";
        int slavePort = 6000 + slave.getId();  // Exemple : Slave #1 → port 6001

        try (Socket slaveSocket = new Socket(slaveIp, slavePort);
             PrintWriter textOut = new PrintWriter(slaveSocket.getOutputStream(), true);
             DataOutputStream binaryOut = new DataOutputStream(slaveSocket.getOutputStream());
             BufferedReader textIn = new BufferedReader(
                     new InputStreamReader(slaveSocket.getInputStream())
             )) {

            // 1. Envoyer commande JSON (métadonnées)
            JsonObject msg = new JsonObject();
            msg.addProperty("type", "STORE_FRAGMENT");

            JsonObject data = new JsonObject();
            data.addProperty("file_id", fileId);
            data.addProperty("fragment_id", fragmentIndex);
            data.addProperty("size", fragmentData.length);

            msg.add("data", data);

            textOut.println(gson.toJson(msg));
            textOut.flush();

            System.out.println("📤 Commande envoyée");

            // 2. Attendre READY du slave
            String readyLine = textIn.readLine();

            if (readyLine == null || !readyLine.contains("\"type\":\"READY\"")) {
                System.err.println("❌ Slave pas prêt : " + readyLine);
                return;
            }

            System.out.println("✅ Slave prêt");

            // 3. Envoyer les données BINAIRES
            binaryOut.write(fragmentData);
            binaryOut.flush();

            System.out.println("📤 Données envoyées : " + fragmentData.length + " bytes");

            // 4. Attendre ACK du slave
            String ackLine = textIn.readLine();
            JsonObject ackMsg = gson.fromJson(ackLine, JsonObject.class);

            JsonObject ackData = ackMsg.getAsJsonObject("data");
            String status = ackData.get("status").getAsString();

            if (status.equals("OK")) {
                String checksum = ackData.has("checksum") ?
                        ackData.get("checksum").getAsString() : "N/A";

                System.out.println("✅ ACK reçu : " + status);
                System.out.println("🔐 Checksum : " + checksum);

                // TODO: Sauvegarder checksum dans métadonnées

            } else {
                System.err.println("❌ Erreur slave : " + status);
            }

        } catch (Exception e) {
            System.err.println("❌ Erreur connexion slave : " + e.getMessage());
            throw new IOException("Échec envoi fragment au slave", e);
        }
    }


    public static void registerSlave(Socket client, String line) throws IOException {
        String slaveId = line.split("\"slave_id\":\"")[1].split("\"")[0];
        String capacityStr = line.split("\"capacity\":")[1].split("}")[0];
        long capacity = Long.parseLong(capacityStr);
        int id = Integer.parseInt(slaveId);
        SlaveInfo slaveInfo = new SlaveInfo(id, capacity);
        //verification raha efa ao le slave
        for (SlaveInfo s : slaves) {
            if (s.getId() == id) {
                System.out.println("Slave deja enregistre: " + id);
                System.out.println("le meilleur slave est : " + getBestCapacityFromSlave());

                return;
            }
        }
        writeToFile(slaveInfo);
        slaves.add(slaveInfo);

    }

    public static void writeToFile(SlaveInfo slaveInfo) throws IOException {
        String path = "logs/slave.txt";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path, true))) {
            String line = slaveInfo.getId() + ";;" + slaveInfo.getCapacity();
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
                String[] parts = line.split(";;");
                String str_id = parts[0];
                String str_capacity = parts[1];
                int id = Integer.parseInt(str_id);
                long capacity = Long.parseLong(str_capacity);
                SlaveInfo slaveInfo = new SlaveInfo(id, capacity);
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


}
