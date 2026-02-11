package server;
import client.ClientSocket;
import slaves.Slave;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class MainServer {
    private static final int PORT = 5000;

    private static List<SlaveInfo> slaves = new ArrayList<>();
    public static void main(String[] args) throws IOException {
        System.out.println("demmarage du serveur sur le port " + PORT);
        System.out.println("Chargement des slaves ....");
        loadSlaveFromFile();
        try(ServerSocket serverSocket = new ServerSocket(PORT)) {
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
        if(line != null) {
            if(line.contains("REGISTER")){
                registerSlave(client , line);
            } else {
                handleClient(client);
            }
        }

    }

    public static void handleClient(Socket client ) throws IOException {
        //miandry an'Misaina


    }

    public static void registerSlave(Socket client , String line) throws IOException {
        String slaveId = line.split("\"slave_id\":\"")[1].split("\"")[0];
        String capacityStr = line.split("\"capacity\":")[1].split("}")[0];
        long capacity = Long.parseLong(capacityStr);
        int id = Integer.parseInt(slaveId);
        SlaveInfo slaveInfo = new SlaveInfo(id, capacity);
        //verification raha efa ao le slave
        for(SlaveInfo s : slaves){
            if(s.getId() == id){
                System.out.println("Slave deja enregistre: " + id);
                System.out.println("le meilleur slave est : " + getBestCapacityFromSlave());

                return;
            }
        }
        writeToFile(slaveInfo);
    }

    public static void writeToFile(SlaveInfo slaveInfo) throws IOException {
        String path = "logs/slave.txt";
        try(BufferedWriter bw = new BufferedWriter(new FileWriter(path,true))){
            String line = slaveInfo.getId() + ";;" + slaveInfo.getCapacity();
            bw.write(line);
            bw.newLine();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //format csv
    public static void loadSlaveFromFile() throws IOException {
        String path = "logs/slave.txt";
        try(BufferedReader bf = new BufferedReader(new FileReader(path))){
            String line;
            while((line = bf.readLine())!= null){
                String[] parts = line.split(";;");
                String str_id = parts[0];
                String str_capacity = parts[1];
                int id = Integer.parseInt(str_id);
                long capacity = Long.parseLong(str_capacity);
                SlaveInfo slaveInfo = new SlaveInfo(id, capacity);
                slaves.add(slaveInfo);
            }
        } catch (IOException e){
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



}
