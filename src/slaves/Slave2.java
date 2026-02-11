package slaves;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;


public class Slave2 {

    private final String slaveId;
    private final String masterHost;
    private final int masterPort;
    private volatile boolean running = true;

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public Slave2(String slaveId, String masterHost, int masterPort) {
        this.slaveId = slaveId;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
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
        String json = "{\"type\":\"REGISTER\",\"data\":{\"slave_id\":\"" + slaveId + "\",\"capacity\":5}}";
        sendRaw(json);
        System.out.println("REGISTER envoyé: " + json);
    }

    private void listenLoop() {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("Reçu du Master : " + line);
                // Si PING, répondre par PONG
                if (line.contains("\"type\":\"PING\"") || line.contains("\"type\": \"PING\"")) {
                    sendRaw("{\"type\":\"PONG\",\"data\":{\"slave_id\":\"" + slaveId + "\"}}");
                    System.out.println("PONG envoyé");
                }

            }
            System.out.println("Lecture terminée (Master fermé la connexion)");
        } catch (IOException e) {
            System.err.println("Erreur d'écoute: " + e.getMessage());
        }
    }

    private synchronized void sendRaw(String jsonLine) {
        if (out == null) {
            System.err.println("Impossible d'envoyer, sortie non disponible");
            return;
        }
        out.println(jsonLine);
        out.flush();
    }

    private void closeSocket() {
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
            System.err.println("Usage: java -cp <classpath> slaves.Slave <slaveId> <masterHost> <masterPort>");
            System.exit(1);
        }
        String slaveId = args[0];
        String masterHost = args[1];
        int masterPort = Integer.parseInt(args[2]);

        Slave s = new Slave(slaveId, masterHost, masterPort);
        s.start();


        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}