package slaves;

import com.google.gson.*;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Slave - Systeme de stockage distribué 
 * 
 * Ce que fait ce programme:
 * 1. Ecoute sur un port (ServerSocket) pour recevoir des commandes du Master
 * 2. Stocke des fragments de fichiers (STORE_FRAGMENT)
 * 3. Renvoie des fragments (GET_FRAGMENT)
 * 4. Envoie régulierement un heartbeat au Master (STATUS)
 * 5. Vérifie les checksums (SHA-256) si fournis
 * 6. Sauvegarde un index des fragments dans index.json
 * 
 * Usage:
 *   java -cp out:libs/gson-2.8.9.jar slaves.Slave ryan-01 localhost 8080 9001 ./slave_storage
 */
public class Slave {
    
    private String slaveId;              
    private String masterHost;          
    private int masterPort;              
    private int listenPort;             
    private String storageDir;           
    
    private Gson gson;                 
    private boolean running;             
    
 
    private ExecutorService threadPool;
    private ScheduledExecutorService heartbeatTimer;
    
    private Map<String, Map<String, Object>> index;
    
    private static final long MAX_FRAGMENT_SIZE = 500L * 1024L * 1024L; // 500 MB max
    private static final long MIN_FREE_SPACE = 100L * 1024L * 1024L;     // 100 MB minimum à garder

    // ========== CONSTRUCTEUR ==========
    
    public Slave(String slaveId, String masterHost, int masterPort, int listenPort, String storageDir) {
        this.slaveId = slaveId;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.listenPort = listenPort;
        this.storageDir = storageDir;
        
        this.gson = new Gson();
        this.running = true;
        this.index = new ConcurrentHashMap<>(); 
        this.threadPool = Executors.newFixedThreadPool(8);
        this.heartbeatTimer = Executors.newScheduledThreadPool(1);
        
        try {
            Files.createDirectories(Paths.get(storageDir));
            afficher("Dossier de stockage créé/vérifié: " + storageDir);
        } catch (Exception e) {
            afficherErreur("Impossible de créer le dossier: " + e.getMessage());
        }
        
        nettoyerFichiersTemp();
        
        chargerIndex();
    }


    
    public void demarrer() {
        afficher("Démarrage du slave " + slaveId + "...");
        
        enregistrerAupresDuMaster();
        
        heartbeatTimer.scheduleAtFixedRate(new Runnable() {
            public void run() {
                envoyerHeartbeat();
            }
        }, 10, 10, TimeUnit.SECONDS);
        
        try {
            ServerSocket serverSocket = new ServerSocket(listenPort);
            afficher("Slave écoute sur le port " + listenPort);
            
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    
                    threadPool.submit(new Runnable() {
                        public void run() {
                            traiterConnexion(clientSocket);
                        }
                    });
                } catch (Exception e) {
                    if (running) {
                        afficherErreur("Erreur accept: " + e.getMessage());
                    }
                }
            }
            
            serverSocket.close();
        } catch (Exception e) {
            afficherErreur("Erreur ServerSocket: " + e.getMessage());
        } finally {
            arreter();
        }
    }


    
    public void arreter() {
        afficher("Arrêt du slave...");
        running = false;
        
        heartbeatTimer.shutdownNow();
        threadPool.shutdown();
        
        sauvegarderIndex();
        
        afficher("Slave arrêté.");
    }

    
    private void enregistrerAupresDuMaster() {
        try {
            Socket socket = new Socket(masterHost, masterPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            
            String host = InetAddress.getLocalHost().getHostAddress();
            
            String json = "{\"type\":\"REGISTER\",\"data\":{" +
                          "\"slave_id\":\"" + slaveId + "\"," +
                          "\"capacity\":" + obtenirEspaceLibre() + "," +
                          "\"host\":\"" + host + "\"," +
                          "\"port\":" + listenPort +
                          "}}";
            
            out.println(json);
            out.flush();
            
            afficher("REGISTER envoyé au master (host=" + host + ", port=" + listenPort + ")");
            
            out.close();
            socket.close();
        } catch (Exception e) {
            afficherErreur("Impossible de s'enregistrer au master: " + e.getMessage());
        }
    }

    
    private void envoyerHeartbeat() {
        try {
            Socket socket = new Socket(masterHost, masterPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            
            String json = "{\"type\":\"STATUS\",\"data\":{" +
                          "\"slave_id\":\"" + slaveId + "\"," +
                          "\"free_space\":" + obtenirEspaceLibre() + "," +
                          "\"fragments_count\":" + index.size() + "," +
                          "\"timestamp\":" + System.currentTimeMillis() +
                          "}}";
            
            out.println(json);
            out.flush();
            
            afficher("STATUS envoyé (fragments: " + index.size() + ", espace: " + obtenirEspaceLibre() + ")");
            
            out.close();
            socket.close();
        } catch (Exception e) {
            
        }
    }

    
    private void traiterConnexion(Socket socket) {
        String remote = socket.getRemoteSocketAddress().toString();
        afficher("Connexion entrante de " + remote);
        
        try {
            socket.setSoTimeout(60000);
            
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            
            String ligne = lireLigneJSON(in);
            
            if (ligne == null || ligne.isEmpty()) {
                afficherErreur("Aucune donnée reçue de " + remote);
                socket.close();
                return;
            }
            
            JsonObject message = gson.fromJson(ligne, JsonObject.class);
            String type = message.get("type").getAsString();
            JsonObject data = message.has("data") ? message.getAsJsonObject("data") : null;
            
            afficher("Commande reçue: " + type);
            
            if (type.equals("STORE_FRAGMENT")) {
                stockerFragment(in, out, data);
            } else if (type.equals("GET_FRAGMENT")) {
                envoyerFragment(out, data);
            } else if (type.equals("PING")) {
                repondrePong(out);
            } else if (type.equals("DELETE_FRAGMENT")) {
                supprimerFragment(out, data);
            } else {
                envoyerErreur(out, "unsupported", "Type non supporté: " + type);
            }
            
            out.flush();
            socket.close();
        } catch (Exception e) {
            afficherErreur("Erreur traitement connexion " + remote + ": " + e.getMessage());
            try { socket.close(); } catch (Exception ignored) {}
        }
    }


    
    private String lireLigneJSON(InputStream in) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        
        while ((b = in.read()) != -1) {
            if (b == '\n') break; 
            buffer.write(b);
            
            if (buffer.size() > 10000) {
                throw new Exception("Ligne JSON trop longue");
            }
        }
        
        if (buffer.size() == 0 && b == -1) return null;
        
        return buffer.toString("UTF-8").trim();
    }

    
    
    private void stockerFragment(InputStream in, OutputStream out, JsonObject data) throws Exception {
        if (!data.has("file_id") || !data.has("fragment_id") || !data.has("length")) {
            envoyerACK(out, -1, "ERROR: champs manquants");
            return;
        }
        
        String fileId = nettoyerNom(data.get("file_id").getAsString());
        int fragmentId = data.get("fragment_id").getAsInt();
        long taille = data.get("length").getAsLong();
        String checksumAttendu = data.has("checksum") ? data.get("checksum").getAsString() : null;
        
        afficher("STORE_FRAGMENT: fileId=" + fileId + ", fragmentId=" + fragmentId + ", taille=" + taille);
        
        if (taille < 0 || taille > MAX_FRAGMENT_SIZE) {
            envoyerACK(out, fragmentId, "ERROR: taille invalide");
            return;
        }
        
        if (obtenirEspaceLibre() < MIN_FREE_SPACE + taille) {
            envoyerACK(out, fragmentId, "ERROR: espace disque insuffisant");
            return;
        }
        
        String nomFichier = fileId + "_fragment_" + fragmentId + ".dat";
        String cheminTemp = Paths.get(storageDir, nomFichier + ".tmp").toString();
        String cheminFinal = Paths.get(storageDir, nomFichier).toString();
        
       
        MessageDigest digest = null;
        if (checksumAttendu != null) {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (Exception e) {
                afficherErreur("SHA-256 non disponible: " + e.getMessage());
            }
        }
        
        
        try {
            FileOutputStream fos = new FileOutputStream(cheminTemp);
            byte[] buffer = new byte[8192]; 
            long restant = taille;
            
            while (restant > 0) {
                int aLire = (int) Math.min(buffer.length, restant);
                int lu = in.read(buffer, 0, aLire);
                
                if (lu == -1) {
                    fos.close();
                    new File(cheminTemp).delete();
                    throw new Exception("Connexion coupée pendant le transfert");
                }
                
                fos.write(buffer, 0, lu);
                
                if (digest != null) {
                    digest.update(buffer, 0, lu);
                }
                
                restant -= lu;
            }
            
            fos.flush();
            fos.close();
            
        } catch (Exception e) {
            new File(cheminTemp).delete();
            envoyerACK(out, fragmentId, "ERROR: " + e.getMessage());
            return;
        }
        
        if (digest != null && checksumAttendu != null) {
            String checksumCalculé = bytesToHex(digest.digest());
            
            if (!checksumCalculé.equalsIgnoreCase(checksumAttendu)) {
                new File(cheminTemp).delete();
                envoyerACK(out, fragmentId, "ERROR: checksum incorrect");
                afficherErreur("Checksum mismatch: attendu=" + checksumAttendu + ", reçu=" + checksumCalculé);
                return;
            }
        }
        
        
        File temp = new File(cheminTemp);
        File dest = new File(cheminFinal);
        
        if (dest.exists()) dest.delete();
        boolean renomme = temp.renameTo(dest);
        
        if (!renomme) {
            temp.delete();
            envoyerACK(out, fragmentId, "ERROR: impossible de renommer le fichier");
            return;
        }
        
        Map<String, Object> meta = new HashMap<>();
        meta.put("fileId", fileId);
        meta.put("fragmentId", fragmentId);
        meta.put("taille", taille);
        meta.put("checksum", checksumAttendu);
        index.put(nomFichier, meta);
        
        sauvegarderIndex();
        
        // Envoyer ACK OK
        envoyerACK(out, fragmentId, "OK");
        afficher("Fragment stocké: " + nomFichier + " (" + taille + " bytes)");
    }

    
    private void envoyerFragment(OutputStream out, JsonObject data) throws Exception {
        if (!data.has("file_id") || !data.has("fragment_id")) {
            envoyerErreur(out, "missing_fields", "file_id ou fragment_id manquant");
            return;
        }
        
        String fileId = nettoyerNom(data.get("file_id").getAsString());
        int fragmentId = data.get("fragment_id").getAsInt();
        String nomFichier = fileId + "_fragment_" + fragmentId + ".dat";
        String chemin = storageDir + "/" + nomFichier;
        
        File fichier = new File(chemin);
        
        if (!fichier.exists()) {
            envoyerErreur(out, "not_found", "Fragment non trouvé");
            return;
        }
        
        long taille = fichier.length();
        
  
        String checksum = null;
        if (index.containsKey(nomFichier)) {
            Map<String, Object> meta = index.get(nomFichier);
            checksum = (String) meta.get("checksum");
        }
        
        String json = "{\"type\":\"GET_FRAGMENT_RESPONSE\",\"data\":{" +
                      "\"file_id\":\"" + fileId + "\"," +
                      "\"fragment_id\":" + fragmentId + "," +
                      "\"length\":" + taille;
        if (checksum != null) {
            json += ",\"checksum\":\"" + checksum + "\"";
        }
        json += "}}";
        
        out.write((json + "\n").getBytes("UTF-8"));
        out.flush();
        
        FileInputStream fis = new FileInputStream(fichier);
        byte[] buffer = new byte[8192];
        int lu;
        
        while ((lu = fis.read(buffer)) != -1) {
            out.write(buffer, 0, lu);
        }
        
        out.flush();
        fis.close();
        
        afficher("Fragment envoyé: " + nomFichier + " (" + taille + " bytes)");
    }

   
    
    private void supprimerFragment(OutputStream out, JsonObject data) throws Exception {
        if (!data.has("file_id") || !data.has("fragment_id")) {
            envoyerErreur(out, "missing_fields", "file_id ou fragment_id manquant");
            return;
        }
        
        String fileId = nettoyerNom(data.get("file_id").getAsString());
        int fragmentId = data.get("fragment_id").getAsInt();
        String nomFichier = fileId + "_fragment_" + fragmentId + ".dat";
        String chemin = storageDir + "/" + nomFichier;
        
        File fichier = new File(chemin);
        boolean supprime = fichier.delete();
        
        if (supprime) {
            index.remove(nomFichier);
            sauvegarderIndex();
            
            String json = "{\"type\":\"DELETE_ACK\",\"data\":{\"status\":\"OK\"}}";
            out.write((json + "\n").getBytes("UTF-8"));
            afficher("Fragment supprimé: " + nomFichier);
        } else {
            envoyerErreur(out, "delete_error", "Impossible de supprimer le fichier");
        }
    }


    
    private void repondrePong(OutputStream out) throws Exception {
        String json = "{\"type\":\"PONG\",\"data\":{\"slave_id\":\"" + slaveId + "\"}}";
        out.write((json + "\n").getBytes("UTF-8"));
        afficher("PONG envoyé");
    }

   
    
    private void envoyerACK(OutputStream out, int fragmentId, String status) throws Exception {
        String json = "{\"type\":\"ACK\",\"data\":{" +
                      "\"fragment_id\":" + fragmentId + "," +
                      "\"status\":\"" + status + "\"}}";
        out.write((json + "\n").getBytes("UTF-8"));
        out.flush();
    }
  
    
    private void envoyerErreur(OutputStream out, String code, String message) throws Exception {
        String json = "{\"type\":\"ERROR\",\"data\":{" +
                      "\"code\":\"" + code + "\"," +
                      "\"message\":\"" + message + "\"}}";
        out.write((json + "\n").getBytes("UTF-8"));
        out.flush();
    }

    
    private void chargerIndex() {
        String cheminIndex = storageDir + "/index.json";
        File fichierIndex = new File(cheminIndex);
        
        if (!fichierIndex.exists()) {
            afficher("Aucun index existant, création d'un nouvel index");
            return;
        }
        
        try {
            FileReader reader = new FileReader(fichierIndex);
            JsonObject json = gson.fromJson(reader, JsonObject.class);
            reader.close();
            
            for (String cle : json.keySet()) {
                JsonObject obj = json.getAsJsonObject(cle);
                Map<String, Object> meta = new HashMap<>();
                meta.put("fileId", obj.get("fileId").getAsString());
                meta.put("fragmentId", obj.get("fragmentId").getAsInt());
                meta.put("taille", obj.get("taille").getAsLong());
                if (obj.has("checksum") && !obj.get("checksum").isJsonNull()) {
                    meta.put("checksum", obj.get("checksum").getAsString());
                }
                index.put(cle, meta);
            }
            
            afficher("Index chargé: " + index.size() + " fragments");
        } catch (Exception e) {
            afficherErreur("Erreur chargement index: " + e.getMessage());
        }
    }
    
    private void sauvegarderIndex() {
        String cheminIndex = storageDir + "/index.json";
        
        try {
            FileWriter writer = new FileWriter(cheminIndex);
            gson.toJson(index, writer);
            writer.flush();
            writer.close();
        } catch (Exception e) {
            afficherErreur("Erreur sauvegarde index: " + e.getMessage());
        }
    }

  
    
    private void nettoyerFichiersTemp() {
        File dossier = new File(storageDir);
        File[] fichiers = dossier.listFiles();
        
        if (fichiers == null) return;
        
        for (File f : fichiers) {
            if (f.getName().endsWith(".tmp")) {
                f.delete();
                afficher("Fichier temporaire supprimé: " + f.getName());
            }
        }
    }


    
    private String nettoyerNom(String nom) {
        String nettoye = nom.replaceAll("[^A-Za-z0-9._-]", "_");
        // Prevent directory traversal by rejecting any remaining ".." sequences
        if (nettoye.contains("..")) {
            throw new IllegalArgumentException("Nom de fichier invalide: séquence '..' non autorisée");
        }
        return nettoye;
    }
    
    private long obtenirEspaceLibre() {
        try {
            File f = new File(storageDir);
            return f.getUsableSpace();
        } catch (Exception e) {
            return -1;
        }
    }
    
    
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    
   
    private void afficher(String message) {
        System.out.println("[" + slaveId + "] " + message);
    }
    
    
    private void afficherErreur(String message) {
        System.err.println("[" + slaveId + "] ERREUR: " + message);
    }

    
    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: java slaves.Slave <slaveId> <masterHost> <masterPort> <listenPort> [storageDir]");
            System.exit(1);
        }
        
        String slaveId = args[0];
        String masterHost = args[1];
        int masterPort = Integer.parseInt(args[2]);
        int listenPort = Integer.parseInt(args[3]);
        String storageDir = args.length >= 5 ? args[4] : "./slave_storage";
        
        try {
            Slave slave = new Slave(slaveId, masterHost, masterPort, listenPort, storageDir);
            
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    slave.arreter();
                }
            });
            
            slave.demarrer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}