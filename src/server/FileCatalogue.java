package server;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public final class FileCatalogue {
    public static final String LOGS_DIR = "logs";
    public static final String SLAVES_FILE = LOGS_DIR + "/slave.txt";
    public static final String FRAGMENTS_CSV = LOGS_DIR + "/fragments.csv";
    public static final String FILES_CSV = LOGS_DIR + "/files.csv";

    private FileCatalogue() {}

    public static void ensureLogsDir() {
        try {
            Files.createDirectories(Paths.get(LOGS_DIR));

            File fragments = new File(FRAGMENTS_CSV);
            if (!fragments.exists()) {
                try (FileWriter fw = new FileWriter(fragments, true)) {
                    fw.write("fileId,fragmentIndex,slaveId,slaveHost,slavePort,fragmentSize\n");
                }
            }

            File files = new File(FILES_CSV);
            if (!files.exists()) {
                try (FileWriter fw = new FileWriter(files, true)) {
                    fw.write("fileId,name,size,fragments,date\n");
                }
            }

            File slaves = new File(SLAVES_FILE);
            if (!slaves.exists()) {
                slaves.getParentFile().mkdirs();
                slaves.createNewFile();
            }
        } catch (IOException e) {
            throw new RuntimeException("Impossible de créer dossier logs: " + e.getMessage(), e);
        }
    }

    public static synchronized void appendFragmentMapping(String fileId, int fragmentIndex, SlaveInfo slave, int fragmentSize) {
        String line = String.format("%s,%d,%d,%s,%d,%d\n",
                escapeCsv(fileId),
                fragmentIndex,
                slave.getId(),
                escapeCsv(slave.getHost()),
                slave.getPort(),
                fragmentSize);
        try (FileWriter fw = new FileWriter(FRAGMENTS_CSV, true)) {
            fw.write(line);
        } catch (IOException e) {
            System.err.println("Impossible d'écrire fragments.csv : " + e.getMessage());
        }
    }

    public static synchronized void appendFileMetadata(String fileId, String name, long size, int fragments) {
        String date = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        String line = String.format("%s,%s,%d,%d,%s\n",
                escapeCsv(fileId),
                escapeCsv(name),
                size,
                fragments,
                escapeCsv(date));
        try (FileWriter fw = new FileWriter(FILES_CSV, true)) {
            fw.write(line);
        } catch (IOException e) {
            System.err.println("Impossible d'écrire files.csv : " + e.getMessage());
        }
    }

    public static FileMetadata readFileMetadata(String fileId) {
        try (BufferedReader br = new BufferedReader(new FileReader(FILES_CSV))) {
            br.readLine(); // skip header
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] parts = splitCsvLine(line);
                if (parts.length < 5) continue;
                if (parts[0].equals(fileId)) {
                    FileMetadata fm = new FileMetadata();
                    fm.fileId = parts[0];
                    fm.name = parts[1];
                    fm.size = Long.parseLong(parts[2]);
                    fm.fragments = Integer.parseInt(parts[3]);
                    fm.date = parts[4];
                    return fm;
                }
            }
        } catch (IOException e) {
            System.err.println("Erreur lecture files.csv: " + e.getMessage());
        }
        return null;
    }

    public static List<FragmentMapping> readFragmentMappings(String fileId) {
        List<FragmentMapping> list = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(FRAGMENTS_CSV))) {
            br.readLine(); // skip header
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] parts = splitCsvLine(line);
                if (parts.length < 6) continue;
                if (!parts[0].equals(fileId)) continue;
                FragmentMapping m = new FragmentMapping();
                m.fileId = parts[0];
                m.fragmentIndex = Integer.parseInt(parts[1]);
                m.slaveId = Integer.parseInt(parts[2]);
                m.slaveHost = parts[3];
                m.slavePort = Integer.parseInt(parts[4]);
                m.fragmentSize = Integer.parseInt(parts[5]);
                list.add(m);
            }
        } catch (IOException e) {
            System.err.println("Erreur lecture fragments.csv: " + e.getMessage());
        }
        return list;
    }

    public static List<FileMetadata> listAllFiles() {
        List<FileMetadata> res = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(FILES_CSV))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] parts = splitCsvLine(line);
                if (parts.length < 5) continue;
                FileMetadata fm = new FileMetadata();
                fm.fileId = parts[0];
                fm.name = parts[1];
                fm.size = Long.parseLong(parts[2]);
                fm.fragments = Integer.parseInt(parts[3]);
                fm.date = parts[4];
                res.add(fm);
            }
        } catch (IOException e) {
            System.err.println("Erreur listAllFiles: " + e.getMessage());
        }
        return res;
    }

    private static String escapeCsv(String s) {
        if (s == null) return "";
        if (s.contains(",") || s.contains("\"") || s.contains("\n")) {
            s = s.replace("\"", "\"\"");
            return "\"" + s + "\"";
        }
        return s;
    }

    private static String[] splitCsvLine(String line) {
        List<String> parts = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        sb.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    sb.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    parts.add(sb.toString());
                    sb.setLength(0);
                } else {
                    sb.append(c);
                }
            }
        }
        parts.add(sb.toString());
        return parts.toArray(new String[0]);
    }

    // DTOs renvoyés au MainServer
    public static class FileMetadata {
        public String fileId;
        public String name;
        public long size;
        public int fragments;
        public String date;
    }

    public static class FragmentMapping {
        public String fileId;
        public int fragmentIndex;
        public int slaveId;
        public String slaveHost;
        public int slavePort;
        public int fragmentSize;
    }
}