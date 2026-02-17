package ui;

import client.ClientSocket;
import client.ClientSocket.FileInfo;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.border.TitledBorder;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.io.File;
import java.util.List;

/**
 * UI simplifiée pour le client : connexion, upload, list, download.
 */
public class MainWindow extends JFrame {

    private JTextField txtHost;
    private JTextField txtPort;
    private JButton btnConnect;
    private JLabel lblConnectionStatus;

    private JButton btnChooseFile;
    private JButton btnUpload;
    private JProgressBar progressUpload;
    private JLabel lblSelectedFile;
    private File selectedFile;

    private DefaultTableModel tableModel;
    private JTable fileTable;
    private JButton btnRefresh;
    private JButton btnDownload;
    private JProgressBar progressDownload;

    private ClientSocket client;
    private boolean isConnected = false;

    public MainWindow() {
        setTitle("Client - Stockage Distribue (simplifie)");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(800, 500);
        setLocationRelativeTo(null);

        initComponents();
        setVisible(true);
    }

    private void initComponents() {
        JPanel main = new JPanel(new BorderLayout(8, 8));
        main.setBorder(new EmptyBorder(8, 8, 8, 8));

        // Top: connection
        JPanel conn = new JPanel(new FlowLayout(FlowLayout.LEFT));
        conn.setBorder(new TitledBorder("Connexion Master"));
        conn.add(new JLabel("Host:"));
        txtHost = new JTextField("localhost", 12);
        conn.add(txtHost);
        conn.add(new JLabel("Port:"));
        txtPort = new JTextField("5000", 6);
        conn.add(txtPort);
        btnConnect = new JButton("Connecter");
        btnConnect.addActionListener(e -> handleConnect());
        conn.add(btnConnect);
        lblConnectionStatus = new JLabel("Deconnecte");
        lblConnectionStatus.setForeground(Color.RED);
        conn.add(lblConnectionStatus);
        main.add(conn, BorderLayout.NORTH);

        // Left: Upload panel
        JPanel left = new JPanel(new BorderLayout(8, 8));

        JPanel uploadPanel = new JPanel(new BorderLayout(6, 6));
        uploadPanel.setBorder(new TitledBorder("Upload"));
        btnChooseFile = new JButton("Choisir fichier...");
        btnChooseFile.setEnabled(false);
        btnChooseFile.addActionListener(e -> handleChooseFile());
        lblSelectedFile = new JLabel("Aucun fichier");
        uploadPanel.add(btnChooseFile, BorderLayout.WEST);
        uploadPanel.add(lblSelectedFile, BorderLayout.CENTER);

        JPanel uploadAction = new JPanel(new FlowLayout(FlowLayout.LEFT));
        btnUpload = new JButton("Uploader");
        btnUpload.setEnabled(false);
        btnUpload.addActionListener(e -> handleUpload());
        progressUpload = new JProgressBar(0, 100);
        progressUpload.setStringPainted(true);
        uploadAction.add(btnUpload);
        uploadAction.add(progressUpload);

        uploadPanel.add(uploadAction, BorderLayout.SOUTH);
        left.add(uploadPanel, BorderLayout.NORTH);

        // Center: List + table
        JPanel listPanel = new JPanel(new BorderLayout(6, 6));
        listPanel.setBorder(new TitledBorder("Fichiers disponibles"));
        String[] cols = {"Nom", "Taille", "Fragments", "Date", "FileId"};
        tableModel = new DefaultTableModel(cols, 0) {
            @Override public boolean isCellEditable(int row, int col) { return false; }
        };
        fileTable = new JTable(tableModel);
        fileTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        JScrollPane sc = new JScrollPane(fileTable);
        listPanel.add(sc, BorderLayout.CENTER);

        JPanel listButtons = new JPanel(new FlowLayout(FlowLayout.LEFT));
        btnRefresh = new JButton("Rafraichir");
        btnRefresh.setEnabled(false);
        btnRefresh.addActionListener(e -> handleRefreshList());
        btnDownload = new JButton("Telecharger");
        btnDownload.setEnabled(false);
        btnDownload.addActionListener(e -> handleDownload());
        progressDownload = new JProgressBar(0, 100);
        progressDownload.setStringPainted(true);
        listButtons.add(btnRefresh);
        listButtons.add(btnDownload);
        listButtons.add(progressDownload);
        listPanel.add(listButtons, BorderLayout.SOUTH);

        left.add(listPanel, BorderLayout.CENTER);

        main.add(left, BorderLayout.CENTER);

        // Right: simple help/info
        JTextArea info = new JTextArea("Utilisation:\n1) Connecter au Master\n2) Choisir un fichier et uploader\n3) Rafraichir la liste et télécharger");
        info.setEditable(false);
        info.setBackground(getContentPane().getBackground());
        main.add(info, BorderLayout.EAST);

        setContentPane(main);
    }

    private void handleConnect() {
        String host = txtHost.getText().trim();
        int port;
        try {
            port = Integer.parseInt(txtPort.getText().trim());
        } catch (NumberFormatException ex) {
            JOptionPane.showMessageDialog(this, "Port invalide", "Erreur", JOptionPane.ERROR_MESSAGE);
            return;
        }

        btnConnect.setEnabled(false);
        lblConnectionStatus.setText("Connexion...");
        lblConnectionStatus.setForeground(Color.ORANGE);

        SwingWorker<Boolean, Void> w = new SwingWorker<>() {
            @Override protected Boolean doInBackground() {
                client = new ClientSocket(host, port);
                return client.testConnection();
            }
            @Override protected void done() {
                try {
                    isConnected = get();
                    if (isConnected) {
                        lblConnectionStatus.setText("Connecte");
                        lblConnectionStatus.setForeground(new Color(0, 128, 0));
                        btnChooseFile.setEnabled(true);
                        btnRefresh.setEnabled(true);
                        btnConnect.setText("Reconnecter");
                    } else {
                        lblConnectionStatus.setText("Echec connexion");
                        lblConnectionStatus.setForeground(Color.RED);
                        JOptionPane.showMessageDialog(MainWindow.this, "Impossible de se connecter au Master", "Erreur", JOptionPane.ERROR_MESSAGE);
                    }
                } catch (Exception e) {
                    lblConnectionStatus.setText("Erreur");
                    lblConnectionStatus.setForeground(Color.RED);
                } finally {
                    btnConnect.setEnabled(true);
                }
            }
        };
        w.execute();
    }

    private void handleChooseFile() {
        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("Choisir un fichier");
        int r = chooser.showOpenDialog(this);
        if (r == JFileChooser.APPROVE_OPTION) {
            selectedFile = chooser.getSelectedFile();
            lblSelectedFile.setText(selectedFile.getName() + " (" + humanSize(selectedFile.length()) + ")");
            btnUpload.setEnabled(true);
        }
    }

    private void handleUpload() {
        if (!isConnected || selectedFile == null) return;

        btnUpload.setEnabled(false);
        btnChooseFile.setEnabled(false);
        progressUpload.setValue(0);

        SwingWorker<String, Integer> w = new SwingWorker<>() {
            @Override protected String doInBackground() throws Exception {
                return client.uploadFile(selectedFile, (sent, total) -> {
                    int pct = total > 0 ? (int) ((sent * 100) / total) : 0;
                    publish(pct);
                });
            }

            @Override protected void process(List<Integer> chunks) {
                int latest = chunks.get(chunks.size()-1);
                progressUpload.setValue(latest);
            }

            @Override protected void done() {
                try {
                    String fileId = get();
                    JOptionPane.showMessageDialog(MainWindow.this, "Upload reussi\nFileId: " + fileId, "OK", JOptionPane.INFORMATION_MESSAGE);
                    // refresh list automatically
                    handleRefreshList();
                } catch (Exception e) {
                    JOptionPane.showMessageDialog(MainWindow.this, "Erreur upload: " + e.getMessage(), "Erreur", JOptionPane.ERROR_MESSAGE);
                } finally {
                    btnUpload.setEnabled(true);
                    btnChooseFile.setEnabled(true);
                    progressUpload.setValue(0);
                }
            }
        };
        w.execute();
    }

    private void handleRefreshList() {
        if (!isConnected) return;
        btnRefresh.setEnabled(false);

        SwingWorker<List<FileInfo>, Void> w = new SwingWorker<>() {
            @Override protected List<FileInfo> doInBackground() throws Exception {
                return client.listFiles();
            }
            @Override protected void done() {
                try {
                    List<FileInfo> files = get();
                    tableModel.setRowCount(0);
                    for (FileInfo f : files) {
                        tableModel.addRow(new Object[]{
                                f.getName(),
                                humanSize(f.getSize()),
                                f.getFragments(),
                                f.getDate(),
                                f.getFileId()
                        });
                    }
                    btnDownload.setEnabled(!files.isEmpty());
                } catch (Exception e) {
                    JOptionPane.showMessageDialog(MainWindow.this, "Erreur list: " + e.getMessage(), "Erreur", JOptionPane.ERROR_MESSAGE);
                } finally {
                    btnRefresh.setEnabled(true);
                }
            }
        };
        w.execute();
    }

    private void handleDownload() {
        int row = fileTable.getSelectedRow();
        if (row == -1) {
            JOptionPane.showMessageDialog(this, "Selectionnez un fichier", "Info", JOptionPane.INFORMATION_MESSAGE);
            return;
        }
        String fileId = (String) tableModel.getValueAt(row, 4);
        String name = (String) tableModel.getValueAt(row, 0);

        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("Enregistrer sous");
        chooser.setSelectedFile(new File(name));
        int r = chooser.showSaveDialog(this);
        if (r != JFileChooser.APPROVE_OPTION) return;
        File saveTo = chooser.getSelectedFile();

        btnDownload.setEnabled(false);
        progressDownload.setValue(0);

        SwingWorker<Void, Integer> w = new SwingWorker<>() {
            @Override protected Void doInBackground() throws Exception {
                client.downloadFile(fileId, saveTo.getAbsolutePath(), (sent, total) -> {
                    int pct = total > 0 ? (int) ((sent * 100) / total) : 0;
                    publish(pct);
                });
                return null;
            }
            @Override protected void process(List<Integer> chunks) {
                int latest = chunks.get(chunks.size()-1);
                progressDownload.setValue(latest);
            }
            @Override protected void done() {
                try {
                    get();
                    JOptionPane.showMessageDialog(MainWindow.this, "Download termine: " + saveTo.getAbsolutePath(), "OK", JOptionPane.INFORMATION_MESSAGE);
                } catch (Exception e) {
                    JOptionPane.showMessageDialog(MainWindow.this, "Erreur download: " + e.getMessage(), "Erreur", JOptionPane.ERROR_MESSAGE);
                } finally {
                    btnDownload.setEnabled(true);
                    progressDownload.setValue(0);
                }
            }
        };
        w.execute();
    }

    private static String humanSize(long b) {
        if (b < 1024) return b + " B";
        if (b < 1024*1024) return String.format("%.1f KB", b/1024.0);
        if (b < 1024L*1024*1024) return String.format("%.1f MB", b/(1024.0*1024));
        return String.format("%.2f GB", b/(1024.0*1024*1024));
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(MainWindow::new);
    }
}