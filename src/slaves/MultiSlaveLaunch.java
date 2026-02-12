package slaves;

import java.io.IOException;

public class MultiSlaveLaunch {
    public static void main(String[] args) throws IOException {
        // Lancer Slave 1
        Thread slave1Thread = new Thread(() -> {
            try {
                Slave s1 = new Slave("1", "localhost", 5000, 5001, "./slave1_storage");
                s1.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        slave1Thread.setName("Slave-1");
        slave1Thread.start();

        // Lancer Slave 2
        Thread slave2Thread = new Thread(() -> {
            try {
                Slave s2 = new Slave("2", "localhost", 5000, 5002, "./slave2_storage");
                s2.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        slave2Thread.setName("Slave-2");
        slave2Thread.start();

        System.out.println("2 slaves lancés !");
    }
}