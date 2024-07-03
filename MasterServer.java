import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MasterServer {
    // Executor services for handling client and slave connections
    private static final ExecutorService CLIENT_EXECUTOR = Executors.newCachedThreadPool();
    private static final ExecutorService SLAVE_EXECUTOR = Executors.newCachedThreadPool();

    // List to keep track of connected slave computers
    private static final List<SlaveComputer> SLAVES = new CopyOnWriteArrayList<>();

    // Ports for client and slave connections
    private static final int CLIENT_PORT = 4999;
    private static final int SLAVE_LISTENING_PORT = 5001;

    public static void main(String[] args) {
        // Separate thread for listening to slave server connections
        Thread slaveListenerThread = new Thread(() -> slaveConnectionsListener());
        slaveListenerThread.start();

        // Listen for client connections
        try (ServerSocket serverSocket = new ServerSocket(CLIENT_PORT)) {
            System.out.println("Listening for CLIENT on port: " + CLIENT_PORT);

            // Handle each client connection in a separate thread
            while (true) {
                Socket clientSocket = serverSocket.accept();
                CLIENT_EXECUTOR.submit(() -> clientHandler(clientSocket));
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            CLIENT_EXECUTOR.shutdown();
            SLAVE_EXECUTOR.shutdown();
        }
    }

    // Function to handle slave connections
    private static void slaveConnectionsListener() {
        try (ServerSocket slaveListener = new ServerSocket(SLAVE_LISTENING_PORT)) {
            System.out.println("Listening for SLAVES on port: " + SLAVE_LISTENING_PORT);
            while (true) {
                Socket slaveSocket = slaveListener.accept();
                BufferedReader input = new BufferedReader(new InputStreamReader(slaveSocket.getInputStream()));
                String slaveAddress = slaveSocket.getInetAddress().getHostAddress();
                int slavePort = Integer.parseInt(input.readLine());

                // Create and add a new slave computer to the list
                SlaveComputer newSlave = new SlaveComputer(slaveAddress, slavePort);
                if(!SLAVES.contains(newSlave)){
                    System.out.println("NEW SLAVE - (Address,Port) (" + newSlave.getAddress() + "," + newSlave.getPort() + ")");
                    System.out.println("NUMBER OF SLAVES LISTENING: " + (SLAVES.size() + 1));
                    System.out.println("-------------------------------------");

                    SLAVES.add(newSlave);
                }

            }
        } catch (IOException ex) {
            System.err.println("Master Server encountered an error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    // Function to handle client requests
    private static void clientHandler(Socket clientSocket) {
        try {
            List<Future<?>> futures = new ArrayList<>();
            AtomicInteger totalPrimes = new AtomicInteger();
            DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
            DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());

            // Read input from client
            int startPoint = dis.readInt();
            int endPoint = dis.readInt();
            int nThreads = dis.readInt();

            if (startPoint % 2 == 0) {
                startPoint++;
            }

            int totalParticipants = SLAVES.size() + 1; // Including master

            if (SLAVES.isEmpty()) {
                System.out.println("No Slaves connected!");

                try {
                    if (nThreads <= 0) {
                        throw new IllegalArgumentException("Number of threads must be greater than 0.");
                    }
                    List<Integer> primes = new ArrayList<>();
                    Lock primesLock = new ReentrantLock();
                    ExecutorService executor = Executors.newFixedThreadPool(nThreads);

                    // Submit tasks to check for prime numbers
                    for (int num = startPoint; num <= endPoint; num += 2) {
                        executor.submit(new PrimeTask(num, primes, primesLock));
                    }
                    executor.shutdown();
                    while (!executor.isTerminated()) {
                        // Wait for all tasks to finish
                    }

                    int primeCount = primes.size();
                    if (startPoint < 3)
                        primeCount += 1; // Adjust for the case where 2 is a prime

                    dos.writeUTF("| Total Primes: " + primeCount);

                } catch (IllegalArgumentException e) {
                    System.err.println("Invalid input: " + e.getMessage());
                    e.printStackTrace();
                }

            } else {
                // Prepare master task
                ExecutorService masterExecutor = Executors.newFixedThreadPool(nThreads);
                List<Integer> primes = new ArrayList<>();
                Lock primesLock = new ReentrantLock();

                for (int num = startPoint; num <= endPoint; num += (2 * totalParticipants)) {
                    masterExecutor.submit(new PrimeTask(num, primes, primesLock));
                }

                // Prepare slave tasks
                for (int i = 0; i < SLAVES.size(); i++) {
                    SlaveComputer currentSlave = SLAVES.get(i);
                    int finalI = i + 1;
                    int finalStartPoint = startPoint;

                    futures.add(SLAVE_EXECUTOR.submit(() -> {
                        try (Socket slaveSocket = new Socket(currentSlave.getAddress(), currentSlave.getPort())) {
                            DataOutputStream slaveDos = new DataOutputStream(slaveSocket.getOutputStream());
                            DataInputStream slaveDis = new DataInputStream(slaveSocket.getInputStream());

                            slaveDos.writeInt(nThreads);
                            for (int num = finalStartPoint + (2 * finalI); num <= endPoint; num += (2 * totalParticipants)) {
                                slaveDos.writeInt(num);
                            }
                            slaveDos.writeInt(-1);

                            // Receive prime count from slave
                            totalPrimes.addAndGet(slaveDis.readInt());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }));
                }

                masterExecutor.shutdown();
                while (!masterExecutor.isTerminated()) {
                    // Wait for all master tasks to finish
                }

                totalPrimes.addAndGet(primes.size());

                // Wait for all slave futures to complete
                for (Future<?> future : futures) {
                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }

                dos.writeUTF("| Total Primes: " + totalPrimes.get());
            }

        } catch (IOException ex) {
            System.err.println("Error handling client: " + clientSocket);
            ex.printStackTrace();
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Class representing a slave computer
    static class SlaveComputer {
        private String address;
        private int port;

        public SlaveComputer(String address, int port) {
            this.address = address;
            this.port = port;
        }

        public String getAddress() {
            return address;
        }

        public int getPort() {
            return port;
        }
    }

    // Task to check if a number is prime
    static class PrimeTask implements Runnable {
        private final List<Integer> primes;
        private final Lock lock;
        private final int num;

        PrimeTask(int num, List<Integer> primes, Lock lock) {
            this.num = num;
            this.primes = primes;
            this.lock = lock;
        }

        @Override
        public void run() {
            if (isPrime(num)) {
                lock.lock();
                try {
                    primes.add(num);
                } finally {
                    lock.unlock();
                }
            }
        }

        // Check if a number is prime
        private boolean isPrime(int n) {
            if (n <= 1) return false;
            for (int i = 2; i * i <= n; i++) {
                if (n % i == 0) return false;
            }
            return true;
        }
    }
}
