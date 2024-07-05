import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;

public class Client {
    private static final String DEFAULT_MASTER_ADDRESS = "localhost"; // Default address of the master server
    private static final int CLIENT_LISTENING_PORT = 4999; // Port number of the master server

    public static void main(String[] args) {
        String defaultMasterAddress = DEFAULT_MASTER_ADDRESS;
        // Check if command-line arguments are provided to override the default address
        if (args.length > 0) {
            defaultMasterAddress = args[0];
        }

        try (Socket socket = new Socket(defaultMasterAddress, CLIENT_LISTENING_PORT); // Create socket connection to the master server
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream()); // Output stream to send data to the server
             DataInputStream dis = new DataInputStream(socket.getInputStream())) { // Input stream to receive data from the server

            System.out.println();
            System.out.println("Connected to Master Server. " + defaultMasterAddress + ":" + CLIENT_LISTENING_PORT);

            Scanner scanner = new Scanner(System.in); // Scanner to read user input
            System.out.print("Start point: ");
            int startPoint = scanner.nextInt(); // User-defined start point for prime number search
            System.out.print("End point: ");
            int endPoint = scanner.nextInt(); // User-defined end point for prime number search
            System.out.print("Number of threads: ");
            int nThreads = scanner.nextInt(); // Number of threads to be used for computation

            if (startPoint >= endPoint) { // Validate input: start point should be less than end point
                System.err.println("Start point should not be bigger than end point.");
                return;
            }

            long startTime = System.currentTimeMillis(); // Record start time of the computation

            // Send user inputs (start point, end point, number of threads) to the master server
            dos.writeInt(startPoint);
            dos.writeInt(endPoint);
            dos.writeInt(nThreads);

            System.out.println("Sent to Master Server.");
            System.out.println("Primes range: "+ startPoint + " to "+ endPoint);
            System.out.println("Threads: "+ nThreads);
            System.out.println("------------ Waiting for results ------------");

            String output = dis.readUTF(); // Receive the number of primes calculated by the master server

            long endTime = System.currentTimeMillis(); // Record end time of the computation

            System.out.println("Master Server responded with " + output);
            System.out.println("Time taken: " + (endTime - startTime) + " ms");
            System.out.println("-------------------- Fin --------------------");

        } catch (IOException e) { // Handle connection and I/O errors
            System.err.println("Could not connect to Master Server on " + defaultMasterAddress + ":" + CLIENT_LISTENING_PORT);
            e.printStackTrace();
        }
    }
}
