import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * 多线程编程，随机打印生僻字
 * 涉及到多线程，定时任务，socket编程
 */
public class SocketThread {
    public static void main(String[] args) {
        System.out.println("我的第一个java程序，随机打印生僻字。");
        Thread server = new ThreadServer();
        server.setDaemon(true);
        server.start();
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
        ses.scheduleWithFixedDelay(new ThreadClient(), 2, 3, TimeUnit.SECONDS);
//        ses.shutdown();
    }
}


class ThreadServer extends Thread {
    @Override
    public void run() {
        try {
            ServerSocket ss = new ServerSocket(6666);
            System.out.println("server is running...");
            for (;;) {
                try {
                    Socket s = ss.accept();
                    System.out.println("[server] connect from " + s.getRemoteSocketAddress());
                    Thread t = new ThreadHandler(s);
                    t.start();
                } catch (IOException e) {
                    e.printStackTrace();
                    ss.close();
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


class ThreadHandler extends Thread {
    private Socket s;

    ThreadHandler(Socket s) {
        this.s = s;
    }

    @Override
    public void run() {
        try (InputStream input = this.s.getInputStream()) {
            try (OutputStream output = this.s.getOutputStream()) {
                handle(input, output);
            }
        } catch (Exception e) {
            try {
                this.s.close();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
//            System.out.println("client disconnected.");
        }
    }

    private void handle(InputStream input, OutputStream output) {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
        BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        for (;;) {
            try {
                String s = reader.readLine();
                writer.write("get: " + s + "\n");
                writer.flush();
            } catch (IOException e) {
//                e.printStackTrace();
                break;
            }
        }
    }
}


class ThreadClient implements Runnable {
    @Override
    public void run() {
        try {
            Socket ss = new Socket("localhost", 6666);
            System.out.println("[client] connect to " + ss.getRemoteSocketAddress());
            try (InputStream input = ss.getInputStream()) {
                try (OutputStream output = ss.getOutputStream()) {
                    handle(input, output);
                }
            }
            ss.close();
//            System.out.println("disconnected.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void handle(InputStream input, OutputStream output) {
        BufferedWriter w = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
        BufferedReader r = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        Random ran = new Random();
        try {
            w.write(ran.nextInt());
            w.newLine();
            w.flush();
            String rp = r.readLine();
            System.out.println("[client] " + rp + "\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
