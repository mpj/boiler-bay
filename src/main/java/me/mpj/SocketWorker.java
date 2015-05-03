package me.mpj;

import java.io.*;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SocketWorker implements Runnable {
    private Socket _socket;
    private KafkaConsumer _consumer = null;

    public SocketWorker(Socket socket) {
        _socket = socket;
    }

    @Override
    public void run() {
        final InputStream inputStream;
        try {
            inputStream = _socket.getInputStream();
        } catch (IOException e) {
            System.out.println("SocketWorker error: Could not get input stream.");
            e.printStackTrace();
            return;
        }
        final InputStreamReader streamReader = new InputStreamReader(inputStream);
        BufferedReader br = new BufferedReader(streamReader);
        // readLine blocks until line arrives or socket closes, upon which it returns null
        String line;
        try {
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                consume(line);
                next(line);
            }
        } catch (IOException e) {
            System.out.println("SocketWorker error: Could not read line.");
            e.printStackTrace();
        }

        System.out.println("Shutting down thread.");
        _consumer.shutdown();

        try {
            if (!_socket.isClosed()) _socket.close();
        } catch (IOException e) {
            // Can't imagine us caring about this error
        }

    }

    private void consume(String line) {
        // consume
        Pattern p = Pattern.compile("consume\\s([a-z]+)\\s([a-z]+)");
        Matcher m = p.matcher(line.trim());

        if (m.find()) {
            String topic = m.group(1);
            String group = m.group(2);
            _consumer = new KafkaConsumer(topic, group);
            System.out.println("Consumer initialized");
        }
    }

    private void next(String line) {
        if (line.trim().equals("next")){
            try {
                // .hasNext is blocking and is really only used to detect when the
                // consumer is shutdown so that we can exit cleanly. Don't know why the consumer would shut down?

                final String msg = new String(_consumer.stream.iterator().next().message());
                Boolean autoFlush = true;
                PrintWriter out =
                        new PrintWriter(_socket.getOutputStream(), autoFlush);
                out.println("msg " + msg);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
