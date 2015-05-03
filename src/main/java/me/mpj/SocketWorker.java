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

        String line;
        try {
            // readLine blocks until line arrives or socket closes, upon which it returns null
            while ((line = br.readLine()) != null) {

                // Handle: consume
                Pattern consumePattern = Pattern.compile("consume\\s([a-z]+)\\s([a-z]+)\\s([a-z]+)");
                Matcher consumeMatcher = consumePattern.matcher(line.trim());
                if (consumeMatcher.find()) {
                    final String topic = consumeMatcher.group(1);
                    final String group = consumeMatcher.group(2);
                    final KafkaConsumer.AutoOffsetReset reset = KafkaConsumer.AutoOffsetReset.valueOf(consumeMatcher.group(3));
                    _consumer = new KafkaConsumer(topic, group, reset);
                    sendLine("consume-started");
                }

                // Handle: next
                else if (line.trim().equals("next")){
                    final String msg = new String(_consumer.stream.iterator().next().message());
                    sendLine("msg " + msg);
                }

                // Handle: commit
                else if (line.trim().equals("commit")){
                    _consumer.commitOffsets();
                    sendLine("commit-ok");
                }

                else {
                    sendLine("command-invalid");
                }

            }
        } catch (IOException e) {
            System.out.println("SocketWorker error: Could not read line.");
            e.printStackTrace();
        }

        // If we reach this line, it means that the socket has
        // has closed, so shut down the consumer.
        _consumer.shutdown();

        // Finally, ensure that socket is closed.
        try {
            if (!_socket.isClosed()) _socket.close();
        } catch (IOException e) {
            // Can't imagine us caring about this error
        }

    }

    private void sendLine(String line) {
        try {
            Boolean autoFlush = true;
            PrintWriter out = new PrintWriter(_socket.getOutputStream(), autoFlush);
            out.println(line);
        } catch (IOException e) {
            System.out.println("SocketWorker Error: Unable to send message on socket");
            e.printStackTrace();
        }

    }
}
