package me.mpj;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SocketWorker implements Runnable {
    private Socket _socket;
    private KafkaConsumer _consumer = null;
    private KafkaProducer _producer = null;

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
            // readLine will be return each line up until a line feed / carriage return,
            // but will not including the line-termination character.
            while ((line = br.readLine()) != null) {

                Pattern sendPattern = Pattern.compile("send\\s(.+)\\s(.+)\\s(.+)");
                Matcher sendMatcher = sendPattern.matcher(line);

                // Handle: consume <topic> <group> <offset-reset>
                if (line.startsWith("consume")) {
                    Pattern pattern = Pattern.compile("consume\\s([a-z]+)\\s([a-z]+)\\s(smallest|largest)");
                    Matcher matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        final String topic = matcher.group(1);
                        final String group = matcher.group(2);
                        final KafkaConsumer.AutoOffsetReset reset =
                                KafkaConsumer.AutoOffsetReset.valueOf(matcher.group(3));
                        _consumer = new KafkaConsumer(topic, group, reset);
                        sendLine("consume-started");
                    } else {
                        sendLine("error command-invalid THe consume command expects three parameters separated by "+
                                 "space; topic (a-z), consumer group (a-z), and offset reset ('smallest'/'largest')");
                    }
                }

                // Handle: next
                else if (line.trim().equals("next")){
                    if(_consumer != null) {
                        final String msg = new String(_consumer.stream.iterator().next().message());
                        sendLine("msg " + msg);
                    } else {
                        sendLine("error consume-not-started You need to start consuming and wait " +
                                 "for consume-started before sending next");
                    }

                }

                // Handle: commit
                else if (line.trim().equals("commit")){
                    _consumer.commitOffsets();
                    sendLine("commit-ok");
                }

                // Handle: send <partitionId> <body>
                else if (sendMatcher.find()) {
                    final String topic       = sendMatcher.group(1);
                    final String partitionId = sendMatcher.group(2);
                    final String body        = sendMatcher.group(3);
                    if(_producer == null)
                        _producer = new KafkaProducer();
                    try {
                        _producer.send(topic, partitionId, body);
                    } catch (ExecutionException e) {
                        sendLine("send-fail exception " + e.getMessage());
                    } catch (InterruptedException e) {
                        sendLine("send-fail interrupted" + e.getMessage());
                    }
                }

                else {
                    sendLine("command-invalid");
                }

            }
        } catch (IOException e) {

            if (e.getMessage() == "Connection Reset") {
                // Happens during normal operation, when clients forcibly disconnect, ignore
                return;
            }

            System.out.println("SocketWorker error: Could not read line.");
            e.printStackTrace();
        }

        // If we reach this line, it means that the socket has
        // has closed, so shut down any consumer or producer
        if (_consumer != null)
            _consumer.shutdown();
        if (_producer != null)
            _producer.shutdown();

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
