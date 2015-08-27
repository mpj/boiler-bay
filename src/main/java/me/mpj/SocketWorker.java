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

        sendLine("ready");

        String line;
        try {
            // readLine blocks until line arrives or socket closes, upon which it returns null
            // readLine will be return each line up until a line feed / carriage return,
            // but will not including the line-termination character.
            while ((line = br.readLine()) != null) {

                // Handle: consume <topic> <group> <offset-reset>
                if (line.startsWith("consume")) {
                    Pattern pattern = Pattern.compile("consume\\s([a-zA-Z0-9_\\-]+)\\s([a-zA-Z0-9_\\-]+)\\s(smallest|largest)");
                    Matcher matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        final String topic = matcher.group(1);
                        final String group = matcher.group(2);
                        final KafkaConsumer.AutoOffsetReset reset =
                                KafkaConsumer.AutoOffsetReset.valueOf(matcher.group(3));
                        _consumer = new KafkaConsumer(topic, group, reset);
                        sendLine("consume-started");

                        final String msg = new String(_consumer.stream.iterator().next().message());
                        sendLine("msg " + msg);
                    } else {
                        sendLine("error command-invalid The consume command expects three parameters separated by "+
                                 "space; topic (alphanumerical/dash), consumer group (alphanumerical/dash), and offset reset ('smallest'/'largest')");
                    }
                }

                // Handle: ack
                else if (line.trim().equals("ack")){
                    if(_consumer != null) {

                        // Committing on every message - with newer Kafka versions, offsets
                        // are stored in Kafka, not Zookeeper like before, so consumes
                        // are less expensive nowadays. It's still a bit expensive,
                        // about 170 msgs/s vs 446msgs/s when not calling commit at all,
                        // but not too bad.
                        
                        // This line will block until offsets are confirmed, see
                        // http://stackoverflow.com/questions/30013116/does-commitoffsets-on-high-level-consumer-block
                        _consumer.commitOffsets();

                        final String msg = new String(_consumer.stream.iterator().next().message());
                        sendLine("msg " + msg);
                    } else {
                        sendLine("error consume-not-started You need to start consuming and wait " +
                                 "for consume-started before sending ack");
                    }
                }


                // Handle: send <topic> <partitionId> <body>
                else if (line.startsWith("send") ) {
                    Pattern pattern = Pattern.compile("send\\s([a-zA-Z0-9_\\-]+)\\s(\\w+)\\s(.+)");
                    Matcher matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        final String topic = matcher.group(1);
                        final String partitionId = matcher.group(2);
                        final String body = matcher.group(3);
                        if (_producer == null)
                            _producer = new KafkaProducer();
                        try {
                            _producer.send(topic, partitionId, body);
                        } catch (ExecutionException e) {
                            sendLine("send-fail exception " + e.getMessage());
                        } catch (InterruptedException e) {
                            sendLine("send-fail interrupted" + e.getMessage());
                        }
                    } else {
                        sendLine("error command-invalid The send command expects three parameters separated by "+
                                "space; topic (a-z), partitionId (letter, number, underscore) and and body (any string).");
                    }
                }
                else {
                    sendLine("command-invalid");
                }

            }
        } catch (IOException e) {

            if (e.getMessage().toLowerCase().contains("connection reset")) {
                // Happens during normal operation, when clients forcibly disconnect, ignore
                return;
            }

            System.out.println("SocketWorker error: Could not read line.");
            e.printStackTrace();
        } finally {
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
