package sample.stream;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Tcp;
import akka.stream.javadsl.Tcp.IncomingConnection;
import akka.stream.javadsl.Tcp.ServerBinding;
import akka.util.ByteString;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;

public class TcpEchoJava {

    /**
     * Use without parameters to start both server and 10 clients.
     *
     * Use parameters `server 0.0.0.0 6001` to start server listening on port
     * 6001.
     *
     * Use parameters `client 127.0.0.1 6001` to start client connecting to server
     * on 127.0.0.1:6001.
     *
     */
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            ActorSystem system = ActorSystem.create("ClientAndServer");
            InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 6000);
            server(system, serverAddress);
            IntStream.range(1, 10).parallel().forEach(each -> client(system, serverAddress));
        } else {
            InetSocketAddress serverAddress;
            if (args.length == 3) {
                serverAddress = new InetSocketAddress(args[1], Integer.valueOf(args[2]));
            } else {
                serverAddress = new InetSocketAddress("127.0.0.1", 6000);
            }
            if (args[0].equals("server")) {
                ActorSystem system = ActorSystem.create("Server");
                server(system, serverAddress);
            } else if (args[0].equals("client")) {
                ActorSystem system = ActorSystem.create("Client");
                client(system, serverAddress);
            }
        }
    }

    private static void server(ActorSystem system, InetSocketAddress serverAddress) {
        final ActorMaterializer materializer = ActorMaterializer.create(system);

        final Sink<IncomingConnection, CompletionStage<Done>> handler = Sink.foreach(conn -> {
            System.out.println("Client connected from: " + conn.remoteAddress());
            conn.handleWith(Flow.<ByteString>create(), materializer);
        });


        final CompletionStage<ServerBinding> bindingFuture =
                Tcp.get(system).bind(serverAddress.getHostString(), serverAddress.getPort()).to(handler).run(materializer);

        bindingFuture.handle((ServerBinding binding, Throwable exception) -> {
            if (binding != null) {
                System.out.println("Server started, listening on: " + binding.localAddress());
            } else {
                System.err.println("Server could not bind to " + serverAddress + " : " + exception.getMessage());
                system.terminate();
            }
            return NotUsed.getInstance();
        });

    }

    private static void client(ActorSystem system, InetSocketAddress serverAddress) {
        final ActorMaterializer materializer = ActorMaterializer.create(system);

        final List<ByteString> testInput = new ArrayList<>();
        for (char c = 'a'; c <= 'z'; c++) {
            testInput.add(ByteString.fromString(String.valueOf(c)));
        }

        Source<ByteString, NotUsed> responseStream =
                Source.from(testInput).via(Tcp.get(system).outgoingConnection(serverAddress.getHostString(), serverAddress.getPort()));

        CompletionStage<ByteString> result = responseStream.runFold(
                ByteString.empty(), ByteString::concat, materializer);

        result.handle((success, failure) -> {
            if (failure != null) {
                System.err.println("Failure: " + failure.getMessage());
            } else {
                System.out.println("Result: " + success.utf8String());
            }
            System.out.println("Shutting down client");
            system.terminate();
            return NotUsed.getInstance();
        });
    }
}
