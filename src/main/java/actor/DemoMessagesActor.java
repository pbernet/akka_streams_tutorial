package actor;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;

import java.time.Duration;
import java.util.concurrent.CompletionStage;


/**
 * Untyped Java Actor example
 * Doc:
 * https://doc.akka.io/docs/akka/2.5.23/actors.html
 * https://www.baeldung.com/java-completablefuture
 *
 */
public class DemoMessagesActor extends AbstractLoggingActor {

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("DemoMessagesActor");
        ActorRef demoActor = system.actorOf(DemoMessagesActor.props(42), "demo");

        //Tell: Fire and forget
        demoActor.tell(new GreetingTell("Hi tell"), ActorRef.noSender());

        //Ask: Wait for answer
        final CompletionStage<Object> future = Patterns.ask(demoActor, new GreetingAsk("Hi ask"), Duration.ofMillis(1000));
        future.thenAccept(result -> System.out.println("Actor returned: " + result));
    }

    static public class GreetingTell {
        private final String from;

        public GreetingTell(String from) {
            this.from = from;
        }

        public String getGreeter() {
            return from;
        }
    }

    static public class GreetingAsk {
        private final String from;

        public GreetingAsk(String from) {
            this.from = from;
        }

        public String getGreeter() {
            return from;
        }
    }

    /**
     * Create Props for an actor of this type.
     * @param magicNumber The magic number to be passed to this actor’s constructor.
     * @return a Props for creating this actor, which can then be further configured
     *         (e.g. calling `.withDispatcher()` on it)
     */
    static Props props(Integer magicNumber) {
        // You need to specify the actual type of the returned actor
        // since Java 8 lambdas have some runtime type information erased
        return Props.create(DemoMessagesActor.class, () -> new DemoMessagesActor(magicNumber));
    }

    private final Integer magicNumber;

    public DemoMessagesActor(Integer magicNumber) {
        this.magicNumber = magicNumber;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(GreetingTell.class, g -> {
                    log().info("I was greeted by {}", g.getGreeter());
                })
                .match(GreetingAsk.class, g -> {
                    log().info("I was greeted by {}", g.getGreeter());
                    getSender().tell("OK", getSelf());
                })
                .build();
    }
}
