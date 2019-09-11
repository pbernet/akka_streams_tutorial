package actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

/**
 * Akka Typed example 1:1
 * Doc:
 * https://doc.akka.io/docs/akka/2.6/typed/actors.html
 *
 */
public class HelloWorldMain extends AbstractBehavior<HelloWorldMain.Start> {

    public static void main(String[] args) {
        final ActorSystem<HelloWorldMain.Start> system =
                ActorSystem.create(HelloWorldMain.create(), "hello");

        system.tell(new HelloWorldMain.Start("World"));
        //system.tell(new HelloWorldMain.Start("Akka"));

        //TODO implement ask seems to be more complicated than in the untyped world
        //https://doc.akka.io/docs/akka/current/typed/interaction-patterns.html#request-response-with-ask-between-two-actors
    }


    public static class Start {
        public final String name;

        public Start(String name) {
            this.name = name;
        }
    }

    public static Behavior<Start> create() {
        return Behaviors.setup(HelloWorldMain::new);
    }

    private final ActorContext<Start>        context;
    private final ActorRef<HelloWorld.Greet> greeter;

    private HelloWorldMain(ActorContext<Start> context) {
        this.context = context;
        greeter = context.spawn(HelloWorld.create(), "greeter");
    }

    @Override
    public Receive<Start> createReceive() {
        return newReceiveBuilder().onMessage(Start.class, this::onStart).build();
    }

    private Behavior<Start> onStart(Start command) {
        ActorRef<HelloWorld.Greeted> replyTo = context.spawn(HelloWorldBot.create(3), command.name);
        greeter.tell(new HelloWorld.Greet(command.name, replyTo));
        return this;
    }
}