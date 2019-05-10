package actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

/**
 * Typed Java Actor example
 * Doc:
 * https://doc.akka.io/docs/akka/2.5/typed-actors.html
 *
 */
public abstract class HelloWorldMain {

    public static void main(String[] args) {
        final ActorSystem<HelloWorldMain.Start> system =
                ActorSystem.create(HelloWorldMain.main, "HelloWorld");

        system.tell(new HelloWorldMain.Start("Worldx"));

        //TODO ask seems to be more complicated than in the untyped world
        //https://doc.akka.io/docs/akka/current/typed/interaction-patterns.html#request-response-with-ask-between-two-actors
    }


    private HelloWorldMain() {
    }

    public static class Start {
        public final String name;

        public Start(String name) {
            this.name = name;
        }
    }

    public static final Behavior<Start> main =
            Behaviors.setup(context -> {
                final ActorRef<HelloWorld.Greet> greeter =
                        context.spawn(HelloWorld.greeter, "greeter");

                return Behaviors.receiveMessage(message -> {
                    //Explicitly include the properly typed replyTo address in the message
                    ActorRef<HelloWorld.Greeted> replyTo =
                            context.spawn(HelloWorldBot.bot(0, 3), message.name);
                    greeter.tell(new HelloWorld.Greet(message.name, replyTo));
                    return Behaviors.same();
                });
            });
}