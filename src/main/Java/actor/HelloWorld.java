package actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

public abstract class HelloWorld {

    //no instances of this class, it's only a name space for messages
    // and static methods
    private HelloWorld() {
    }


    public static final class Greet {
        public final String whom;
        public final ActorRef<Greeted> replyTo;

        public Greet(String whom, ActorRef<Greeted> replyTo) {
            this.whom = whom;
            this.replyTo = replyTo;
        }
    }

    public static final class Greeted {
        public final String whom;
        public final ActorRef<Greet> from;

        public Greeted(String whom, ActorRef<Greet> from) {
            this.whom = whom;
            this.from = from;
        }
    }

    public static final Behavior<Greet> greeter = Behaviors.receive((context, message) -> {
        context.getLog().info("Hello {}!", message.whom);
        message.replyTo.tell(new Greeted(message.whom, context.getSelf()));
        return Behaviors.same();
    });
}