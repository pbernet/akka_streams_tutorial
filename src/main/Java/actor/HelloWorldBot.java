package actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

public abstract class HelloWorldBot {
    private HelloWorldBot() {
    }

    public static final Behavior<HelloWorld.Greeted> bot(int greetingCounter, int max) {
        return Behaviors.receive((context, message) -> {
            int n = greetingCounter + 1;
            context.getLog().info("Greeting {} for {}", n, message.whom);
            if (n == max) {
                return Behaviors.stopped();
            } else {
                message.from.tell(new HelloWorld.Greet(message.whom, context.getSelf()));
                return bot(n, max);
            }
        });
    }
}
