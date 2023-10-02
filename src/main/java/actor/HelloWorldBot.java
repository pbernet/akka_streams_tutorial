package actor;

import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

public class HelloWorldBot extends AbstractBehavior<HelloWorld.Greeted> {

    public static Behavior<HelloWorld.Greeted> create(int max) {
        return Behaviors.setup(context -> new HelloWorldBot(context, max));
    }

    private final int max;
    private int greetingCounter;

    private HelloWorldBot(ActorContext<HelloWorld.Greeted> context, int max) {
        super(context);
        this.max = max;
    }

    @Override
    public Receive<HelloWorld.Greeted> createReceive() {
        return newReceiveBuilder().onMessage(HelloWorld.Greeted.class, this::onGreeted).build();
    }

    private Behavior<HelloWorld.Greeted> onGreeted(HelloWorld.Greeted message) {
        greetingCounter++;
        getContext().getLog().info("Greeting {} for {}", greetingCounter, message.whom);
        if (greetingCounter == max) {
            return Behaviors.stopped();
        } else {
            message.from.tell(new HelloWorld.Greet(message.whom, getContext().getSelf()));
            return this;
        }
    }
}
