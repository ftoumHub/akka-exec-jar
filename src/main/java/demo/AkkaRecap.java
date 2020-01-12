package demo;

import static io.vavr.API.println;

import akka.NotUsed;
import akka.actor.*;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.Timeout;
import io.vavr.control.Option;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class AkkaRecap {

    public static void main(String[] args) {

        Option<String> keystore = Option.some(new File(AkkaRecap.class.getResource("/certificats/keystore.jks").getPath()).getPath());
        Option<String> truststore = Option.some(new File(AkkaRecap.class.getResource("/certificats/truststore.jks").getPath()).getPath());

        println(keystore.get());
        println(truststore.get());

        ActorSystem system = ActorSystem.create("IntegratingWithActors");
        ActorMaterializer mat = ActorMaterializer.create(system);

        ActorRef simpleActor = system.actorOf(Props.create(SimpleActor.class), "simpleActor");

        // On définit une source d'entier qu'on va passer à notre simpleActor
        final Source<Integer, NotUsed> numbersSource = Source.range(1, 10);

        // On va utiliser notre acteur sous la forme d'un Flow utilisant la méthode ask pour requêter
        // l'acteur (envoi d'un message et attente d'une réponse sous la forme d'une Future avec timeout)
        // Le facteur de parallélisation définit le nb de messages possibles dans la mailbox avant
        // que l'acteur déclenche le mécanisme de backpressure
        // Etant donnée que les futures peuvent retourner n'import quel type, on indique le type
        // qui nous intéresse, ici Integer
        final Flow<Integer, Integer, NotUsed> actorBasedFlow =
                Flow.<Integer>create().ask(
                        4,
                        simpleActor,
                        Integer.class,
                        new Timeout(2, TimeUnit.SECONDS));

        numbersSource.via(actorBasedFlow).to(Sink.ignore()).run(mat);
        // equivalent à :
        //numbersSource.ask(4, simpleActor, Integer.class, new Timeout(2, TimeUnit.SECONDS));
    }

    private static class SimpleActor extends AbstractLoggingActor {

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, s -> {
                        log().info("Just received a string " + s);
                        //sender().tell();
                    })
                    .match(Integer.class, n -> {
                        log().info("Just received an integer " + n);
                        getSender().tell(n, getSelf());
                    })
                    .matchAny(__ -> println(__))
                    .build();
        }

    }
}

