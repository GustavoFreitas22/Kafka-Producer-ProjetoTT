package freitasDev.kafka.twitterApp;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	Logger log = LoggerFactory.getLogger(TwitterProducer.class.getName());

	public TwitterProducer() {

	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

	public void run() {
		log.info("Dando setup no sistema");
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		// Twitter client
		
		Client cliente = createTwitterClient(msgQueue);
		
		// Attempts to establish a connection.
		
		cliente.connect();

		// Twitter kafka producer

		KafkaProducer<String, String> producer = createKafka();

		// loop para enviar os tweets para o kafka
		while (!cliente.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (Exception e) {
				e.printStackTrace();
				cliente.stop();
			}
			if (msg != null) {
				log.info(msg);
				producer.send(new ProducerRecord<String, String>("Tweets_from_Animes", null, msg), new Callback() {

					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if (exception != null) {
							log.error("Deu ruim irmão", exception);
						}

					}
				});
			}

		}
		log.info("Fim de execução do sistema");
	}

	private KafkaProducer<String, String> createKafka() {
		String bootstrapServers = "127.0.0.1:9092";

		// Propriedades do produtor

		Properties properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Criando um producer seguro

		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // Basicamente garantindo que a
																					// mensagem não vai ser duplicada
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // pede retorno de todas as replicas, alem do lider
																	// de partição
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)); // quantidade de
																									// vezes que o
																									// re-envio vai ser
																									// tentado
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // serve para limitar a
																							// quantidade de
																							// solicitações no buffer do
																							// produtor
		
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // tipo de compressão
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // tempo de espera entre lotes
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // tamanho do lote (32kb)

		/*
		 * Os atributos de configuração do producer são todos constatntes, logo poddemos
		 * usar essa classe "ProducerConfig"
		 */

		// criamos o produtor

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		return producer;
	}

	private String consumerKey = "QU8fNaCrFjG61XwmpU5rusm24";
	private String consumerSecret = "9pJU78SYPBloJMccTCWqCkjUowrXbWxA7CGux6IBnBbGMsK9dQ";
	private String token = "1056140012708917248-lptM6FU2CLElqqh0FbD2AhnBxHgJlc";
	private String secret = "7f4t87WT1FJwgXSdoG78mZUzXW0EATTiIviJBd44fiAQE";
	List<String> terms = Lists.newArrayList("animes", "Valorant", "League of Legends", "lolzinho", "Programação");

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms

		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}

}
