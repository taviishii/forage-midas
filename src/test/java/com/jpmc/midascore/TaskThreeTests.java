package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Optional;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskThreeTests {

    static final Logger logger = LoggerFactory.getLogger(TaskThreeTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private UserRepository userRepository;

    @Test
    void task_three_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");

        ObjectMapper mapper = new ObjectMapper();

        for (String transactionLine : transactionLines) {
            try {
                Transaction transaction = mapper.readValue(transactionLine, Transaction.class);
                kafkaProducer.send(transaction);
            } catch (Exception e) {
                logger.error("❌ Failed to parse transaction: " + transactionLine, e);
            }
        }

        // Give Kafka time to process all transactions
        Thread.sleep(3000);

        // 🔍 Look up Waldorf and log their balance
        Optional<UserRecord> waldorfOpt = userRepository.findAll()
                .stream()
                .filter(u -> "waldorf".equalsIgnoreCase(u.getName()))
                .findFirst();

        logger.info("----------------------------------------------------------");
        logger.info("🔍 Waldorf's Final Balance:");

        if (waldorfOpt.isPresent()) {
            UserRecord waldorf = waldorfOpt.get();
            int finalBalance = (int) waldorf.getBalance(); // Rounded down
            logger.info("💰 Balance: " + finalBalance);
        } else {
            logger.warn("❌ Could not find user 'waldorf'");
        }

        logger.info("----------------------------------------------------------");

        // Optional: halt here for inspection
        while (true) {
            Thread.sleep(20000);
            logger.info("...");
        }
    }
}
