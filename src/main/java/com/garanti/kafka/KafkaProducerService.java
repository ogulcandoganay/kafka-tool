package com.garanti.kafka;

import com.garanti.kafka.config.KafkaConfig;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaProducerService {

    private KafkaProducer<String, String> producer;
    private final Consumer<String> statusCallback;
    private final Gson gson;

    // Regex Pattern'ları Tanımla
    // #s(uzunluk) -> Rastgele String
    private static final Pattern STRING_PATTERN = Pattern.compile("#s\\((\\d+)\\)");
    // #i(min, max) -> Rastgele Integer
    private static final Pattern INT_PATTERN = Pattern.compile("#i\\((\\d+),\\s*(\\d+)\\)");
    // YENI: #b() -> Rastgele Boolean (true/false)
    private static final Pattern BOOL_PATTERN = Pattern.compile("#b\\(\\)");


    public KafkaProducerService(Consumer<String> statusCallback) {
        this.statusCallback = statusCallback;
        this.gson = new GsonBuilder().setPrettyPrinting().create();
    }

    public void initialize(KafkaConfig config) {
        try {
            producer = new KafkaProducer<>(config.getProducerProperties());
            statusCallback.accept("[PRODUCER HAZIR] Mesaj gönderilebilir\n");
        } catch (Exception e) {
            statusCallback.accept("[HATA] Producer başlatılamadı: " + e.getMessage() + "\n");
        }
    }

    public void sendMessage(String topic, String message) {
        if (producer == null) {
            statusCallback.accept("[HATA] Producer başlatılmamış!\n");
            return;
        }

        try {
            // Dinamik mesaj üretimi
            String dynamicMessage = applyFaker(message);

            String key = UUID.randomUUID().toString();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, dynamicMessage);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    statusCallback.accept("[GÖNDERIM HATASI] " + exception.getMessage() + "\n");
                } else {
                    String result = String.format("[GÖNDERİLDİ] %s | Topic: %s | Partition: %d | Offset: %d\n",
                            getCurrentTime(),
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset()
                    );
                    statusCallback.accept(result);
                }
            });

        } catch (Exception e) {
            statusCallback.accept("[HATA] " + e.getMessage() + "\n");
        }
    }

    public void sendBulkCustomMessages(String topic, String message, int count) {
        if (producer == null) {
            statusCallback.accept("[HATA] Producer başlatılmamış!\n");
            return;
        }

        long startTime = System.currentTimeMillis();
        AtomicInteger successCount = new AtomicInteger(0);

        try {
            for (int i = 0; i < count; i++) {
                // Dinamik mesaj üretimi her döngüde yenilenir
                String dynamicMessage = applyFaker(message);

                String key = UUID.randomUUID().toString();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, dynamicMessage);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        statusCallback.accept("[TOPLU HATA] Mesaj gönderilemedi: " + exception.getMessage() + "\n");
                    } else {
                        successCount.incrementAndGet();
                    }
                });
            }

            producer.flush();

        } catch (Exception e) {
            statusCallback.accept("[TOPLU HATA] İşlem sırasında beklenmedik hata: " + e.getMessage() + "\n");
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        double rate = (double) successCount.get() / (duration / 1000.0);
        statusCallback.accept(
                String.format("╔═════════════════════════════════════════════════════════════╗\n" +
                                "║ [TOPLU GÖNDERİM BİTTİ]                                      ║\n" +
                                "║ Toplam Mesaj: %-15d Gönderim Süresi: %-8d ms      ║\n" +
                                "║ Başarılı ACK: %-15d Gönderim Hızı:  %.2f msg/sn  ║\n" +
                                "╚═════════════════════════════════════════════════════════════╝\n",
                        count, duration, successCount.get(), rate)
        );
    }

    // GÜNCELLENEN METOT: Dinamik Veri Üretimi
    private String applyFaker(String message) {
        String result = message;

        // 1. Rastgele Integer üretimi (#i(min, max))
        // Sayı olduğu için tırnak işareti olmadan direkt JSON'a eklenir.
        Matcher intMatcher = INT_PATTERN.matcher(result);
        while (intMatcher.find()) {
            try {
                int min = Integer.parseInt(intMatcher.group(1));
                int max = Integer.parseInt(intMatcher.group(2));
                int randomInt = ThreadLocalRandom.current().nextInt(min, max + 1);
                result = result.replace(intMatcher.group(0), String.valueOf(randomInt));
                intMatcher = INT_PATTERN.matcher(result);
            } catch (NumberFormatException e) {
                // Hata durumunda etiketi olduğu gibi bırak
            }
        }

        // 2. Rastgele Boolean üretimi (#b())
        // Boolean olduğu için tırnak işareti olmadan direkt JSON'a eklenir.
        Matcher boolMatcher = BOOL_PATTERN.matcher(result);
        while (boolMatcher.find()) {
            boolean randomBool = ThreadLocalRandom.current().nextBoolean();
            // Boolean değeri JSON'a tırnaksız eklenir
            result = result.replace(boolMatcher.group(0), String.valueOf(randomBool));
            boolMatcher = BOOL_PATTERN.matcher(result);
        }

        // 3. Rastgele String üretimi (#s(uzunluk))
        // JSON formatına uygun olması için tırnak işaretleriyle ("...") sarılır.
        Matcher stringMatcher = STRING_PATTERN.matcher(result);
        while (stringMatcher.find()) {
            try {
                int length = Integer.parseInt(stringMatcher.group(1));
                String randomString = generateRandomString(length);
                // JSON değeri olduğu için string'i tırnak içine alıyoruz
                result = result.replace(stringMatcher.group(0), "\"" + randomString + "\"");
                stringMatcher = STRING_PATTERN.matcher(result);
            } catch (NumberFormatException e) {
                // Hata durumunda etiketi olduğu gibi bırak
            }
        }

        return result;
    }

    private String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = ThreadLocalRandom.current().nextInt(characters.length());
            sb.append(characters.charAt(index));
        }
        return sb.toString();
    }


    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
            statusCallback.accept("[PRODUCER KAPATILDI]\n");
        }
    }

    private String getCurrentTime() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
    }
}