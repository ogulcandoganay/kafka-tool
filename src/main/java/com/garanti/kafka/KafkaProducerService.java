package com.garanti.kafka;

import com.garanti.kafka.config.KafkaConfig;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class KafkaProducerService {

    private KafkaProducer<String, String> producer;
    private final Consumer<String> statusCallback;
    private final Gson gson;

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
            String key = UUID.randomUUID().toString();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);

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
                String key = UUID.randomUUID().toString();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);

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