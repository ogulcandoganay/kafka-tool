package com.garanti.kafka;

import com.garanti.kafka.config.KafkaConfig;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.function.Consumer;

public class KafkaConsumerService {

    private KafkaConsumer<String, String> consumer;
    private volatile boolean running = false;
    private Thread consumerThread;

    private final Consumer<String> statusCallback;
    private final Consumer<Object[]> rowCallback;
    private final Gson prettyGson;

    public KafkaConsumerService(Consumer<String> statusCallback, Consumer<Object[]> rowCallback) {
        this.statusCallback = statusCallback;
        this.rowCallback = rowCallback;
        this.prettyGson = new GsonBuilder().setPrettyPrinting().create();
    }

    public void start(KafkaConfig config, String topic) {
        if (running) {
            statusCallback.accept("[HATA] Consumer zaten çalışıyor!\n");
            return;
        }

        try {
            consumer = new KafkaConsumer<>(config.getConsumerProperties());
            consumer.subscribe(Collections.singletonList(topic));
            running = true;

            consumerThread = new Thread(() -> {
                statusCallback.accept(String.format("[BAŞLATILDI] Topic: %s - %s\n",
                        topic, getCurrentTime()));
                statusCallback.accept("─".repeat(60) + "\n");

                try {
                    while (running) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                        for (ConsumerRecord<String, String> record : records) {
                            processAndAddRow(record);
                        }
                    }
                } catch (org.apache.kafka.common.errors.WakeupException we) {
                    if (!running) {
                        statusCallback.accept("[BİLGİ] Consumer uykudan uyandırıldı ve durduruluyor.\n");
                    } else {
                        statusCallback.accept("[HATA] Beklenmedik WakeupException: " + we.getMessage() + "\n");
                    }
                } catch (Exception e) {
                    if (running) {
                        statusCallback.accept("[HATA] Beklenmedik Hata: " + e.getMessage() + "\n");
                    }
                } finally {
                    if (consumer != null) {
                        consumer.close();
                        statusCallback.accept("[BİLGİ] Consumer nesnesi temizlendi.\n");
                    }
                }
            });

            consumerThread.start();

        } catch (Exception e) {
            statusCallback.accept("[BAĞLANTI HATASI] " + e.getMessage() + "\n");
            running = false;
        }
    }

    public void stop() {
        if (running) {
            running = false;

            if (consumer != null) {
                consumer.wakeup();
            }

            try {
                if (consumerThread != null && consumerThread.isAlive()) {
                    consumerThread.join(500);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                statusCallback.accept("[DURDURMA HATASI] Consumer thread kesintiye uğradı.\n");
            }

            statusCallback.accept("\n[DURDURULDU] Consumer kapatıldı - " + getCurrentTime() + "\n");
        }
    }

    private void processAndAddRow(ConsumerRecord<String, String> record) {

        // 💡 KRİTİK GÜVENLİK KONTROLÜ
        // Eğer mesaj değeri yoksa (boş/null) veya veri temizlenemiyorsa loga düşsün ama tabloya eklenmesin.
        if (record.value() == null || record.value().trim().isEmpty()) {
            statusCallback.accept(String.format("[UYARI] Boş mesaj atlandı. P: %d, O: %d\n",
                    record.partition(), record.offset()));
            return;
        }

        // Mesaj değerinin sadece bir kısmını alalım, tabloyu kilitlemesin.
        String key = record.key() != null ? record.key() : "null";
        String prettyValue = formatJson(record.value());

        // Tabloya eklenecek veri
        Object[] rowData = new Object[] {
                getCurrentTime(),
                record.partition(),
                record.offset(),
                key,
                // JSON string'i, tek satırda görünmesi için yeni satır karakterlerini kaldırıyoruz
                prettyValue != null ? prettyValue.replace("\n", " ") : "null"
        };

        rowCallback.accept(rowData);
    }

    private String formatJson(String value) {
        if (value == null || value.isEmpty()) return value;
        if (value.startsWith("{") || value.startsWith("[")) {
            try {
                Object json = prettyGson.fromJson(value, Object.class);
                return prettyGson.toJson(json);
            } catch (Exception e) {
                return value;
            }
        }
        return value;
    }

    private String getCurrentTime() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
    }

    public boolean isRunning() {
        return running;
    }
}