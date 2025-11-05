package com.garanti.kafka;

import com.garanti.kafka.config.KafkaConfig;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class KafkaToolApp extends JFrame {

    private JTextField bootstrapServerField;
    private JTextField topicField;
    private JTextField groupIdField;
    private JCheckBox kerberosCheckBox;
    private JTextField truststoreLocationField;
    private JPasswordField truststorePasswordField;

    private JTable messageTable;
    private DefaultTableModel tableModel;
    private JTextArea statusArea;

    private JButton startConsumerBtn;
    private JButton stopConsumerBtn;

    private JTextField bulkCountField;
    private JButton sendBulkCustomBtn;

    private JTextField customMessageField;
    private JButton sendCustomBtn;

    private KafkaConsumerService consumerService;
    private KafkaProducerService producerService;

    public KafkaToolApp() {
        setTitle("Garanti BBVA - Kafka Tool");
        setSize(1200, 750);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout(10, 10));

        initComponents();
        initServices();

        setLocationRelativeTo(null);
    }

    private void initComponents() {
        JPanel configPanel = new JPanel(new GridBagLayout());
        configPanel.setBorder(BorderFactory.createTitledBorder("Kafka ve Güvenlik Ayarları"));
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.fill = GridBagConstraints.HORIZONTAL;

        gbc.gridx = 0; gbc.gridy = 0; gbc.weightx = 0;
        configPanel.add(new JLabel("Bootstrap Servers:"), gbc);
        gbc.gridx = 1; gbc.weightx = 1.0;
        bootstrapServerField = new JTextField("localhost:9092");
        configPanel.add(bootstrapServerField, gbc);

        gbc.gridx = 0; gbc.gridy = 1; gbc.weightx = 0;
        configPanel.add(new JLabel("Topic:"), gbc);
        gbc.gridx = 1; gbc.weightx = 1.0;
        topicField = new JTextField("test-topic");
        configPanel.add(topicField, gbc);

        gbc.gridx = 0; gbc.gridy = 2; gbc.weightx = 0;
        configPanel.add(new JLabel("Group ID:"), gbc);
        gbc.gridx = 1; gbc.weightx = 1.0;
        groupIdField = new JTextField("kafka-tool-group");
        configPanel.add(groupIdField, gbc);

        gbc.gridx = 0; gbc.gridy = 3; gbc.gridwidth = 2;
        kerberosCheckBox = new JCheckBox("Kerberos/SSL Authentication Kullan (JAAS/JKS Gerekli)");
        configPanel.add(kerberosCheckBox, gbc);
        kerberosCheckBox.addActionListener(e -> toggleKerberosFields(kerberosCheckBox.isSelected()));

        gbc.gridx = 0; gbc.gridy = 4; gbc.gridwidth = 1; gbc.weightx = 0;
        configPanel.add(new JLabel("Truststore (JKS) Yolu:"), gbc);
        gbc.gridx = 1; gbc.weightx = 1.0;
        truststoreLocationField = new JTextField();
        truststoreLocationField.setToolTipText("Örn: C:\\kerberos\\kafka.server.truststore.jks");
        configPanel.add(truststoreLocationField, gbc);

        gbc.gridx = 0; gbc.gridy = 5; gbc.weightx = 0;
        configPanel.add(new JLabel("Truststore Şifresi:"), gbc);
        gbc.gridx = 1; gbc.weightx = 1.0;
        truststorePasswordField = new JPasswordField();
        configPanel.add(truststorePasswordField, gbc);

        toggleKerberosFields(false);

        add(configPanel, BorderLayout.NORTH);

        JPanel centerPanel = new JPanel(new BorderLayout(5, 5));
        centerPanel.setBorder(BorderFactory.createTitledBorder("Mesajlar"));

        String[] columnNames = {"ZAMAN", "PARTITION", "OFFSET", "KEY", "VALUE (JSON)"};
        tableModel = new DefaultTableModel(columnNames, 0) {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false;
            }
        };

        messageTable = new JTable(tableModel);
        messageTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        messageTable.setFont(new Font("SansSerif", Font.PLAIN, 12));

        messageTable.getColumnModel().getColumn(0).setPreferredWidth(60);
        messageTable.getColumnModel().getColumn(1).setPreferredWidth(60);
        messageTable.getColumnModel().getColumn(2).setPreferredWidth(80);
        messageTable.getColumnModel().getColumn(3).setPreferredWidth(150);
        messageTable.getColumnModel().getColumn(4).setPreferredWidth(800);

        JScrollPane messageScrollPane = new JScrollPane(messageTable);

        statusArea = new JTextArea(4, 1);
        statusArea.setEditable(false);
        statusArea.setFont(new Font("Consolas", Font.PLAIN, 12));
        statusArea.setBackground(new Color(40, 44, 52));
        statusArea.setForeground(new Color(255, 255, 255));
        statusArea.setCaretColor(Color.WHITE);
        JScrollPane statusScrollPane = new JScrollPane(statusArea);
        statusScrollPane.setBorder(BorderFactory.createTitledBorder("Durum Logları"));

        JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, messageScrollPane, statusScrollPane);
        splitPane.setResizeWeight(0.7);
        splitPane.setOneTouchExpandable(true);

        centerPanel.add(splitPane, BorderLayout.CENTER);
        add(centerPanel, BorderLayout.CENTER);


        JPanel controlPanel = new JPanel(new BorderLayout(5, 5));
        controlPanel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));

        JPanel consumerPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        consumerPanel.setBorder(BorderFactory.createTitledBorder("Consumer"));

        startConsumerBtn = new JButton("▶ Consume Başlat");
        startConsumerBtn.setBackground(new Color(76, 175, 80));
        startConsumerBtn.setForeground(Color.BLACK);
        startConsumerBtn.setFocusPainted(false);
        startConsumerBtn.addActionListener(e -> startConsumer());

        stopConsumerBtn = new JButton("⏹ Durdur");
        stopConsumerBtn.setBackground(new Color(244, 67, 54));
        stopConsumerBtn.setForeground(Color.BLACK);
        stopConsumerBtn.setFocusPainted(false);
        stopConsumerBtn.setEnabled(false);
        stopConsumerBtn.addActionListener(e -> stopConsumer());

        consumerPanel.add(startConsumerBtn);
        consumerPanel.add(stopConsumerBtn);

        JPanel producerPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        producerPanel.setBorder(BorderFactory.createTitledBorder("Producer"));

        customMessageField = new JTextField(30);
        customMessageField.setToolTipText("JSON mesaj girin");

        sendCustomBtn = new JButton("📤 Tek Gönder");
        sendCustomBtn.setBackground(new Color(156, 39, 176));
        sendCustomBtn.setForeground(Color.BLACK);
        sendCustomBtn.setFocusPainted(false);
        sendCustomBtn.addActionListener(e -> sendCustomMessage());

        producerPanel.add(new JLabel("Mesaj (JSON):"));
        producerPanel.add(customMessageField);
        producerPanel.add(sendCustomBtn);

        sendBulkCustomBtn = new JButton("🚀   Toplu Gönder");
        sendBulkCustomBtn.setBackground(new Color(255, 152, 0));
        sendBulkCustomBtn.setForeground(Color.BLACK);
        sendBulkCustomBtn.setFocusPainted(false);
        sendBulkCustomBtn.addActionListener(e -> sendBulkCustomMessageWrapper());

        bulkCountField = new JTextField("1000", 6);
        bulkCountField.setToolTipText("Gönderilecek Adet");

        producerPanel.add(new JLabel(" | Adet:"));
        producerPanel.add(bulkCountField);
        producerPanel.add(sendBulkCustomBtn);

        JButton clearBtn = new JButton("🗑 Temizle");
        clearBtn.addActionListener(e -> clearOutput());
        producerPanel.add(clearBtn);

        JPanel buttonsPanel = new JPanel(new GridLayout(2, 1, 5, 5));
        buttonsPanel.add(consumerPanel);
        buttonsPanel.add(producerPanel);

        controlPanel.add(buttonsPanel, BorderLayout.CENTER);
        add(controlPanel, BorderLayout.SOUTH);
    }

    private void toggleKerberosFields(boolean enable) {
        truststoreLocationField.setEnabled(enable);
        truststorePasswordField.setEnabled(enable);
    }

    private void initServices() {
        consumerService = new KafkaConsumerService(this::appendStatus, this::appendRowToTable);
        producerService = new KafkaProducerService(this::appendStatus);
    }

    private void startConsumer() {

        clearOutput();

        String bootstrapServers = bootstrapServerField.getText().trim();
        String topic = topicField.getText().trim();
        String groupId = groupIdField.getText().trim();
        boolean useKerberos = kerberosCheckBox.isSelected();

        String truststoreLocation = truststoreLocationField.getText().trim();
        String truststorePassword = new String(truststorePasswordField.getPassword()).trim();

        if (bootstrapServers.isEmpty() || topic.isEmpty()) {
            JOptionPane.showMessageDialog(this,
                    "Bootstrap servers ve topic alanları doldurulmalı!",
                    "Hata", JOptionPane.ERROR_MESSAGE);
            return;
        }

        if (useKerberos && (truststoreLocation.isEmpty() || truststorePassword.isEmpty())) {
            JOptionPane.showMessageDialog(this,
                    "Kerberos/SSL seçili. Truststore Yolu ve Şifresi zorunludur!",
                    "Güvenlik Hatası", JOptionPane.ERROR_MESSAGE);
            return;
        }

        KafkaConfig config = new KafkaConfig(
                bootstrapServers,
                groupId,
                useKerberos,
                truststoreLocation,
                truststorePassword
        );

        consumerService.start(config, topic);
        producerService.initialize(config);

        startConsumerBtn.setEnabled(false);
        stopConsumerBtn.setEnabled(true);
        bootstrapServerField.setEnabled(false);
        topicField.setEnabled(false);
        groupIdField.setEnabled(false);
        kerberosCheckBox.setEnabled(false);
        truststoreLocationField.setEnabled(false);
        truststorePasswordField.setEnabled(false);
    }

    private void stopConsumer() {
        consumerService.stop();
        producerService.close();

        startConsumerBtn.setEnabled(true);
        stopConsumerBtn.setEnabled(false);
        bootstrapServerField.setEnabled(true);
        topicField.setEnabled(true);
        groupIdField.setEnabled(true);
        kerberosCheckBox.setEnabled(true);
        toggleKerberosFields(kerberosCheckBox.isSelected());
    }

    private void sendCustomMessage() {
        String topic = topicField.getText().trim();
        String message = customMessageField.getText().trim();

        if (topic.isEmpty() || message.isEmpty()) {
            appendStatus("[HATA] Topic ve mesaj doldurulmalı!\n");
            return;
        }

        new Thread(() -> {
            producerService.sendMessage(topic, message);
        }).start();
    }

    private void sendBulkCustomMessageWrapper() {
        String topic = topicField.getText().trim();
        String message = customMessageField.getText().trim();

        if (topic.isEmpty() || message.isEmpty()) {
            appendStatus("[HATA] Topic ve mesaj doldurulmalı!\n");
            return;
        }

        int count;
        try {
            count = Integer.parseInt(bulkCountField.getText().trim());
            if (count <= 0) throw new NumberFormatException();
        } catch (NumberFormatException ex) {
            appendStatus("[HATA] Geçerli bir adet sayısı girin (örn: 10000)!\n");
            return;
        }

        new Thread(() -> {
            appendStatus(String.format("[TOPLU BAŞLANGIÇ] %d adet custom mesaj gönderiliyor...\n", count));
            producerService.sendBulkCustomMessages(topic, message, count);
        }).start();
    }

    private void appendRowToTable(Object[] row) {
        if (row == null) return;

        SwingUtilities.invokeLater(() -> {
            if (row != null && tableModel != null) {
                tableModel.insertRow(0, row);
            }
        });
    }

    private void appendStatus(String text) {
        SwingUtilities.invokeLater(() -> {
            statusArea.append(text);
            statusArea.setCaretPosition(statusArea.getDocument().getLength());
        });
    }

    private String getCurrentTime() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
    }

    // 🔴 DÜZELTME: Kayıp metot eklendi!
    private void clearOutput() {
        SwingUtilities.invokeLater(() -> {
            tableModel.setRowCount(0);
            statusArea.setText("");
        });
    }

    public static void main(String[] args) {
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception e) {
            e.printStackTrace();
        }

        SwingUtilities.invokeLater(() -> {
            KafkaToolApp app = new KafkaToolApp();
            app.setVisible(true);
        });
    }
}