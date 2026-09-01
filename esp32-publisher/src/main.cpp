#include <Arduino.h>
#include <WiFi.h>
#include "esp_camera.h"
#include "camera_init.h"
#include "wifi_secrets.h"

// Must match TOPIC_LENGTH in src/messaging.h and the topic src/subscriber.cpp
// subscribes to.
#define TOPIC_LENGTH 12
static const char* TOPIC = "/abc";

WiFiClient client;

bool writeExact(WiFiClient& c, const uint8_t* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
        int n = c.write(buf + total, len - total);
        if (n <= 0) {
            if (!c.connected()) return false;
            delay(1);
            continue;
        }
        total += n;
    }
    return true;
}

// Wire format matches src/messaging.h: 1-byte msgType, 12-byte topic,
// 4-byte little-endian msgLen, then msgLen payload bytes.
bool publishFrame(camera_fb_t* fb) {
    char msgType = 'P';
    char topic[TOPIC_LENGTH] = {0};
    strncpy(topic, TOPIC, TOPIC_LENGTH - 1);
    int32_t msgLen = fb->len;

    if (!writeExact(client, (const uint8_t*)&msgType, 1)) return false;
    if (!writeExact(client, (const uint8_t*)topic, sizeof(topic))) return false;
    if (!writeExact(client, (const uint8_t*)&msgLen, sizeof(msgLen))) return false;
    if (!writeExact(client, fb->buf, fb->len)) return false;
    return true;
}

void connectWiFi() {
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    Serial.print("Connecting to WiFi");
    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.print(".");
    }
    Serial.println();
    Serial.print("WiFi connected, IP: ");
    Serial.println(WiFi.localIP());
}

bool connectManager() {
    Serial.printf("Connecting to manager at %s:%d\n", MANAGER_HOST, MANAGER_PORT);
    if (!client.connect(MANAGER_HOST, MANAGER_PORT)) {
        Serial.println("Connection to manager failed");
        return false;
    }
    Serial.println("Connected to manager");
    return true;
}

void setup() {
    Serial.begin(115200);

    initCamera();
    connectWiFi();
    connectManager();
}

void loop() {
    if (!client.connected()) {
        Serial.println("Manager connection lost, reconnecting...");
        client.stop();
        if (!connectManager()) {
            delay(2000);
            return;
        }
    }

    camera_fb_t* fb = esp_camera_fb_get();
    if (!fb) {
        Serial.println("Camera capture failed");
        delay(1000);
        return;
    }

    if (!publishFrame(fb)) {
        Serial.println("Publish failed, will reconnect");
        client.stop();
    } else {
        Serial.printf("Published frame (%u bytes)\n", fb->len);
    }

    esp_camera_fb_return(fb);

    delay(1000);
}
