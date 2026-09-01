#include <Arduino.h>
#include "esp_camera.h"
#include "camera_init.h"

// Local live-view hardware check, no WiFi needed: continuously streams JPEG
// frames out over the same USB-serial link used for flashing, each framed as
// the ASCII marker "FRAME" + a 4-byte little-endian length + the raw JPEG
// bytes. Paired with tools/live_viewer.py on the host.

void setup() {
    Serial.begin(115200);
    initCamera();
}

void loop() {
    camera_fb_t* fb = esp_camera_fb_get();
    if (!fb) {
        return;
    }

    Serial.write((const uint8_t*)"FRAME", 5);
    uint32_t len = fb->len;
    Serial.write((const uint8_t*)&len, sizeof(len));
    Serial.write(fb->buf, fb->len);
    Serial.flush();

    esp_camera_fb_return(fb);
}
