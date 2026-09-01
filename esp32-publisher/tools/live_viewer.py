#!/usr/bin/env python3
"""Live viewer for the esp32cam_serial_test firmware.

Reads JPEG frames from the ESP32-CAM's USB-serial link, each framed as:
    b"FRAME" + 4-byte little-endian length + raw JPEG bytes
and displays them in a live-updating window. Press 'q' to quit.
"""

import struct
import sys

import cv2
import numpy as np
import serial

PORT = "/dev/cu.usbserial-AD9OE90Y"
BAUD = 115200
MAGIC = b"FRAME"
MAX_FRAME_BYTES = 2_000_000


def find_magic(ser):
    window = bytearray()
    while True:
        byte = ser.read(1)
        if not byte:
            continue
        window += byte
        if len(window) > len(MAGIC):
            del window[0]
        if window == bytearray(MAGIC):
            return


def read_exact(ser, n):
    data = bytearray()
    while len(data) < n:
        chunk = ser.read(n - len(data))
        if not chunk:
            return None
        data.extend(chunk)
    return bytes(data)


def main():
    port = sys.argv[1] if len(sys.argv) > 1 else PORT
    ser = serial.Serial(port, BAUD, timeout=2)
    print(f"Connected to {port} at {BAUD} baud. Press 'q' in the video window to quit.")

    try:
        while True:
            find_magic(ser)

            len_bytes = read_exact(ser, 4)
            if len_bytes is None:
                continue
            (length,) = struct.unpack("<I", len_bytes)
            if length == 0 or length > MAX_FRAME_BYTES:
                continue

            jpeg_bytes = read_exact(ser, length)
            if jpeg_bytes is None:
                continue

            frame = cv2.imdecode(np.frombuffer(jpeg_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
            if frame is None:
                continue

            cv2.imshow("ESP32-CAM Live", frame)
            if cv2.waitKey(1) & 0xFF == ord("q"):
                break
    finally:
        ser.close()
        cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
