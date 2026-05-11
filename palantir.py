#!/usr/bin/env python3
"""
PALANTIR — Surveillance & Intelligence Module for SYSWATCH V5
Handles: network presence, device fingerprinting, news, weather, markets, sports
All data collection is passive and non-invasive.
"""

import subprocess, threading, time, json, re, sqlite3, os
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError

DB_PATH = os.path.expanduser("~/.syswatch_palantir.db")

# ── MAC VENDOR PREFIX DATABASE (top manufacturers) ─────────────────────────
MAC_VENDORS = {
    # Apple (iPhones, iPads, Macs)
    "00:03:93": "Apple", "00:0a:27": "Apple", "00:0a:95": "Apple",
    "00:1b:63": "Apple", "00:1c:b3": "Apple", "00:1d:4f": "Apple",
    "00:1e:52": "Apple", "00:1e:c2": "Apple", "00:1f:5b": "Apple",
    "00:1f:f3": "Apple", "00:21:e9": "Apple", "00:22:41": "Apple",
    "00:23:12": "Apple", "00:23:32": "Apple", "00:23:6c": "Apple",
    "00:24:36": "Apple", "00:25:00": "Apple", "00:25:4b": "Apple",
    "00:25:bc": "Apple", "00:26:08": "Apple", "00:26:4a": "Apple",
    "00:26:b0": "Apple", "00:26:bb": "Apple", "00:30:65": "Apple",
    "00:3e:e1": "Apple", "00:50:e4": "Apple", "00:56:cd": "Apple",
    "00:61:71": "Apple", "00:6d:52": "Apple", "00:88:65": "Apple",
    "00:f4:b9": "Apple", "04:0c:ce": "Apple", "04:15:52": "Apple",
    "04:1e:64": "Apple", "04:26:65": "Apple", "04:48:9a": "Apple",
    "04:4b:ed": "Apple", "04:52:f3": "Apple", "04:54:53": "Apple",
    "04:69:f8": "Apple", "04:d3:cf": "Apple", "04:db:56": "Apple",
    "04:e5:36": "Apple", "04:f1:3e": "Apple", "04:f7:e4": "Apple",
    "08:00:07": "Apple", "08:6d:41": "Apple", "08:70:45": "Apple",
    "08:74:02": "Apple", "08:f4:ab": "Apple", "0c:1d:cf": "Apple",
    "0c:3e:9f": "Apple", "0c:4d:e9": "Apple", "0c:51:01": "Apple",
    "0c:74:c2": "Apple", "0c:77:1a": "Apple", "0c:bc:9f": "Apple",
    "0c:d7:46": "Apple", "10:1c:0c": "Apple", "10:40:f3": "Apple",
    "10:41:7f": "Apple", "10:93:e9": "Apple", "10:9a:dd": "Apple",
    "10:dd:b1": "Apple", "14:10:9f": "Apple", "14:5a:05": "Apple",
    "14:8f:c6": "Apple", "14:99:e2": "Apple", "18:20:32": "Apple",
    "18:34:51": "Apple", "18:65:90": "Apple", "18:9e:fc": "Apple",
    "18:af:61": "Apple", "18:e7:f4": "Apple", "18:f6:43": "Apple",
    "1c:1a:c0": "Apple", "1c:36:bb": "Apple", "1c:5c:f2": "Apple",
    "1c:91:48": "Apple", "1c:ab:a7": "Apple", "20:78:f0": "Apple",
    "20:9b:cd": "Apple", "20:a2:e4": "Apple", "20:ab:37": "Apple",
    "20:c9:d0": "Apple", "24:1e:eb": "Apple", "24:5b:a7": "Apple",
    "24:a0:74": "Apple", "24:ab:81": "Apple", "28:0b:5c": "Apple",
    "28:37:37": "Apple", "28:5a:eb": "Apple", "28:6a:ba": "Apple",
    "28:a0:2b": "Apple", "28:cf:da": "Apple", "28:cf:e9": "Apple",
    "28:e0:2c": "Apple", "28:e1:4c": "Apple", "28:e7:cf": "Apple",
    "28:ed:6a": "Apple", "28:f0:76": "Apple", "2c:20:0b": "Apple",
    "2c:33:61": "Apple", "2c:b4:3a": "Apple", "2c:be:08": "Apple",
    "2c:f0:a2": "Apple", "30:10:e4": "Apple", "30:35:ad": "Apple",
    "30:90:ab": "Apple", "30:f7:c5": "Apple", "34:08:bc": "Apple",
    "34:36:3b": "Apple", "34:51:c9": "Apple", "34:a3:95": "Apple",
    "34:c0:59": "Apple", "38:0f:4a": "Apple", "38:48:4c": "Apple",
    "38:b5:4d": "Apple", "38:c9:86": "Apple", "3c:07:54": "Apple",
    "3c:15:c2": "Apple", "3c:2e:f9": "Apple", "3c:d0:f8": "Apple",
    "40:30:04": "Apple", "40:40:a7": "Apple", "40:6c:8f": "Apple",
    "40:83:de": "Apple", "40:a6:d9": "Apple", "40:b3:95": "Apple",
    "40:cb:c0": "Apple", "40:d3:2d": "Apple", "44:00:10": "Apple",
    "44:2a:60": "Apple", "44:4c:0c": "Apple", "44:d8:84": "Apple",
    "44:fb:42": "Apple", "48:43:7c": "Apple", "48:60:bc": "Apple",
    "48:74:6e": "Apple", "48:a1:95": "Apple", "48:bf:6b": "Apple",
    "48:d7:05": "Apple", "4c:57:ca": "Apple", "4c:74:03": "Apple",
    "4c:7c:5f": "Apple", "4c:8d:79": "Apple", "50:32:37": "Apple",
    "50:7a:55": "Apple", "50:82:d5": "Apple", "50:bc:96": "Apple",
    "50:de:06": "Apple", "50:ea:d6": "Apple", "54:26:96": "Apple",
    "54:4e:90": "Apple", "54:72:4f": "Apple", "54:99:63": "Apple",
    "54:ae:27": "Apple", "54:e4:3a": "Apple", "58:1f:aa": "Apple",
    "58:40:4e": "Apple", "58:55:ca": "Apple", "58:7f:57": "Apple",
    "58:b0:35": "Apple", "58:e2:8f": "Apple", "5c:1d:d9": "Apple",
    "5c:59:48": "Apple", "5c:95:ae": "Apple", "5c:97:f3": "Apple",
    "5c:f7:e6": "Apple", "60:03:08": "Apple", "60:33:4b": "Apple",
    "60:69:44": "Apple", "60:8c:4a": "Apple", "60:92:17": "Apple",
    "60:9a:c1": "Apple", "60:c5:47": "Apple", "60:d9:c7": "Apple",
    "60:f4:45": "Apple", "60:fb:42": "Apple", "60:fe:c5": "Apple",
    "64:20:0c": "Apple", "64:76:ba": "Apple", "64:9a:be": "Apple",
    "64:a3:cb": "Apple", "64:b9:e8": "Apple", "68:09:27": "Apple",
    "68:5b:35": "Apple", "68:64:4b": "Apple", "68:9c:70": "Apple",
    "68:a8:6d": "Apple", "68:ab:1e": "Apple", "68:d9:3c": "Apple",
    "68:fb:7e": "Apple", "6c:19:c0": "Apple", "6c:3e:6d": "Apple",
    "6c:40:08": "Apple", "6c:4d:73": "Apple", "6c:70:9f": "Apple",
    "6c:72:e7": "Apple", "6c:8d:c1": "Apple", "6c:94:f8": "Apple",
    "6c:ab:31": "Apple", "6c:c2:6b": "Apple", "70:11:24": "Apple",
    "70:14:a6": "Apple", "70:3e:ac": "Apple", "70:48:0f": "Apple",
    "70:56:81": "Apple", "70:73:cb": "Apple", "70:81:eb": "Apple",
    "70:cd:60": "Apple", "70:de:e2": "Apple", "70:ec:e4": "Apple",
    "74:1b:b2": "Apple", "74:8d:08": "Apple", "74:e1:b6": "Apple",
    "74:e2:f5": "Apple", "78:31:c1": "Apple", "78:4f:43": "Apple",
    "78:67:d7": "Apple", "78:6c:1c": "Apple", "78:7b:8a": "Apple",
    "78:88:6d": "Apple", "78:9f:70": "Apple", "78:a3:e4": "Apple",
    "78:ca:39": "Apple", "78:d7:5f": "Apple", "78:fd:94": "Apple",
    "7c:01:91": "Apple", "7c:11:be": "Apple", "7c:6d:62": "Apple",
    "7c:c3:a1": "Apple", "7c:d1:c3": "Apple", "7c:f9:54": "Apple",
    "80:00:6e": "Apple", "80:49:71": "Apple", "80:82:23": "Apple",
    "80:92:9f": "Apple", "80:be:05": "Apple", "80:d6:05": "Apple",
    "80:e6:50": "Apple", "84:29:99": "Apple", "84:38:35": "Apple",
    "84:78:8b": "Apple", "84:85:06": "Apple", "84:89:ad": "Apple",
    "84:b1:53": "Apple", "84:fc:ac": "Apple", "88:1f:a1": "Apple",
    "88:53:95": "Apple", "88:63:df": "Apple", "88:66:a5": "Apple",
    "88:ae:07": "Apple", "88:c6:63": "Apple", "88:cb:87": "Apple",
    "88:e8:7f": "Apple", "8c:00:6d": "Apple", "8c:29:37": "Apple",
    "8c:2d:aa": "Apple", "8c:58:77": "Apple", "8c:7b:9d": "Apple",
    "8c:7c:92": "Apple", "8c:85:90": "Apple", "8c:8d:28": "Apple",
    "8c:fa:ba": "Apple", "90:1b:0e": "Apple", "90:27:e4": "Apple",
    "90:3c:92": "Apple", "90:60:f1": "Apple", "90:72:40": "Apple",
    "90:8d:6c": "Apple", "90:b0:ed": "Apple", "90:b9:31": "Apple",
    "90:c1:c6": "Apple", "90:fd:61": "Apple", "94:94:26": "Apple",
    "94:bf:2d": "Apple", "94:e9:6a": "Apple", "94:f6:a3": "Apple",
    "98:01:a7": "Apple", "98:03:d8": "Apple", "98:10:e8": "Apple",
    "98:9e:63": "Apple", "98:b8:e3": "Apple", "98:d6:bb": "Apple",
    "98:e0:d9": "Apple", "98:f0:ab": "Apple", "9c:04:eb": "Apple",
    "9c:20:7b": "Apple", "9c:29:76": "Apple", "9c:35:eb": "Apple",
    "9c:4f:da": "Apple", "9c:84:bf": "Apple", "9c:f3:87": "Apple",
    "a0:18:28": "Apple", "a0:3b:e3": "Apple", "a0:4e:a7": "Apple",
    "a0:99:9b": "Apple", "a0:d7:95": "Apple", "a0:ed:cd": "Apple",
    "a4:5e:60": "Apple", "a4:b1:97": "Apple", "a4:b8:05": "Apple",
    "a4:c3:61": "Apple", "a4:d1:8c": "Apple", "a4:d9:31": "Apple",
    "a8:20:66": "Apple", "a8:5b:4f": "Apple", "a8:5c:2c": "Apple",
    "a8:60:b6": "Apple", "a8:66:7f": "Apple", "a8:86:dd": "Apple",
    "a8:8e:24": "Apple", "a8:96:8a": "Apple", "a8:bb:cf": "Apple",
    "a8:be:27": "Apple", "a8:fa:d8": "Apple", "ac:1f:74": "Apple",
    "ac:29:3a": "Apple", "ac:3c:0b": "Apple", "ac:61:ea": "Apple",
    "ac:7f:3e": "Apple", "ac:87:a3": "Apple", "ac:bc:32": "Apple",
    "ac:cf:5c": "Apple", "ac:de:48": "Apple", "ac:e4:b5": "Apple",
    "ac:fd:ec": "Apple", "b0:19:c6": "Apple", "b0:34:95": "Apple",
    "b0:65:bd": "Apple", "b0:70:2d": "Apple", "b0:9f:ba": "Apple",
    "b0:be:83": "Apple", "b4:18:d1": "Apple", "b4:4b:d2": "Apple",
    "b4:8b:19": "Apple", "b4:f0:ab": "Apple", "b8:09:8a": "Apple",
    "b8:17:c2": "Apple", "b8:41:a4": "Apple", "b8:53:ac": "Apple",
    "b8:63:4d": "Apple", "b8:8d:12": "Apple", "b8:c1:11": "Apple",
    "b8:e8:56": "Apple", "b8:f6:b1": "Apple", "bc:3b:af": "Apple",
    "bc:52:b7": "Apple", "bc:54:2f": "Apple", "bc:67:78": "Apple",
    "bc:6c:21": "Apple", "bc:92:6b": "Apple", "bc:9f:ef": "Apple",
    "bc:a9:20": "Apple", "bc:ec:a1": "Apple", "c0:1a:da": "Apple",
    "c0:63:94": "Apple", "c0:84:7a": "Apple", "c0:9f:42": "Apple",
    "c0:ce:cd": "Apple", "c0:d0:12": "Apple", "c4:2c:03": "Apple",
    "c4:61:8b": "Apple", "c4:b3:01": "Apple", "c8:1e:e7": "Apple",
    "c8:2a:14": "Apple", "c8:33:4b": "Apple", "c8:3c:85": "Apple",
    "c8:6f:1d": "Apple", "c8:85:50": "Apple", "c8:b5:b7": "Apple",
    "c8:bc:c8": "Apple", "c8:d0:83": "Apple", "c8:e0:eb": "Apple",
    "c8:f6:50": "Apple", "cc:08:8d": "Apple", "cc:20:e8": "Apple",
    "cc:25:ef": "Apple", "cc:29:f5": "Apple", "cc:44:63": "Apple",
    "cc:78:5f": "Apple", "cc:c7:60": "Apple", "d0:03:4b": "Apple",
    "d0:23:db": "Apple", "d0:33:11": "Apple", "d0:4f:7e": "Apple",
    "d0:81:7a": "Apple", "d0:a6:37": "Apple", "d0:c5:f3": "Apple",
    "d4:61:9d": "Apple", "d4:9a:20": "Apple", "d4:dc:cd": "Apple",
    "d4:f4:6f": "Apple", "d8:00:4d": "Apple", "d8:1d:72": "Apple",
    "d8:30:62": "Apple", "d8:96:95": "Apple", "d8:9e:3f": "Apple",
    "d8:a2:5e": "Apple", "d8:bb:c1": "Apple", "d8:cf:9c": "Apple",
    "dc:0c:5c": "Apple", "dc:2b:2a": "Apple", "dc:37:14": "Apple",
    "dc:41:5f": "Apple", "dc:86:d8": "Apple", "dc:9b:9c": "Apple",
    "dc:a4:ca": "Apple", "dc:d3:21": "Apple", "e0:5f:45": "Apple",
    "e0:66:78": "Apple", "e0:ac:cb": "Apple", "e0:b5:5f": "Apple",
    "e0:c7:67": "Apple", "e0:f5:c6": "Apple", "e4:25:e7": "Apple",
    "e4:8b:7f": "Apple", "e4:9a:dc": "Apple", "e4:b2:fb": "Apple",
    "e4:ce:8f": "Apple", "e4:e0:a6": "Apple", "e8:04:0b": "Apple",
    "e8:06:88": "Apple", "e8:80:2e": "Apple", "e8:b2:ac": "Apple",
    "ec:35:86": "Apple", "ec:85:2f": "Apple", "ec:ad:b8": "Apple",
    "f0:18:98": "Apple", "f0:24:75": "Apple", "f0:2f:4b": "Apple",
    "f0:79:60": "Apple", "f0:b4:79": "Apple", "f0:c1:f1": "Apple",
    "f0:cb:a1": "Apple", "f0:d1:a9": "Apple", "f0:db:e2": "Apple",
    "f0:dc:e2": "Apple", "f0:f6:1c": "Apple", "f4:0f:24": "Apple",
    "f4:1b:a1": "Apple", "f4:31:59": "Apple", "f4:37:b7": "Apple",
    "f4:5c:89": "Apple", "f4:f1:5a": "Apple", "f8:03:77": "Apple",
    "f8:1e:df": "Apple", "f8:27:93": "Apple", "f8:38:80": "Apple",
    "f8:62:14": "Apple", "f8:87:f1": "Apple", "f8:e9:4e": "Apple",
    "fc:25:3f": "Apple", "fc:e9:98": "Apple",
    # Samsung
    "00:07:ab": "Samsung", "00:12:47": "Samsung", "00:15:b9": "Samsung",
    "00:17:c9": "Samsung", "00:1a:8a": "Samsung", "00:1d:25": "Samsung",
    "00:1e:7d": "Samsung", "00:21:19": "Samsung", "00:23:39": "Samsung",
    "00:24:54": "Samsung", "00:26:37": "Samsung", "08:08:c2": "Samsung",
    "08:d4:2b": "Samsung", "08:ec:a9": "Samsung", "0c:14:20": "Samsung",
    "10:1d:c0": "Samsung", "10:30:47": "Samsung", "10:d5:42": "Samsung",
    "14:49:e0": "Samsung", "18:3f:47": "Samsung", "1c:62:b8": "Samsung",
    "20:13:e0": "Samsung", "20:d3:90": "Samsung", "24:4b:81": "Samsung",
    "28:39:26": "Samsung", "2c:ae:2b": "Samsung", "30:19:66": "Samsung",
    "34:14:5f": "Samsung", "34:be:00": "Samsung", "38:16:d1": "Samsung",
    "3c:5a:37": "Samsung", "40:0e:85": "Samsung", "44:f4:59": "Samsung",
    "48:13:7e": "Samsung", "4c:3c:16": "Samsung", "4c:bc:a5": "Samsung",
    "50:01:bb": "Samsung", "50:85:69": "Samsung", "50:cc:f8": "Samsung",
    "54:92:be": "Samsung", "54:bd:79": "Samsung", "58:ef:68": "Samsung",
    "5c:3c:27": "Samsung", "5c:49:7d": "Samsung", "5c:a3:9d": "Samsung",
    "60:a1:0a": "Samsung", "60:d0:a9": "Samsung", "64:77:91": "Samsung",
    "68:27:37": "Samsung", "6c:2f:2c": "Samsung", "6c:83:36": "Samsung",
    "70:f9:27": "Samsung", "74:45:8a": "Samsung", "78:1f:db": "Samsung",
    "7c:64:56": "Samsung", "84:25:db": "Samsung", "84:a4:66": "Samsung",
    "88:32:9b": "Samsung", "8c:71:f8": "Samsung", "90:18:7c": "Samsung",
    "94:35:0a": "Samsung", "94:76:b7": "Samsung", "98:52:b1": "Samsung",
    "98:aa:fc": "Samsung", "9c:02:98": "Samsung", "a0:07:98": "Samsung",
    "a0:82:1f": "Samsung", "a4:eb:d3": "Samsung", "a8:06:00": "Samsung",
    "ac:36:13": "Samsung", "b0:df:3a": "Samsung", "b4:07:f9": "Samsung",
    "b4:3a:28": "Samsung", "b8:5a:73": "Samsung", "bc:14:85": "Samsung",
    "bc:44:86": "Samsung", "c0:bd:d1": "Samsung", "c4:42:02": "Samsung",
    "c4:88:e5": "Samsung", "c8:14:79": "Samsung", "cc:07:ab": "Samsung",
    "d0:17:6a": "Samsung", "d0:59:e4": "Samsung", "d4:ae:05": "Samsung",
    "d8:57:ef": "Samsung", "dc:71:96": "Samsung", "e0:99:71": "Samsung",
    "e4:40:e2": "Samsung", "e4:92:fb": "Samsung", "e8:50:8b": "Samsung",
    "ec:9b:f3": "Samsung", "f0:25:b7": "Samsung", "f4:09:d8": "Samsung",
    "f8:04:2e": "Samsung", "fc:a1:3e": "Samsung",
    # Google Pixel
    "00:1a:11": "Google", "08:9e:08": "Google", "1c:f2:9a": "Google",
    "20:df:b9": "Google", "48:d6:d5": "Google", "54:60:09": "Google",
    "94:eb:2c": "Google", "a4:77:33": "Google", "f4:f5:d8": "Google",
    # Ubiquiti (routers/APs)
    "00:15:6d": "Ubiquiti", "00:27:22": "Ubiquiti", "04:18:d6": "Ubiquiti",
    "0c:80:63": "Ubiquiti", "18:e8:29": "Ubiquiti", "24:a4:3c": "Ubiquiti",
    "44:d9:e7": "Ubiquiti", "4c:e1:73": "Ubiquiti", "60:22:32": "Ubiquiti",
    "68:72:51": "Ubiquiti", "70:a7:41": "Ubiquiti", "74:83:c2": "Ubiquiti",
    "78:8a:20": "Ubiquiti", "80:2a:a8": "Ubiquiti", "ac:8b:a9": "Ubiquiti",
    "b4:fb:e4": "Ubiquiti", "dc:9f:db": "Ubiquiti", "e0:63:da": "Ubiquiti",
    "f0:9f:c2": "Ubiquiti", "f4:92:bf": "Ubiquiti", "fc:ec:da": "Ubiquiti",
    # TCL/Roku
    "00:17:88": "Roku", "08:05:81": "Roku", "b0:a7:37": "Roku",
    "b8:3e:59": "Roku", "cc:a1:2b": "TCL/Roku", "d4:e2:2f": "Roku",
    "dc:3a:5e": "Roku", "f0:5c:77": "Roku",
    # HP laptops
    "00:0f:61": "HP", "00:11:0a": "HP", "00:12:79": "HP", "00:13:21": "HP",
    "00:14:38": "HP", "00:15:60": "HP", "00:16:35": "HP", "00:17:08": "HP",
    "00:18:71": "HP", "00:19:bb": "HP", "00:1a:4b": "HP", "00:1b:78": "HP",
    "00:1c:c4": "HP", "00:1d:b3": "HP", "00:1e:0b": "HP", "00:1f:29": "HP",
    "00:21:5a": "HP", "00:22:64": "HP", "00:23:7d": "HP", "00:24:81": "HP",
    "00:25:b3": "HP", "00:26:55": "HP", "1c:c1:de": "HP", "28:92:4a": "HP",
    "30:e3:a4": "HP", "3c:d9:2b": "HP", "40:b0:34": "HP", "54:04:a6": "HP",
    "58:20:b1": "HP", "5c:b9:01": "HP", "68:b5:99": "HP", "6c:c2:17": "HP",
    "70:5a:ac": "HP", "78:48:59": "HP", "80:c1:6e": "HP", "84:34:97": "HP",
    "90:1b:0e": "HP", "98:e7:f4": "HP", "a0:b3:cc": "HP", "b4:99:ba": "HP",
    "b8:ca:3a": "HP", "c4:34:6b": "HP", "d8:d3:85": "HP", "e8:39:35": "HP",
    "f0:92:1c": "HP", "fc:15:b4": "HP",
    # Amazon Echo/Fire
    "00:bb:3a": "Amazon", "0c:47:c9": "Amazon", "28:ef:01": "Amazon",
    "34:d2:70": "Amazon", "38:f7:3d": "Amazon", "40:b4:cd": "Amazon",
    "44:65:0d": "Amazon", "50:f5:da": "Amazon", "68:37:e9": "Amazon",
    "74:c2:46": "Amazon", "84:d6:d0": "Amazon", "a0:02:dc": "Amazon",
    "b4:7c:9c": "Amazon", "f0:27:2d": "Amazon", "f0:4f:7c": "Amazon",
    "f0:81:af": "Amazon", "fc:a1:83": "Amazon",
    # Sony PlayStation
    "00:04:1f": "Sony/PS", "00:13:15": "Sony/PS", "00:15:c1": "Sony/PS",
    "00:19:c5": "Sony/PS", "00:1d:0d": "Sony/PS", "00:1f:a7": "Sony/PS",
    "00:24:8d": "Sony/PS", "28:3f:69": "Sony/PS", "2c:cc:44": "Sony/PS",
    "70:9e:29": "Sony/PS", "bc:60:a7": "Sony/PS", "f8:46:1c": "Sony/PS",
    # Nintendo Switch
    "00:09:bf": "Nintendo", "00:17:ab": "Nintendo", "00:19:fd": "Nintendo",
    "00:1b:ea": "Nintendo", "00:1f:32": "Nintendo", "00:22:4c": "Nintendo",
    "00:24:44": "Nintendo", "00:25:a0": "Nintendo", "34:af:2c": "Nintendo",
    "40:f4:07": "Nintendo", "7c:bb:8a": "Nintendo", "98:b6:e9": "Nintendo",
    "a4:c0:e1": "Nintendo", "e0:0c:7f": "Nintendo",
    # Microsoft Xbox / Surface
    "00:12:5a": "Microsoft", "00:15:5d": "Microsoft", "00:17:fa": "Microsoft",
    "00:1d:d8": "Microsoft", "00:22:48": "Microsoft", "00:50:f2": "Microsoft",
    "28:18:78": "Microsoft", "30:59:b7": "Microsoft", "48:b0:2d": "Microsoft",
    "54:27:1e": "Microsoft", "5c:ba:37": "Microsoft", "60:45:bd": "Microsoft",
    "7c:1e:52": "Microsoft", "94:e6:f7": "Microsoft", "98:5f:d3": "Microsoft",
    "a4:c3:f0": "Microsoft", "b8:81:98": "Microsoft", "c4:9d:ed": "Microsoft",
    "dc:41:a9": "Microsoft", "e4:a7:a0": "Microsoft",
    # Raspberry Pi
    "28:cd:c1": "RaspberryPi", "2c:cf:67": "RaspberryPi", "b8:27:eb": "RaspberryPi",
    "d8:3a:dd": "RaspberryPi", "dc:a6:32": "RaspberryPi", "e4:5f:01": "RaspberryPi",
}

# Device type classification
PHONE_VENDORS   = {"Apple", "Samsung", "Google"}
TV_VENDORS      = {"TCL/Roku", "Roku", "Sony/PS"}
ROUTER_VENDORS  = {"Ubiquiti", "Cisco", "Netgear", "ASUS"}
LAPTOP_VENDORS  = {"HP", "Dell", "Lenovo", "Microsoft"}
GAMING_VENDORS  = {"Nintendo", "Sony/PS", "Microsoft"}
IOT_VENDORS     = {"Amazon", "RaspberryPi"}

# ── DATABASE ────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS devices (
        mac TEXT PRIMARY KEY,
        ip TEXT, label TEXT, vendor TEXT, device_type TEXT,
        first_seen REAL, last_seen REAL, is_phone INTEGER DEFAULT 0,
        is_known INTEGER DEFAULT 0, notes TEXT DEFAULT ''
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS presence_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mac TEXT, label TEXT, event TEXT, ts REAL
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS news_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        headline TEXT, source TEXT, url TEXT, ts REAL
    )""")
    conn.commit()
    conn.close()

def db_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

# ── MAC UTILITIES ───────────────────────────────────────────────────────────
def normalize_mac(mac):
    """Normalize MAC to lowercase colon-separated."""
    mac = mac.lower().replace("-", ":").replace(".", ":")
    parts = mac.split(":")
    if len(parts) == 1 and len(mac) == 12:
        parts = [mac[i:i+2] for i in range(0, 12, 2)]
    return ":".join(p.zfill(2) for p in parts)

def get_vendor(mac):
    mac = normalize_mac(mac)
    prefix = mac[:8]
    if prefix in MAC_VENDORS:
        return MAC_VENDORS[prefix]
    prefix3 = mac[:5]
    for k, v in MAC_VENDORS.items():
        if k[:5] == prefix3:
            return v
    return "Unknown"

def classify_device(vendor, mac):
    if vendor in PHONE_VENDORS:
        return "phone"
    if vendor in TV_VENDORS:
        return "tv"
    if vendor in ROUTER_VENDORS:
        return "router"
    if vendor in LAPTOP_VENDORS:
        return "laptop"
    if vendor in GAMING_VENDORS:
        return "gaming"
    if vendor in IOT_VENDORS:
        return "iot"
    if mac.startswith("ff:ff:ff") or mac.startswith("01:00"):
        return "broadcast"
    return "unknown"

# ── ARP SCANNER ─────────────────────────────────────────────────────────────
def scan_network():
    """Run arp -a and parse all devices."""
    try:
        result = subprocess.run(["arp", "-a"], capture_output=True, text=True, timeout=5)
        devices = []
        for line in result.stdout.splitlines():
            # Match: hostname (ip) at mac on interface
            m = re.search(r'\((\d+\.\d+\.\d+\.\d+)\)\s+at\s+([\w:]+)', line)
            if not m:
                continue
            ip  = m.group(1)
            mac = m.group(2)
            if mac in ("(incomplete)", "ff:ff:ff:ff:ff:ff"):
                continue
            if ip.endswith(".255") or ip.startswith("224.") or ip.startswith("239."):
                continue
            mac = normalize_mac(mac)
            vendor = get_vendor(mac)
            dtype  = classify_device(vendor, mac)
            # Extract hostname
            hn_match = re.match(r'(\S+)\s+\(', line)
            hostname = hn_match.group(1) if hn_match and hn_match.group(1) != "?" else ""
            devices.append({
                "mac": mac, "ip": ip, "vendor": vendor,
                "device_type": dtype, "hostname": hostname,
                "is_phone": dtype == "phone"
            })
        return devices
    except Exception as e:
        return []

# ── PING SWEEP ──────────────────────────────────────────────────────────────
def ping_device(ip, timeout=1):
    """Ping a single IP, return True if alive."""
    try:
        result = subprocess.run(
            ["ping", "-c", "1", "-W", str(timeout), ip],
            capture_output=True, timeout=timeout + 1
        )
        return result.returncode == 0
    except:
        return False

# ── PRESENCE ENGINE ─────────────────────────────────────────────────────────
class PresenceEngine:
    def __init__(self):
        self._lock    = threading.Lock()
        self._devices = {}      # mac -> device dict
        self._known   = {}      # mac -> label (user-defined)
        self._history = []      # last 200 presence events
        self._load_known()

    def _load_known(self):
        try:
            conn = db_conn()
            rows = conn.execute(
                "SELECT mac, label FROM devices WHERE is_known=1"
            ).fetchall()
            for mac, label in rows:
                self._known[mac] = label
            conn.close()
        except:
            pass

    def label_device(self, mac, label):
        """User assigns a name to a MAC."""
        mac = normalize_mac(mac)
        self._known[mac] = label
        try:
            conn = db_conn()
            conn.execute(
                "UPDATE devices SET label=?, is_known=1 WHERE mac=?",
                (label, mac)
            )
            conn.commit()
            conn.close()
        except:
            pass

    def update(self, raw_devices):
        """Process a fresh ARP scan result."""
        now    = time.time()
        seen   = {d["mac"]: d for d in raw_devices}
        events = []

        with self._lock:
            # Devices that appeared
            for mac, dev in seen.items():
                label  = self._known.get(mac) or dev.get("hostname") or dev["vendor"]
                if mac not in self._devices:
                    events.append({"mac": mac, "label": label, "event": "joined", "ts": now})
                self._devices[mac] = {**dev, "label": label, "last_seen": now,
                                      "first_seen": self._devices.get(mac, {}).get("first_seen", now)}

            # Devices that left
            for mac in list(self._devices.keys()):
                if mac not in seen:
                    label = self._devices[mac].get("label", mac)
                    events.append({"mac": mac, "label": label, "event": "left", "ts": now})
                    del self._devices[mac]

            # Log events
            for ev in events:
                self._history.append(ev)
            if len(self._history) > 500:
                self._history = self._history[-500:]

        # Persist to DB
        if events or raw_devices:
            try:
                conn = db_conn()
                for mac, dev in seen.items():
                    label = self._known.get(mac) or dev.get("hostname") or dev["vendor"]
                    conn.execute("""INSERT OR REPLACE INTO devices
                        (mac, ip, label, vendor, device_type, first_seen, last_seen, is_phone, is_known)
                        VALUES (?,?,?,?,?,
                            COALESCE((SELECT first_seen FROM devices WHERE mac=?), ?),
                            ?,?,?)""",
                        (mac, dev["ip"], label, dev["vendor"], dev["device_type"],
                         mac, now, now, dev["is_phone"],
                         1 if mac in self._known else 0))
                for ev in events:
                    conn.execute(
                        "INSERT INTO presence_log (mac, label, event, ts) VALUES (?,?,?,?)",
                        (ev["mac"], ev["label"], ev["event"], ev["ts"])
                    )
                conn.commit()
                conn.close()
            except:
                pass

        return events

    def get_state(self):
        with self._lock:
            devices = list(self._devices.values())
        phones  = [d for d in devices if d.get("is_phone")]
        others  = [d for d in devices if not d.get("is_phone")
                   and d.get("device_type") not in ("broadcast",)]
        unknown = [d for d in devices if d.get("device_type") == "unknown"
                   and not d.get("is_phone")]
        return {
            "total": len(devices),
            "phones": phones,
            "others": others,
            "unknown_devices": unknown,
            "all": devices,
            "history": self._history[-50:],
        }

    def get_known_map(self):
        return dict(self._known)


# ── NEWS FETCHER ─────────────────────────────────────────────────────────────
class NewsFetcher:
    FEEDS = {
        "top":      "https://feeds.bbci.co.uk/news/rss.xml",
        "markets":  "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC,^DJI,^IXIC&region=US&lang=en-US",
        "tech":     "https://feeds.feedburner.com/TechCrunch",
        "world":    "https://feeds.bbci.co.uk/news/world/rss.xml",
        "business": "https://feeds.bbci.co.uk/news/business/rss.xml",
    }

    def __init__(self):
        self._lock    = threading.Lock()
        self._cache   = []
        self._last    = 0

    def fetch(self):
        now = time.time()
        if now - self._last < 300:  # 5 min cache
            with self._lock:
                return list(self._cache)
        items = []
        for cat, url in self.FEEDS.items():
            try:
                req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urlopen(req, timeout=5) as r:
                    content = r.read().decode("utf-8", errors="ignore")
                # Parse RSS items
                titles = re.findall(r'<title><!\[CDATA\[(.*?)\]\]></title>', content)
                titles += re.findall(r'<title>(.*?)</title>', content)
                links  = re.findall(r'<link>(https?://[^<]+)</link>', content)
                for i, title in enumerate(titles[1:8]):  # skip feed title
                    items.append({
                        "headline": title.strip(),
                        "source": cat,
                        "url": links[i] if i < len(links) else "",
                        "ts": now
                    })
            except:
                pass
        with self._lock:
            self._cache = items[:40]
            self._last  = now
        return items[:40]


# ── WEATHER FETCHER ──────────────────────────────────────────────────────────
class WeatherFetcher:
    def __init__(self):
        self._cache = {}
        self._last  = 0

    def fetch(self, lat=40.7128, lon=-74.0060, city="New York"):
        now = time.time()
        if now - self._last < 600:  # 10 min cache
            return self._cache
        try:
            url = (f"https://api.open-meteo.com/v1/forecast"
                   f"?latitude={lat}&longitude={lon}"
                   f"&current=temperature_2m,weathercode,windspeed_10m,relativehumidity_2m"
                   f"&hourly=temperature_2m,weathercode"
                   f"&temperature_unit=fahrenheit&windspeed_unit=mph"
                   f"&forecast_days=1&timezone=auto")
            with urlopen(url, timeout=5) as r:
                data = json.loads(r.read())
            cur = data.get("current", {})
            self._cache = {
                "temp_f":    round(cur.get("temperature_2m", 0)),
                "wind_mph":  round(cur.get("windspeed_10m", 0)),
                "humidity":  cur.get("relativehumidity_2m", 0),
                "code":      cur.get("weathercode", 0),
                "condition": _wmo_code(cur.get("weathercode", 0)),
                "city":      city,
                "updated":   datetime.now().strftime("%H:%M"),
            }
            self._last = now
        except:
            pass
        return self._cache


def _wmo_code(code):
    codes = {
        0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
        45: "Foggy", 48: "Icy fog", 51: "Light drizzle", 53: "Drizzle",
        55: "Heavy drizzle", 61: "Light rain", 63: "Rain", 65: "Heavy rain",
        71: "Light snow", 73: "Snow", 75: "Heavy snow", 77: "Snow grains",
        80: "Light showers", 81: "Showers", 82: "Heavy showers",
        85: "Snow showers", 86: "Heavy snow showers",
        95: "Thunderstorm", 96: "Thunderstorm w/ hail", 99: "Heavy thunderstorm",
    }
    return codes.get(code, "Unknown")


# ── MARKET TICKER ────────────────────────────────────────────────────────────
class MarketFetcher:
    SYMBOLS = ["^GSPC", "^DJI", "^IXIC", "^VIX", "BTC-USD", "GC=F", "CL=F"]

    def __init__(self):
        self._cache = {}
        self._last  = 0

    def fetch(self):
        now = time.time()
        if now - self._last < 60:  # 1 min cache
            return self._cache
        results = {}
        for sym in self.SYMBOLS:
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=2d"
                req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urlopen(req, timeout=5) as r:
                    data = json.loads(r.read())
                meta   = data["chart"]["result"][0]["meta"]
                price  = meta.get("regularMarketPrice", 0)
                prev   = meta.get("chartPreviousClose", price)
                chg    = price - prev
                chg_pct = (chg / prev * 100) if prev else 0
                name   = {
                    "^GSPC": "S&P 500", "^DJI": "DOW", "^IXIC": "NASDAQ",
                    "^VIX": "VIX", "BTC-USD": "BTC", "GC=F": "GOLD", "CL=F": "OIL"
                }.get(sym, sym)
                results[sym] = {
                    "name": name, "price": price,
                    "change": round(chg, 2), "change_pct": round(chg_pct, 2),
                    "up": chg >= 0
                }
            except:
                pass
        if results:
            self._cache = results
            self._last  = now
        return self._cache


# ── SPORTS SCORES ────────────────────────────────────────────────────────────
class SportsFetcher:
    def __init__(self):
        self._cache = []
        self._last  = 0

    def fetch(self):
        now = time.time()
        if now - self._last < 300:
            return self._cache
        games = []
        # ESPN API — free, no key needed
        leagues = [
            ("nba", "NBA"),
            ("nfl", "NFL"),
            ("mlb", "MLB"),
            ("nhl", "NHL"),
        ]
        for slug, name in leagues:
            try:
                url = f"https://site.api.espn.com/apis/site/v2/sports/{_espn_sport(slug)}/{slug}/scoreboard"
                with urlopen(url, timeout=5) as r:
                    data = json.loads(r.read())
                for ev in data.get("events", [])[:4]:
                    comps = ev.get("competitions", [{}])[0]
                    teams = comps.get("competitors", [])
                    if len(teams) < 2:
                        continue
                    t1 = teams[0]
                    t2 = teams[1]
                    status = ev.get("status", {}).get("type", {})
                    games.append({
                        "league": name,
                        "home": t1.get("team", {}).get("abbreviation", ""),
                        "away": t2.get("team", {}).get("abbreviation", ""),
                        "home_score": t1.get("score", ""),
                        "away_score": t2.get("score", ""),
                        "status": status.get("shortDetail", ""),
                        "live": status.get("state", "") == "in",
                    })
            except:
                pass
        self._cache = games
        self._last  = now
        return games


def _espn_sport(slug):
    return {"nba": "basketball", "nfl": "football",
            "mlb": "baseball", "nhl": "hockey"}.get(slug, "basketball")


# ── MAIN PALANTIR STATE ──────────────────────────────────────────────────────
init_db()
presence  = PresenceEngine()
news      = NewsFetcher()
weather   = WeatherFetcher()
markets   = MarketFetcher()
sports    = SportsFetcher()

_pal_state = {
    "devices": {},
    "news": [],
    "weather": {},
    "markets": {},
    "sports": [],
    "alerts": [],
    "last_updated": 0,
}
_pal_lock = threading.Lock()


def _palantir_loop():
    """Background thread — runs every 30 seconds."""
    # Import systems lazily to avoid circular imports
    try:
        import systems as _systems
        _has_systems = True
    except ImportError:
        _has_systems = False

    while True:
        try:
            # Network scan
            raw       = scan_network()
            events    = presence.update(raw)
            dev_state = presence.get_state()

            # Fetch external data (staggered by cache TTL inside each fetcher)
            news_data    = news.fetch()
            weather_data = weather.fetch()
            market_data  = markets.fetch()
            sports_data  = sports.fetch()

            # Build alerts from presence events
            alerts = []
            for ev in events:
                if ev["event"] == "joined":
                    d     = next((x for x in raw if x["mac"] == ev["mac"]), {})
                    dtype = d.get("device_type", "unknown")
                    if dtype == "unknown":
                        alerts.append({
                            "type": "UNKNOWN_DEVICE",
                            "msg": f"Unknown device joined: {ev['mac']} ({d.get('ip','')})",
                            "urgency": 7, "ts": ev["ts"]
                        })
                    elif dtype == "phone":
                        alerts.append({
                            "type": "PERSON_HOME",
                            "msg": f"{ev['label']} arrived home",
                            "urgency": 3, "ts": ev["ts"]
                        })
                elif ev["event"] == "left":
                    # Check if this was a phone by looking at SPECTER or device type
                    was_phone = ev.get("device_type") == "phone"
                    name_known = ev["label"] not in ("Unknown", ev["mac"], "")
                    if was_phone or name_known:
                        alerts.append({
                            "type": "PERSON_LEFT",
                            "msg": f"{ev['label']} left",
                            "urgency": 2, "ts": ev["ts"]
                        })

            with _pal_lock:
                _pal_state["devices"]      = dev_state
                _pal_state["news"]         = news_data[:20]
                _pal_state["weather"]      = weather_data
                _pal_state["markets"]      = market_data
                _pal_state["sports"]       = sports_data
                _pal_state["alerts"]       = (alerts + _pal_state.get("alerts", []))[:50]
                _pal_state["last_updated"] = time.time()
                current_pal = dict(_pal_state)

            # Run all extended systems on this tick
            if _has_systems:
                try:
                    from syswatch_web import local_state as _ls, oracle_state as _os
                    _systems.tick(_ls, _os, current_pal)
                except Exception:
                    pass

        except Exception:
            pass
        time.sleep(30)


def start():
    """Start background collection thread."""
    # Pre-seed known devices
    _preseed_known_devices()
    t = threading.Thread(target=_palantir_loop, daemon=True)
    t.name = "palantir-loop"
    t.start()

def _preseed_known_devices():
    """Pre-register known MACs so they show correctly from first scan."""
    known = {
        "8a:f7:62:02:82:6e": "Caiden iPhone",
        "30:e3:a4:b2:f1:33": "Mom Laptop",
        "cc:a1:2b:70:e5:31": "Roku TV",
    }
    for mac, label in known.items():
        presence.label_device(mac, label)


def get_state():
    with _pal_lock:
        return dict(_pal_state)


def label_device(mac, label):
    presence.label_device(normalize_mac(mac), label)
    return {"ok": True}


def get_presence_history():
    return presence._history[-100:]
