#!/usr/bin/env python3
"""
BT Scanner — runs as isolated subprocess, prints JSON to stdout.
Uses bleak which handles the NSRunLoop correctly as a main-thread process.
"""
import asyncio, json, sys, time

async def scan():
    try:
        from bleak import BleakScanner
        devices = await BleakScanner.discover(timeout=10, return_adv=True)
        results = {}
        for addr, (device, adv) in devices.items():
            try:
                rssi = adv.rssi if hasattr(adv, 'rssi') else -100
                results[addr] = {
                    "rssi": rssi,
                    "name": device.name or "",
                    "ts":   time.time(),
                }
            except Exception:
                pass
        print(json.dumps(results))
    except Exception as e:
        print(json.dumps({"error": str(e)}))

if __name__ == "__main__":
    asyncio.run(scan())
