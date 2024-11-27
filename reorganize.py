#!/usr/bin/env python3
import json
import sys

decoder = json.JSONDecoder()
buf = ''
while tmp := sys.stdin.read(1024*1024):
    buf = buf + tmp
    while buf.find('}') != -1:
        obj, index = decoder.raw_decode(buf)
        buf = buf[index:]
