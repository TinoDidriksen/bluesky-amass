#!/usr/bin/env python3
import datetime
import time
import random
import os
from pathlib import Path
from websockets.sync.client import connect

ts = datetime.datetime.now(datetime.timezone.utc)
m = ts.strftime('%M')
stamp = int(time.time_ns() / 1000)
stamp = 1732496461 * 1000 * 1000 - 10*1000*1000
if os.path.exists('stream/stamp.txt'):
	stamp = int(Path('stream/stamp.txt').read_text()) - 10*1000*1000
else:
	Path('stream/stamp.txt').touch()

hosts = ['jetstream1.us-east.bsky.network', 'jetstream2.us-east.bsky.network', 'jetstream1.us-west.bsky.network', 'jetstream2.us-west.bsky.network']
host = random.choice(hosts)
print(f'Fetching from {host} cursor {stamp}', flush=True)

folder = 'stream/'+ts.strftime('%Y/%m/%d/%H')
os.makedirs(folder, exist_ok=True)
f = open(f'{folder}/{m}.zst', 'ab')
print(f'{folder}/{m}.zst', flush=True)

sf = open('stream/stamp.txt', 'r+')

with connect(f"wss://{host}/subscribe?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.postgate&wantedCollections=app.bsky.feed.repost&wantedCollections=app.bsky.feed.threadgate&compress=true&cursor={stamp}") as websocket:
	while True:
		message = websocket.recv()
		ts = datetime.datetime.now(datetime.timezone.utc)
		nm = ts.strftime('%M')
		if nm != m:
			m = nm
			f.close()
			folder = 'stream/'+ts.strftime('%Y/%m/%d/%H')
			os.makedirs(folder, exist_ok=True)
			f = open(f'{folder}/{m}.zst', 'ab')
			print(f'{folder}/{m}.zst', flush=True)
		f.write(message)

		sf.seek(0)
		sf.write(str(int(time.time_ns() / 1000)))
