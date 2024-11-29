#!/usr/bin/env python3
import datetime
import time
import random
import os
import zstandard
from pathlib import Path
from websockets.sync.client import connect

ts = datetime.datetime.now(datetime.timezone.utc)
m = ts.strftime('%M')
stamp = int(time.time_ns() / 1000) - 10*1000*1000
if os.path.exists('stream/stamp.txt'):
	stamp = int(Path('stream/stamp.txt').read_text()) - 10*1000*1000
else:
	os.makedirs('stream', exist_ok=True)
	Path('stream/stamp.txt').touch()

hosts = ['jetstream1.us-east.bsky.network', 'jetstream2.us-east.bsky.network', 'jetstream1.us-west.bsky.network', 'jetstream2.us-west.bsky.network']
host = random.choice(hosts)
print(f'Fetching from {host} cursor {stamp}', flush=True)

dict = zstandard.ZstdCompressionDict(Path('zstd_dictionary').read_bytes())
decomp = zstandard.ZstdDecompressor(dict_data=dict)

folder = 'stream/'+ts.strftime('%Y/%m/%d/%H')
os.makedirs(folder, exist_ok=True)
fn = f'{folder}/{m}.txt'
f = open(fn, 'ab')
df = decomp.stream_writer(f)
print(fn, flush=True)

sf = open('stream/stamp.txt', 'r+')
nl = bytes("\n", encoding='UTF-8')

with connect(f"wss://{host}/subscribe?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.postgate&wantedCollections=app.bsky.feed.repost&wantedCollections=app.bsky.feed.threadgate&compress=true&cursor={stamp}") as websocket:
	while True:
		message = websocket.recv()

		ts = datetime.datetime.now(datetime.timezone.utc)
		nm = ts.strftime('%M')
		if nm != m:
			m = nm
			df.close()
			folder = 'stream/'+ts.strftime('%Y/%m/%d/%H')
			os.makedirs(folder, exist_ok=True)
			fn = f'{folder}/{m}.txt'
			f = open(fn, 'ab')
			df = decomp.stream_writer(f)
			print(fn, flush=True)

		df.write(message)
		df.flush()
		f.write(nl)

		sf.seek(0)
		sf.write(str(int(time.time_ns() / 1000)))
