#!/usr/bin/python
#

from datetime import datetime, timedelta
from collections import defaultdict
import sys

filename = sys.argv[1]
hashtags = set()
mentions = set()

data = {}

with open(filename) as f:
    for line in f.readlines():
        fields = line.split(",")
        t  = fields[0]
        ts = fields[1]
        ts = datetime.strptime(ts,"%Y-%m-%dT%H:%M:%S.%fZ")
        if ts.microsecond > 500000:
            ts += timedelta(microseconds = 1000000-ts.microsecond)
        else:
            ts -= timedelta(microseconds = ts.microsecond)
        fields = fields[2:]
        if ts not in data:
            data[ts]=defaultdict(int)
        for i in xrange(0,len(fields),2):
            label, count = fields[i:i+2]
            count = int(count)
            if t == 'hashtag':
                hashtags.add(label)
            else:
                mentions.add(label)
            data[ts][label]=count

    labels = list(hashtags) + list(mentions)
    print("date,%s"%",".join(labels))
    ts = data.keys()
    ts.sort()
    for t in ts:
        print("%s,%s"%(
            t.strftime('%Y-%m-%d %H:%M:%S'),
            ",".join("%d"%data[t][i] for i in labels)
            ))
