#!/usr/bin/python
#

from datetime import datetime, timedelta
from collections import namedtuple, deque
import sys, argparse

TwitterFrequency = namedtuple(
    "TwitterFrequency",
    ["entity","timestamp","frequency"]
    )

def load_data(filename,fl = None):
    with open(filename) as f:
        for line in f.readlines():
            timestamp,entity,count = line.split(",")
            try:
                count = int(count.strip())
            except ValueError:
                count = 0
            entity = entity.strip()
            timestamp = datetime.strptime(timestamp,"%Y-%m-%dT%H:%M:%S.%fZ")
            ## Round timestamp
            if timestamp.microsecond > 500000:
                timestamp += timedelta(microseconds = 1000000-timestamp.microsecond)
            else:
                timestamp -= timedelta(microseconds = timestamp.microsecond)
            if fl(entity):
                yield TwitterFrequency(entity = entity,
                    timestamp = timestamp,
                    frequency = count
                    )

def format_out(data, label, aggregate=1):
        
    if not label:
        print("Error, no label passed")
        return
    
    print("Date,%s"%(",".join(label)))
    pos = {e:j for j,e in enumerate(label)}
    time = None
    tmp_data = [deque([0],maxlen = aggregate) for i in xrange(len(label))]

    data.sort(key=lambda x:x.timestamp)

    for e in data:
        if time is None:
            #Init code
            time = e.timestamp

        while abs((e.timestamp - time).total_seconds()) > 1:
            if len(tmp_data[0]) == aggregate:
                print("%s,%s"%(
                    (time-timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'),
                    ",".join("%.1f"%(0.1*sum(i)) for i in tmp_data)
                    ))
            for i in xrange(len(label)):
                tmp_data[i].appendleft(0)
            time += timedelta(seconds = 5)
        try:
            tmp_data[pos[e.entity]][0] = e.frequency
        except KeyError:
            pass

    if time:
        print("%s,%s"%(
            (time-timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'),
            ",".join("%d"%0.1*sum(i) for i in tmp_data)
            ))

def main(argv):
    argparser = argparse.ArgumentParser()
    argparser.add_argument('filename', type=str, help='the filename')
    argparser.add_argument('labels', metavar='labels', type=str, nargs='+',
                   help='the labels to extract')
    argparser.add_argument('--window', '-w', dest='window', type=int, default = 1)

    flags = argparser.parse_args()

    data = list(load_data(flags.filename, lambda x:x in set(flags.labels)))
    format_out(data,flags.labels,flags.window)

if __name__ == '__main__':
    main(sys.argv)