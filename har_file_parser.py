#!/usr/bin/env python3
import json
import sys

# Need to update this to take in from the file from the helper functions
# We should have the client automatically pass all files with .har extension to be parsed by this

try:
    with open(sys.argv[1]) as infile:
        j = json.loads(infile.read())
except Exception as inst:
    sys.exit("Unable to read har file: " + str(sys.argv[1]) + "\r\nERROR:\r\n" + str(inst))

for e in j['log']['entries']:
    # Get the Referer header value
    for header in (e['request']['headers']):
        if header['name'] == 'Referer':
            referer = header['value']
    # Get the Date header value
    for header in (e['response']['headers']):
        if header['name'] == 'Date':
            date = header['value']

    # Print the output
    print("%s -> REF: %s -> %s %s -> %s %s %s bytes" % (
        date,
        referer,
        e['request']['method'],
        e['request']['url'],
        e['response']['status'],
        e['response']['statusText'],
        e['response']['content']['size']
    ))
    # Print the cache-control header value
    for l in e['response']['headers']:
        if l['name'] in ('cache-control'):  # filter by header name
            print(l['name'] + ": " + l['value'], end=", ")
    print()
