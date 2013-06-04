import sys
import re

url_regex = re.compile("(?P<url>https?://[^\s]+)")
cur_page = "NONE"
# For each page, we buffer URL counts in memory
url_count = {}

for line in sys.stdin:
  if (line[0:4] == "http" and len(line.split(" ")) == 5):
    cur_page = line.split(" ")[0]
    for (url, count) in url_count.items():
      print "%s\t%s\t%s" % (cur_page, url, count)
    url_count = {}

  for url in url_regex.findall(line):
     url_count[url] = url_count.get(url, 0) + 1

for (url, count) in url_count.items():
  print "%s\t%s\t%s" % (cur_page, url, count)
