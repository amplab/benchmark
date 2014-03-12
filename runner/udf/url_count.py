# Copyright 2013 The Regents of The University California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
