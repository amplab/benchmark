---
title: Big Data Benchmark
layout: default
---

_**Here from HackerNews? This was originally posted several months ago. Check back in two weeks for an updated benchmark including newer versions of Hive, Impala, and Shark.**_  

<!-- This is an open source benchmark which compares the performance of several large scale data-processing frameworks. -->

<h2 id="introduction">Introduction</h2>

Several analytic frameworks have been announced in the last six months. Among them are inexpensive data-warehousing solutions based on traditional Massively Parallel Processor (MPP) architectures ([Redshift](http://aws.amazon.com/redshift/)), systems which impose MPP-like execution engines on top of Hadoop ([Impala](http://blog.cloudera.com/blog/2012/10/cloudera-impala-real-time-queries-in-apache-hadoop-for-real/), [HAWQ](http://www.greenplum.com/news/press-release/emc-introduces-worlds-most-powerful-hadoop-distribution-pivotal-hd)) and systems which optimize MapReduce to improve performance on analytical workloads ([Shark](http://shark.cs.berkeley.edu/), [Stinger](http://hortonworks.com/blog/100x-faster-hive/)). This benchmark provides [quantitative](#results) and [qualitative](#discussion) comparisons of four sytems. It is entirely hosted on EC2 and can be reproduced directly from your computer.

* [Redshift](http://aws.amazon.com/redshift/) - a hosted MPP database offered by Amazon.com based on the ParAccel data warehouse. 
* [Hive](http://hive.apache.org/) - a Hadoop-based data warehousing system. (v0.10, 1/2013 *Note: Hive [v0.11](http://hortonworks.com/blog/apache-hive-0-11-stinger-phase-1-delivered/), which advertises improved performance, was recently released but is not yet included*)
* [Shark](http://shark.cs.berkeley.edu/) - a Hive-compatible SQL engine which runs on top of the [Spark](http://spark-project.org) computing framework. (v0.8 preview, 5/2013)
* [Impala](http://blog.cloudera.com/blog/2012/10/cloudera-impala-real-time-queries-in-apache-hadoop-for-real/) - a Hive-compatible<a href="#discussion">*</a> SQL engine with its own MPP-like execution engine. (v1.0, 4/2013)

This remains a  _**work in progress**_ and will evolve to include additional frameworks and new capabilities. We welcome <a href="#contributions">contributions</a>.

### What is being evaluated?
This benchmark measures response time on a handful of relational queries: scans, aggregations, joins, and UDF's, across different data sizes. Keep in mind that these systems have very different sets of capabilities. MapReduce-like systems (Shark/Hive) target flexible and large-scale computation, supporting complex User Defined Functions (UDF's), tolerating failures, and scaling to thousands of nodes. Traditional MPP databases are strictly SQL compliant and heavily optimized for relational queries. The workload here is simply one set of queries that most of these systems these can complete.

 
<h3 id="workload">Dataset and Workload</h3>
Our dataset and queries are inspired by the benchmark contained in "[A comparison of approaches to large scale analytics](http://database.cs.brown.edu/sigmod09/benchmarks-sigmod09.pdf)". The input data set consists of a set of unstructured HTML documents and two SQL tables which contain summary information. It was generated using [Intel's Hadoop benchmark tools](https://github.com/intel-hadoop/HiBench) and data sampled from the [Common Crawl](http://commoncrawl.org) document corpus. There are three datasets with the following schemas:

<div class="span11" style="float: none; margin-top: 5px; margin-bottom: 5px; margin-left: auto; margin-right: auto;">
<table padding="10" id="inputSchema">
  <tr> 
    <th markdown="1">`Documents`</th>
    <th markdown="1">`Rankings`</th>
    <th markdown="1">`UserVisits`</th>
  </tr>
  <tr>
    <td style="padding-left: 20px; padding-right: 20px;">
      _Unstructured HTML documents_
    </td>
    <td style="padding-left: 20px; padding-right: 20px;" markdown="1">
      _Lists websites and their page rank_
    </td>
    <td style="padding-left: 20px; padding-right: 20px;">
      _Stores server logs for each web page_
    </td>
  </tr>
  <tr>
    <td></td>
    <td markdown="1" align="center" valign="top">
{% highlight mysql %}
pageURL VARCHAR(300)
pageRank INT
avgDuration INT
{% endhighlight %}
    </td>
    <td align="center" valign="top">
{% highlight mysql %}
sourceIP VARCHAR(116)
destURL VARCHAR(100)
visitDate DATE
adRevenue FLOAT
userAgent VARCHAR(256)
countryCode CHAR(3)
languageCode CHAR(6)
searchWord VARCHAR(32)
duration INT
{% endhighlight %}
    </td>
  </tr>
</table>
</div>

 * [Query 1](#query1) and [Query 2](#query2) are exploratory SQL queries. We vary the size of the result to expose scaling properties of each systems.
  * Varaint A: __BI-Like__ - result sets are small (e.g., could fit in memory in a BI tool)
  * Variant B: __Intermediate__ - result set may not fit in memory on one node
  * Variant C: __ETL-Like__ - result sets are large and require several nodes to store

 * [Query 3](#query3) is a join query with a small result set, but varying sizes of joins.

 * [Query 4](#query4) is a bulk UDF query. It calculates a simplified version of PageRank using a sample of the [Common Crawl](http://commoncrawl.org) dataset.

<h4 class="clickable collapsed" data-toggle="collapse" data-target="#hardware-div" id="hardware">Hardware Configuration <img src="media/toggle.gif"/></h4>
<div id="hardware-div" class="collapse">
For Impala, Hive, and Shark, this benchmark uses the m2.4xlarge EC2 instance type. Redshift only has very small and very large instances, so rather than compare identical hardware, we <em>fix the cost</em> of the cluster and opt to purchase a larger number of small nodes for Redshift. We use a scale factor of 5 for the experiments in all cases.

<h4> Instance stats </h4>

<table class="table table-hover tight_rows">
  <tr>
    <th>Framework</th>
    <th>Instance Type</th>
    <th>Memory</th>
    <th>Storage</th>
    <th>Virtual Cores</th>
    <th>$/hour</th>
  </tr>
  <tr>
    <td>Impala, Hive, Shark</td>
    <td>m2.4xlarge</td>
    <td>68.4 GB</td>
    <td>1680GB (2HDD)</td>
    <td>8</td>
    <td>.41</td>
  </tr>
  <tr>
    <td>Redshift</td>
    <td>dw.hs1.xlarge</td>
    <td>15 GB</td>
    <td>2 TB (3HDD)</td>
    <td>2</td>
    <td>.85</td>
  </tr>
</table>

<h4> Cluster stats </h4>
<table class="table table-hover">
  <tr>
    <th>Framework</th>
    <th>Instance Type</th>
    <th>Instances</th>
    <th>Memory</th>
    <th>Storage</th>
    <th>Virtual Cores</th>
    <th>Cluster $/hour</th>
  </tr>
  <tr>
    <td>Impala, Hive, Shark</td>
    <td>m2.4xlarge</td>
    <td>5</td>
    <td>342 GB</td>
    <td>8.4 TB (10HDD)</td>
    <td>40</td>
    <td><strong>$8.20</strong></td>
  </tr>
  <tr>
    <td>Redshift</td>
    <td>dw.hs1.xlarge</td>
    <td>10</td>
    <td>150 GB</td>
    <td>20 TB (30HDD)</td>
    <td>20</td>
    <td><strong>$8.50</strong></td>
  </tr>
</table>
</div>


<h3 id="results"> Results | May 2013</h3>

We launch EC2 clusters and run each query several times. We report the median response time here. Except for Redshift, all data is stored on HDFS in compressed SequenceFile format using CDH 4.2.0. Each query is run with six frameworks:

<table class="table tight_rows" markdown="1">

<tr><td>__Redshift__</td><td>Amazon Redshift with default options.</td></tr>
<tr><td>__Shark - disk__</td><td>Input and output tables are on-disk compressed with gzip. OS buffer cache is cleared before each run.</td></tr>
<tr><td>__Impala - disk__</td><td>Input and output tables are on-disk compressed with snappy. OS buffer cache is cleared before each run.</td></tr>
<tr><td>__Shark - mem__</td><td>Input tables are stored in Spark cache. Output tables are stored in Spark cache.</td></tr>
<tr><td>__Impala - mem__</td><td>Input tables are coerced into the OS buffer cache. Output tables are on disk (Impala has no notion of a cached table).</td></tr>
<tr><td>__Hive__</td><td>Hive with default options. Input and output tables are on disk compressed with snappy. OS buffer cache is cleared before each run.</td></tr>
</table>

<h4 id="query1">1. Scan Query </h4>

{% highlight mysql %}
SELECT pageURL, pageRank FROM rankings WHERE pageRank > X
{% endhighlight %}

<table style="width:800px" class="table tight_rows">
  <tr>
    <th></th>
    <th>Query 1A<br>32,888 results</th>
    <th>Query 1B<br>3,331,851 results</th>
    <th>Query 1C<br>89,974,976 results</th>
  </tr>
  <tr>
    <th></th>
    <th>
      <script type="text/javascript">
        var one_a_data = [[2.43], [9.86], [0.75], [11.79], [1.07], [45.08]];
        var labels = ["redshift", "impala - disk", "impala - mem", "shark - disk", "shark - mem", "hive 0.10"];
        make_graph(one_a_data, labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        var one_b_data = [[2.49], [11.95], [4.48], [11.85], [1.08], [63.30]];
        var labels = ["redshift", "impala - disk", "impala - mem", "shark - disk", "shark - mem", "hive 0.10"];
        make_graph(one_b_data, labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        var one_c_data = [[12.15], [103.97], [108.19], [24.91], [3.52], [69.70]];
        var labels = ["redshift", "impala - disk", "impala - mem", "shark - disk", "shark - mem", "hive 0.10"];
        make_graph(one_c_data, labels);
      </script>
    </th>
  </tr>
  <tr><td></td><td class="title-cell" colspan="3">Median Response Time (s)</td></tr>
  <tr><td>Redshift</td><td>2.4</td><td>2.5</td><td>12.2</td></tr>
  <tr><td>Impala - disk</td><td>9.9</td><td>12</td><td>104</td></tr>
  <tr><td>Impala - mem</td><td>0.75</td><td>4.48</td><td>108</td></tr>
  <tr><td>Shark - disk</td><td>11.8</td><td>11.9</td><td>24.9</td></tr>
  <tr><td>Shark - mem</td><td>1.1</td><td>1.1</td><td>3.5</td></tr>
  <tr><td>Hive</td><td>45</td><td>63</td><td>70</td></tr>
</table>

This query scans and filters the dataset and stores the results.

This query primarily tests the throughput with which each framework can read and write table data. The best performers are Impala (mem) and Shark (mem) which see excellent throughput by avoiding disk. For on-disk data, Redshift sees the best throughput for two reasons. First, the Redshift clusters have more disks and second, Redshift uses columnar compression which allows it to bypass a field which is not used in the query. Shark and Impala scan at HDFS throughput with fewer disks.

Both Shark and Impala outperform Hive by 3-4X due in part to more efficient task launching and scheduling. As the result sets get larger, Impala becomes bottlenecked on the ability to persist the results back to disk. It seems as if writing large tables is not yet optimized in Impala, presumably because its core focus is BI-style queries.

<h4 id="query2">2. Aggregation Query</h4>

{% highlight mysql %}
SELECT SUBSTR(sourceIP, 1, X), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, X)
{% endhighlight %}

<table style="width:800px" class="table tight_rows">
  <tr><th></th>
      <th>Query 2A<br>2,067,313 groups</th>
      <th>Query 2B<br>31,348,913 groups</th>
      <th>Query 2C<br>253,890,330 groups</th>
  </tr>
  <tr>
    <th></th>
    <th>
      <script type="text/javascript">
        var two_a_data = [[27.69], [130.0], [121.3], [210.10], [111.29], [466.62]]
        var labels = ["redshift", "impala - disk", "impala - mem", "shark - disk", "shark - mem", "hive 0.10"];
        make_graph(two_a_data, labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        var two_b_data = [[64.90], [215.67], [208.82], [238.02], [140.73], [490.47]]
        var labels = ["redshift", "impala - disk", "impala - mem", "shark - disk", "shark - mem", "hive 0.10"];
        make_graph(two_b_data, labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        var two_c_data = [[91.54], [564.93], [557.29], [278.54], [156.133], [552.01]]
        var labels = ["redshift", "impala - disk", "impala - mem", "shark - disk", "shark - mem", "hive 0.10"];
        make_graph(two_c_data, labels);
      </script>
    </th>
  </tr>
  <tr><td></td><td class="title-cell" colspan="3">Median Response Time (s)</td></tr>
  <tr><td>Redshift</td><td>28</td><td>65</td><td>92</td></tr>
  <tr><td>Impala - disk</td><td>130</td><td>216</td><td>565</td></tr>
  <tr><td>Impala - mem</td><td>121</td><td>208</td><td>557</td></tr>
  <tr><td>Shark - disk</td><td>210</td><td>238</td><td>279</td></tr>
  <tr><td>Shark - mem</td><td>111</td><td>141</td><td>156</td></tr>
  <tr><td>Hive - disk</td><td>466</td><td>490</td><td>552</td></tr>
</table>

This query applies string parsing to each input tuple then performs a high-cardinality aggregation.

Redshift's columnar storage provides greater benefit than in Query 1 since several columns of the `UserVistits` table are un-used. While Shark's in-memory tables are also columnar, it is bottlenecked here on the speed at which it evaluates the `SUBSTR` expression. Since Impala is reading from the OS buffer cache, it must read and decompress entire rows. Unlike Shark, however, Impala evaluates this expression using very efficient compiled code. These two factors offset each other and Impala and Shark achieve roughly the same raw throughput for in memory tables. For larger result sets, Impala again sees high latency due to the speed of materializing output tables.

<!-- Important to note is that Impala and Redshift perform _streaming aggregations_, where intermediate results are not persisted to disk and all must run concurrently. Shark and Hive write intermediate results to disk before shuffling them. In both this and the following query, the all intermediate data fits within the OS buffer for Shark/Hive. -->

<h4 id="query3">3. Join Query </h4>
{% highlight mysql %}
SELECT sourceIP, totalRevenue, avgPageRank
FROM
  (SELECT sourceIP,
          AVG(pageRank) as avgPageRank,
          SUM(adRevenue) as totalRevenue
    FROM Rankings AS R, UserVisits AS UV
    WHERE R.pageURL = UV.destURL
       AND UV.visitDate BETWEEN Date(`1980-01-01') AND Date(`X')
    GROUP BY UV.sourceIP)
  ORDER BY totalRevenue DESC LIMIT 1
{% endhighlight %}

<table style="width:800px" class="table tight_rows">
  <tr><th></th>
      <th>Query 3A<br>485,312 rows</th>
      <th>Query 3B<br>53,332,015 rows</th>
      <th>Query 3C<br>533,287,121 rows</th>
  </tr>
  <tr>
    <th></th>
    <th>
      <script type="text/javascript">
        var three_a_data = [[42.12], [158.38], [74.32], [253.22], [130.34], [423.20]]
        var labels = ["redshift", "impala - disk", "impala - mem", "shark - disk", "shark - mem", "hive 0.10"];
        make_graph(three_a_data, labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        var three_b_data = [[46.75], [168.15], [90.3], [277.23], [171.94], [638.42]]
        var labels = ["redshift", "impala - disk", "impala - mem", "shark - disk", "shark - mem", "hive 0.10"];
        make_graph(three_b_data, labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        var three_c_data = [[199.74], [345.18], [337.84], [537.80], [447.30], [1822.44]]
        var labels = ["redshift", "impala - disk", "impala - mem", "shark - disk", "shark - mem", "hive 0.10"];
        make_graph(three_c_data, labels);
      </script>
    </th>
  </tr>
  <tr><td></td><td class="title-cell" colspan="3">Median Response Time (s)</td></tr>
  <tr><td>Redshift</td><td>42</td><td>47</td><td>200</td></tr>
  <tr><td>Impala - disk</td><td>158</td><td>168</td><td>345</td></tr>
  <tr><td>Impala - mem</td><td>74</td> <td>90</td><td>337</td></tr>
  <tr><td>Shark - disk</td><td>253</td><td>277</td><td>538</td></tr>
  <tr><td>Shark - mem</td><td>131</td><td>172</td><td>447</td></tr>
  <tr><td>Hive</td><td>423</td><td>638</td><td>1822</td></tr>
</table>


This query joins a smaller table to a larger table then sorts the results.

When the join is small (3A), all frameworks spend the majority of time scanning the large table and performing date comparisons. For larger joins, the initial scan becomes a less significant fraction of overall response time. For this reason the gap between in-memory and on-disk representations diminishes in query 3C. All frameworks perform partitioned joins to answer this query. CPU (due to hashing join keys) and network IO (due to shuffling data) are the primary bottlenecks. Redshift has an edge in this case because the overall network capacity in the cluster is higher.

<h4 id="query4">4. UDF Query</h4>
{% highlight mysql %}
CREATE TABLE url_counts_partial AS 
  SELECT TRANSFORM (line)
    USING "python /root/url_count.py" as (sourcePage, destPage, cnt) 
  FROM documents;
CREATE TABLE url_counts_total AS 
  SELECT SUM(cnt) AS totalCount, destPage 
  FROM url_counts_partial 
  GROUP BY destPage;

{% endhighlight %}

<table style="width:800px" class="table tight_rows">
  <tr>
    <th></th>
    <th>Query 4 (phase 1)</th>
    <th>Query 4 (phase 2)</th>
    <th>Query 4 (total)</th>
  </tr>
  <tr>
    <th></th>
    <th>
      <script type="text/javascript">
        var four_1_data = [[0], [0], [0], [582.95], [155.88], [659.08]];
        var labels = ["redshift", "impala - disk", "impala - mem", "shark - disk", "shark - mem", "hive 0.10"];
        make_graph(four_1_data, labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        var four_2_data = [[0], [0], [0], [132.72], [33.80], [358.38]];
        var labels = ["redshift", "impala - disk", "impala - mem", "shark - disk", "shark - mem", "hive 0.10"];
        make_graph(four_2_data, labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        var four_t_data = [[0], [0], [0], [715.66], [188.67], [1017.46]];
        var labels = ["redshift", "impala - disk", "impala - mem", "shark - disk", "shark - mem", "hive 0.10"];
        make_graph(four_t_data, labels);
      </script>
    </th>
  </tr>
  <tr><td></td><td class="title-cell" colspan="3">Median Response Time (s)</td></tr>
  <tr><td>Redshift</td><td><em>not supported</em></td><td>-</td><td>-</td></tr>
  <tr><td>Impala - mem</td><td><em>not supported</em></td><td>-</td><td>-</td></tr>
  <tr><td>Impala - disk</td><td><em>not supported</em></td><td>-</td><td>-</td></tr>
  <tr><td>Shark - mem</td><td>156</td><td>34</td><td>189</td></tr>
  <tr><td>Shark - disk</td><td>583</td><td>133</td><td>716</td></tr>
  <tr><td>Hive</td><td>659</td><td>358</td><td>1017</td></tr>
</table>

This query calls an external Python function which extracts and aggregates URL information from a web crawl dataset. It then aggregates a total count per URL.

Impala and Redshift do not currently support calling this type of UDF, so they are omitted from the result set. The performance advantage of Shark (disk) over Hive in this query is less pronounced than in 1, 2, or 3 because the shuffle and reduce phases take a relatively small amount of time (this query only shuffles a small amount of data) so the task-launch overhead of Hive is less pronounced. Also note that when the data is in-memory, Shark is bottlenecked by the speed at which it can pipe tuples to the Python process rather than memory throughput. This makes the speedup relative to disk around 5X (rather than 10X or more seen in other queries).

<h3 id="discussion">Discussion</h3>
These numbers compare performance on SQL workloads, but raw performance is just one of many important attributes of an analytic framework. The reason why systems like Hive, Impala, and Shark are used is because they offer a high degree of flexibility, both in terms of the underlying format of the data and the type of computation employed. Below we summarize a few qualitative points of comparison:

<table class="table">
  <tr>
      <th>System</th>
      <th>SQL variant</th>
      <th>Execution engine</th>
      <th>UDF Support</th>
      <th>Mid-query fault tolerance</th>
      <th>Open source</th>
      <th>Commercial support</th>
      <th>HDFS Compatible</th>
  </tr>
  <tr><td>Hive</td>
      <td>Hive QL (HQL)</td>
      <td>MapReduce</td>
      <td>Yes</td>
      <td>Yes</td>
      <td>Yes</td>
      <td>Yes</td>
      <td>Yes</td>
  </tr>
  <tr>
      <td>Shark</td>
      <td>Hive QL (HQL)</td>
      <td>Spark</td>
      <td>Yes</td>
      <td>Yes</td>
      <td>Yes</td>
      <td>No</td>
      <td>Yes</td>
  </tr>
  <tr>
      <td>Impala</td>
      <td>Some HQL + some extensions</td>
      <td>DBMS</td>
      <td>No</td>
      <td>No</td>
      <td>Yes</td>
      <td>Yes</td>
      <td>Yes</td>
  </tr>
  <tr><td>Redshift</td>
      <td>Full SQL 92 (?)</td>
      <td>DBMS</td>
      <td>No</td>
      <td>No</td>
      <td>No</td>
      <td>Yes</td>
      <td>No</td>
  </tr>
</table>


<h2 id="faq">FAQ</h2>
<h5>What's next?</h5>
We would like to include the columnar storage formats for Hadoop-based systems, such as [Parquet](http://blog.cloudera.com/blog/2013/03/introducing-parquet-columnar-storage-for-apache-hadoop/) and [RC file](http://en.wikipedia.org/wiki/RCFile). We would also like to run the suite at higher scale factors, using different types of nodes, and/or inducing failures during execution. Finally, we plan to re-evaluate on a regular basis as new versions are released.

We wanted to begin with a relatively well known workload, so we chose a variant of the Pavlo benchmark. This benchmark is heavily influenced by relational queries (SQL) and leaves out other types of analytics, such as machine learning and graph processing. The largest table also has fewer columns than in many modern RDBMS warehouses. In future iterations of this benchmark, we may extend the workload to address these gaps.

<h5>How is this different from the 2008 Pavlo et al. benchmark?</h5>
This benchmark is not an attempt to exactly recreate the environment of the Pavlo at al. benchmark. Instead, it
draws on that benchmark for inspiration in the dataset and workload. The most notable differences are as follows:



1. We run on a public cloud instead of using dedicated hardware.
1. We require the results are materialized to an output table. This is necessary because some queries in our version have results which do not fit in memory on one machine.
1. The dataset used for Query 4 is an actual web crawl rather than a synthetic one.
1. Query 4 uses a Python UDF instead of SQL/Java UDF's.
1. We create different permutations of queries 1-3. These permutations result in shorter or longer response times.  
1. The dataset is generated using the newer [Intel](https://github.com/intel-hadoop/HiBench) generator instead of the original C scripts. The newer tools are well supported and designed to output Hadoop datasets.

<h5>Did you consider comparing Vertica, Teradata, SAP Hana, MongoDB, Postgres, RAMCloud, SQLite, insert-dbms-or-query-engine-here... etc?</h5>

We've started with a small number of EC2-hosted query engines because our primary goal is producing verifiable results. Over time we'd like to grow the set of frameworks. We actively welcome contributions!

<h5>This workload doesn't represent queries I run -- how can I test these frameworks on my own workload?</h5>

We've tried to cover a set of fundamental operations in this benchmark, but of course, it may not correspond to your own workload. The prepare scripts provided with this benchmark will load sample data sets into each framework. From there, you are welcome to run your own types of queries against these tables. Because these are all easy to launch on EC2, you can also load your own datasets.

<h5>Do these queries take advantage of data-layout options, such as Hive/Impala/Shark partitions or Redshift sort columns?</h5>

For now, no. The idea is to test "out of the box" performance on these queries even if you haven't done a bunch of up-front work at the loading stage to optimize for specific access patterns. We may relax this requirement in the future.

<h5>Why didn't you test Hive in memory?</h5>
We did, but the results were very hard to stabilize. The reason is that it is hard to coerce the entire input into the buffer cache because of the way Hive uses HDFS: Each file in HDFS has three replicas and Hive's underlying scheduler may choose to launch a task at any replica on a given run. As a result, you would need 3X the amount of buffer cache (which exceeds the capacity in these clusters) and or need to have precise control over which node runs a given task (which is not offered by the MapReduce scheduler).


<h2 id="contributions">Contributing a New Framework</h2>
We plan to run this benchmark regularly and may introduce additional workloads over time. We welcome the addition of new frameworks as well. The only requirement is that running the benchmark be reproducible and verifiable in similar fashion to those already included. The best place to start is by contacting [Patrick Wendell](mailto:pwendell@gmail.com) from the U.C. Berkeley AMPLab.

<h2 id="running">Run This Benchmark Yourself</h2>
_Since Redshift, Shark, Hive, and Impala all provide tools to easily provision a cluster on EC2, this benchmark can be easily replicated._

### Hosted data sets
To allow this benchmark to be easily reproduced, we've prepared various sizes of the input dataset in S3. The scale factor is defined such that each node in a cluster of the given size will hold ~25GB of the `UserVisits` table, ~1GB of the `Rankings` table, and ~30GB of the web crawl, uncompressed. The datasets are encoded in `TextFile` and `SequenceFile` format along with corresponding compressed versions. They are available publicly at `s3n://big-data-benchmark/pavlo/[text|text-deflate|sequence|sequence-snappy]/[suffix]`.

<table class="table table-hover">
  <tr>
    <th>S3 Suffix</th><th>Scale Factor</th>
    <th markdown="1">`Rankings` (rows)</th>
    <th markdown="1">`Rankings` (bytes)</th>
    <th markdown="1">`UserVisits` (rows)</th>
    <th markdown="1">`UserVisits` (bytes)</th>
    <th markdown="1">`Documents` (bytes)</th>
  </tr>
  <tr>
    <td>/tiny/</td>
    <td>small</td>
    <td>1200</td><td>77.6KB</td>
    <td>10000</td><td>1.7MB</td>
    <td>6.8MB</td>
  </tr>
  <tr>
    <td>/1node/</td>
    <td>1</td>
    <td>18 Million</td><td>1.28GB</td>
    <td>155 Million</td><td>25.4GB</td>
    <td>29.0GB</td>
  </tr>
  <tr>
    <td>/5nodes/</td>
    <td>5</td>
    <td>90 Million</td><td>6.38GB</td>
    <td>775 Million</td><td>126.8GB</td>
    <td>136.9GB</td>
  </tr>
</table>



### Launching and Loading Clusters

1. Create an Impala, Redshift, Hive or Shark cluster using their provided provisioning tools.
  * Each cluster should be created in the US East EC2 Region
  * For Redshift, use the [Amazon AWS console](https://console.aws.amazon.com/redshift/). Make sure to whitelist the node you plan to run the benchmark from in the Redshift control panel.
  * For Impala and Hive, use the [Cloudera Manager EC2 deployment instructions](http://blog.cloudera.com/blog/2013/03/how-to-create-a-cdh-cluster-on-amazon-ec2-via-cloudera-manager/). Make sure to upload your own RSA key so that you can use the same key to log into the nodes and run queries.
  * For Shark, use the [Spark/Shark EC2 launch scripts](http://spark-project.org/docs/latest/ec2-scripts.html). These are available as part of the latest Spark distribution.
  * {% highlight bash %}
$> ec2/spark-ec2 -s 5 -k [KEY PAIR NAME] -i [IDENTITY FILE] --hadoop-major-version=2 -t "m2.4xlarge" launch [SECURITY GROUP] {% endhighlight %} **NOTE:** You must set **AWS\_ACCESS\_KEY\_ID** and **AWS\_SECRET\_ACCESS\_KEY** environment variables.

2. Scripts for preparing data are included in the [benchmark github repo](https://github.com/amplab/benchmark.git). Use the provided `prepare-benchmark.sh` to load an appropriately sized dataset into the cluster. <br><br> `./prepare-benchmark.sh --help`

Here are a few examples showing the options used in this benchmark...

<table style="width:1000px;margin-top:20px;">
  <tr>
    <th>Redshift</th>
    <th>Shark</th>
    <th>Impala/Hive</th>
  </tr>
<tr valign="top">
<td>
{% highlight bash %}
$> ./prepare-benchmark.sh
  --redshift
  --aws-key-id=[AWS KEY ID]
  --aws-key=[AWS KEY]
  --redshift-username=[USERNAME]
  --redshift-password=[PASSWORD]
  --redshift-host=[ODBC HOST]
  --redshift-database=[DATABASE]
  --scale-factor=5

{% endhighlight %}
</td><td>
{% highlight bash %}
$> ./prepare-benchmark.sh
  --shark
  --aws-key-id=[AWS KEY ID]
  --aws-key=[AWS KEY]
  --shark-host=[SHARK MASTER]
  --shark-identity-file=[IDENTITY FILE]
  --scale-factor=5
  --file-format=text-deflate

{% endhighlight %}
</td><td>
{% highlight bash %}
$> ./prepare-benchmark.sh
  --impala
  --aws-key-id=[AWS KEY ID]
  --aws-key=[AWS KEY]
  --impala-host=[NAME NODE]
  --impala-identity-file=[IDENTITY FILE]
  --scale-factor=5
  --file-format=sequence-snappy

{% endhighlight %}
</td></tr>

<tr valign="top">
<td>
{% highlight bash %}
$> ./run-query.sh
--redshift
--redshift-username=[USERNAME]
--redshift-password=[PASSWORD]
--redshift-host=[ODBC HOST]
--redshift-database=[DATABASE]
--query-num=[QUERY NUM]
{% endhighlight %}
</td><td>
{% highlight bash %}
$> ./run-query.sh
--shark
--shark-host=[SHARK MASTER]
--shark-identity-file=[IDENTITY FILE]
--query-num=[QUERY NUM]
{% endhighlight %}
</td><td>
{% highlight bash %}
$> ./run-query.sh
--impala
--impala-hosts=[COMMA SEPARATED LIST OF IMPALA NODES]
--impala-identity-file=[IDENTITY FILE]
--query-num=[QUERY NUM]
{% endhighlight %}
</td></tr>

</table>

<ol>
<li value="3"> If you are adding a new framework or using this to produce your own scientific performance numbers, get in touch with us. The virtualized environment of EC2 makes eeking out the best results a bit tricky. We can help.
</li>
</ol>
