---
title: Big Data Benchmark
layout: default
---

<!-- This is an open source benchmark which compares the performance of several large scale data-processing frameworks. -->

<script type="text/javascript">
  // Ordering = 1a, 1b, 1c, 2a, 2b, 2c, 3a, 3b, 3c, 4, 4a, 4b

  // Current Runs
  var labels = [["Redshift (HDD)", "Current"], ["Impala - Disk", "1.2.3"], ["Impala - Mem", "1.2.3"], ["Shark - Disk", "0.8.1"], ["Shark - Mem", "0.8.1"], ["Hive", "0.12 YARN"], ["Tez", "0.2.0"]];
  var redshift = [[2.49],[2.61],[9.46],[25.46],[56.51],[79.15],[33.29],[46.08],[168.25], ["not supported"], ["not supported"], ["not supported"]];
  var impala_disk = [[12.015],[12.015],[37.085],[113.72],[155.31],[277.53],[108.68],[129.815],[431.26]];
  impala_mem = [[2.17],[3.01],[36.04],[84.35],[134.82],[261.015],[41.21],[76.005],[386.6]]
  var shark_disk = [[6.6],[7.0],[22.4],[151.4],[164.3],[196.5],[111.7],[135.6],[382.6],[279.4],[232.2],[47.2]];
  var shark_mem = [[1.7],[1.8],[3.6],[83.7],[100.1],[132.6],[44.7],[67.3],[318.0],[191.4],[162.9],[28.1]];
  var hive_12_warmup = [[50.49],[59.93],[43.34],[730.62],[764.95],[833.30],[561.14],[717.56],[2374.17],[1047.45], [896.47], [150.48]];
  var tez = [[28.22],[36.35],[26.44],[377.48],[438.03],[427.56],[323.06],[402.33],[1361.90],[966.18], [894.16], [62.60]];

  var impala_disk_table = [ [12.015],[12.015],[37.085],[113.72],[155.31],[277.53],[108.68],[129.815], [431.26], ["untested"], ["untested"], ["untested"]];
  var impala_mem_table = [[2.17],[3.01],[36.04],[84.35],[134.82],[261.015],[41.21],[76.005], [386.6], ["untested"], ["untested"], ["untested"]];

  // Old Runs
  var old_labels = [["Redshift (HDD)", "Old"], ["Impala - Disk", "1.0"], ["Impala - Mem", "1.0"], ["Shark - Disk", "0.7.3"], ["Shark - Mem", "0.7.3"]];
  var redshift_old = [[2.4],[2.5],[12.2],[28],[65],[92],[42],[47],[200]]
  var impala_disk_old = [[9.9],[12],[104],[130],[216],[565],[158],[168],[345]];
  var impala_mem_old = [[0.75],[4.48],[108],[121],[208],[557],[74],[90],[337]];
  var shark_disk_old = [[11.8],[11.9],[24.9],[210],[238],[279],[253],[277],[538],[716],[583],[133]];
  var shark_mem_old = [[1.1],[1.1],[3.5],[111],[141],[156],[131],[172],[447],[189],[156],[34]];

  function get_data(index) {
    return [[redshift[index]],[impala_disk[index]],[impala_mem[index]],[shark_disk[index]],[shark_mem[index]],[hive_12_warmup[index]],[tez[index]]];
  }

  function get_olddata(index) {
    return [[redshift_old[index]],[impala_disk_old[index]],[impala_mem_old[index]],[shark_disk_old[index]],[shark_mem_old[index]]];
  }

  function get_q4_data(index) {
    return [[0],[0],[0],[shark_disk[index]],[shark_mem[index]],[hive_12_warmup[index]],[tez[index]]];
  }

  function get_q4_olddata(index) {
    return [[0],[0],[0],[shark_disk_old[index]],[shark_mem_old[index]]];
  }

  function write_table(query, a, b, c) {
    var table = $("#" + query);
    $("." + query + "candestroy").remove();

    table.append("<tr class=\"" + query + "candestroy" + "\"><td><button class=\"swap" + query + "\">Old Data</button></td><td class=\"title-cell\" colspan=\"3\">Median Response Time (s)</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[0][0] + " - " + labels[0][1] + "</td><td>" + redshift[a] + "</td><td>" + redshift[b] + "</td><td>" + redshift[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[1][0] + " - " + labels[1][1] + "</td><td>" + impala_disk_table[a] + "</td><td>" + impala_disk_table[b] + "</td><td>" + impala_disk_table[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[2][0] + " - " + labels[2][1] + "</td><td>" + impala_mem_table[a] + "</td><td>" + impala_mem_table[b] + "</td><td>" + impala_mem_table[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[3][0] + " - " + labels[3][1] + "</td><td>" + shark_disk[a] + "</td><td>" + shark_disk[b] + "</td><td>" + shark_disk[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[4][0] + " - " + labels[4][1] + "</td><td>" + shark_mem[a] + "</td><td>" + shark_mem[b] + "</td><td>" + shark_mem[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[5][0] + " - " + labels[5][1] + "</td><td>" + hive_12_warmup[a] + "</td><td>" + hive_12_warmup[b] + "</td><td>" + hive_12_warmup[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[6][0] + " - " + labels[6][1] + "</td><td>" + tez[a] + "</td><td>" + tez[b] + "</td><td>" + tez[c] + "</td></tr>")

    var swap = $(".swap" + query);
    swap.unbind("click");
    swap.bind("click", function() { write_old_table(query, a, b, c) } );
  }

  function write_old_table(query, a, b, c) {
    var table = $("#" + query);
    $("." + query + "candestroy").remove();
    table.append("<tr class=\"" + query + "candestroy" + "\"><td><button class=\"swap" + query + "\" >Current Data</button></td><td class=\"title-cell\" colspan=\"3\">Old vs Current Benchmark (s)</td></tr>")
    table.append("<tr class=\"" + query + "candestroy olddata" + "\"><td nowrap>" + old_labels[0][0] + " - " + old_labels[0][1] + "</td><td>" + redshift_old[a] + "</td><td>" + redshift_old[b] + "</td><td>" + redshift_old[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[0][0] + " - " + labels[0][1] + "</td><td>" + redshift[a] + "</td><td>" + redshift[b] + "</td><td>" + redshift[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy olddata" + "\"><td nowrap>" + old_labels[1][0] + " - " + old_labels[1][1] + "</td><td>" + impala_disk_old[a] + "</td><td>" + impala_disk_old[b] + "</td><td>" + impala_disk_old[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[1][0] + " - " + labels[1][1] + "</td><td>" + impala_disk_table[a] + "</td><td>" + impala_disk_table[b] + "</td><td>" + impala_disk_table[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy olddata" + "\"><td nowrap>" + old_labels[2][0] + " - " + old_labels[2][1] + "</td><td>" + impala_mem_old[a] + "</td><td>" + impala_mem_old[b] + "</td><td>" + impala_mem_old[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[2][0] + " - " + labels[2][1] + "</td><td>" + impala_mem_table[a] + "</td><td>" + impala_mem_table[b] + "</td><td>" + impala_mem_table[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy olddata" + "\"><td nowrap>" + old_labels[3][0] + " - " + old_labels[3][1] + "</td><td>" + shark_disk_old[a] + "</td><td>" + shark_disk_old[b] + "</td><td>" + shark_disk_old[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[3][0] + " - " + labels[3][1] + "</td><td>" + shark_disk[a] + "</td><td>" + shark_disk[b] + "</td><td>" + shark_disk[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy olddata" + "\"><td nowrap>" + old_labels[4][0] + " - " + old_labels[4][1] + "</td><td>" + shark_mem_old[a] + "</td><td>" + shark_mem_old[b] + "</td><td>" + shark_mem_old[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[4][0] + " - " + labels[4][1] + "</td><td>" + shark_mem[a] + "</td><td>" + shark_mem[b] + "</td><td>" + shark_mem[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[5][0] + " - " + labels[5][1] + "</td><td>" + hive_12_warmup[a] + "</td><td>" + hive_12_warmup[b] + "</td><td>" + hive_12_warmup[c] + "</td></tr>")
    table.append("<tr class=\"" + query + "candestroy" + "\"><td nowrap>" + labels[6][0] + " - " + labels[6][1] + "</td><td>" + tez[a] + "</td><td>" + tez[b] + "</td><td>" + tez[c] + "</td></tr>")

    var swap = $(".swap" + query);
    swap.unbind("click");
    swap.bind("click", function() { write_table(query, a, b, c) });
  }

  $(window).bind("load", function() { write_table("Q1", 0, 1, 2) });
  $(window).bind("load", function() { write_table("Q2", 3, 4, 5) });
  $(window).bind("load", function() { write_table("Q3", 6, 7, 8) });
  $(window).bind("load", function() { write_table("Q4", 10, 11, 9) });
</script>

***Click [Here](v1) for the previous version of the benchmark***

<h2 id="introduction">Introduction</h2>

Several analytic frameworks have been announced in the last year. Among them are inexpensive data-warehousing solutions based on traditional Massively Parallel Processor (MPP) architectures ([Redshift](http://aws.amazon.com/redshift/)), systems which impose MPP-like execution engines on top of Hadoop ([Impala](http://blog.cloudera.com/blog/2012/10/cloudera-impala-real-time-queries-in-apache-hadoop-for-real/), [HAWQ](http://www.greenplum.com/news/press-release/emc-introduces-worlds-most-powerful-hadoop-distribution-pivotal-hd)) and systems which optimize MapReduce to improve performance on analytical workloads ([Shark](http://shark.cs.berkeley.edu/), [Stinger/Tez](http://hortonworks.com/blog/100x-faster-hive/)). This benchmark provides [quantitative](#results) and [qualitative](#discussion) comparisons of five systems. It is entirely hosted on EC2 and can be reproduced directly from your computer.

* [Redshift](http://aws.amazon.com/redshift/) - a hosted MPP database offered by Amazon.com based on the ParAccel data warehouse. We tested Redshift on HDDs.
* [Hive](http://hive.apache.org/) - a Hadoop-based data warehousing system. (v0.12)
* [Shark](http://shark.cs.berkeley.edu/) - a Hive-compatible SQL engine which runs on top of the [Spark](http://spark-project.org) computing framework. (v0.8.1)
* [Impala](http://blog.cloudera.com/blog/2012/10/cloudera-impala-real-time-queries-in-apache-hadoop-for-real/) - a Hive-compatible[\*](#discussion) SQL engine with its own MPP-like execution engine. (v1.2.3)
* [Stinger/Tez](http://hortonworks.com/blog/announcing-stinger-phase-3-technical-preview) - Tez is a next generation Hadoop execution engine currently in development (v0.2.0)

This remains a  _**work in progress**_ and will evolve to include additional frameworks and new capabilities. We welcome <a href="#contributions">contributions</a>.

### What this benchmark is not
This benchmark is not intended to provide a comprehensive overview of the tested platforms. We are aware that by choosing default configurations we have excluded many optimizations. Instead we target a simple comparison between these systems with the goal that the results are __understandable and reproducible__.

### What is being evaluated?
This benchmark measures response time on a handful of relational queries: scans, aggregations, joins, and UDF\'s, across different data sizes. Keep in mind that these systems have very different sets of capabilities. MapReduce-like systems (Shark/Hive) target flexible and large-scale computation, supporting complex User Defined Functions (UDF\'s), tolerating failures, and scaling to thousands of nodes. Traditional MPP databases are strictly SQL compliant and heavily optimized for relational queries. The workload here is simply one set of queries that most of these systems these can complete.

### Changes and Notes (February 2014)

* We changed the Hive configuration from Hive 0.10 on CDH4 to Hive 0.12 on HDP 2.0.6. As a result, direct comparisons between the current and previous Hive results should not be made. It is difficult to account for changes resulting from modifications to Hive as opposed to changes in the underlying Hadoop distribution.
* We have added [Tez](http://hortonworks.com/blog/announcing-stinger-phase-3-technical-preview/) as a supported platform. __It is important to note that Tez is currently in a preview state__.
* Hive has improved its query optimization, which is also inherited by Shark. This set of queries does not test the improved optimizer.
* We have changed the underlying filesystem from Ext3 to Ext4 for Hive, Tez, Impala, and Shark benchmarking.



 
<h3 id="workload">Dataset and Workload</h3>
Our dataset and queries are inspired by the benchmark contained in [a comparison of approaches to large scale analytics](http://database.cs.brown.edu/sigmod09/benchmarks-sigmod09.pdf). The input data set consists of a set of unstructured HTML documents and two SQL tables which contain summary information. It was generated using [Intel\'s Hadoop benchmark tools](https://github.com/intel-hadoop/HiBench) and data sampled from the [Common Crawl](http://commoncrawl.org) document corpus. There are three datasets with the following schemas:

<div class="span11" style="float: none; margin-top: 5px; margin-bottom: 5px; margin-left: auto; margin-right: auto;">
<table padding="10" id="inputSchema">
  <tr> 
    <th markdown="1">`Documents`</th>
    <th markdown="1">`Rankings`</th>
    <th markdown="1">`UserVisits`</th>
  </tr>
  <tr>
    <td style="padding-left: 20px; padding-right: 20px;" markdown="1">
      _Unstructured HTML documents_
    </td>
    <td style="padding-left: 20px; padding-right: 20px;" markdown="1">
      _Lists websites and their page rank_
    </td>
    <td style="padding-left: 20px; padding-right: 20px;" markdown="1">
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
    * Variant A: __BI-Like__ - result sets are small (e.g., could fit in memory in a BI tool)
    * Variant B: __Intermediate__ - result set may not fit in memory on one node
    * Variant C: __ETL-Like__ - result sets are large and require several nodes to store

 * [Query 3](#query3) is a join query with a small result set, but varying sizes of joins.

 * [Query 4](#query4) is a bulk UDF query. It calculates a simplified version of PageRank using a sample of the [Common Crawl](http://commoncrawl.org) dataset.

<h4 class="clickable collapsed" data-toggle="collapse" data-target="#hardware-div" id="hardware">Hardware Configuration <img src="media/toggle.gif"/></h4>
<div id="hardware-div" class="collapse">
For Impala, Hive, Tez, and Shark, this benchmark uses the m2.4xlarge EC2 instance type. Redshift only has very small and very large instances, so rather than compare identical hardware, we <em>fix the cost</em> of the cluster and opt to purchase a larger number of small nodes for Redshift. We use a scale factor of 5 for the experiments in all cases.

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
    <td>Impala, Hive, Tez, Shark</td>
    <td>m2.4xlarge</td>
    <td>68.4 GB</td>
    <td>1680GB (2HDD)</td>
    <td>8</td>
    <td>1.640</td>
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
    <td>Impala, Hive, Tez, Shark</td>
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


<h3 id="results"> Results | February 2014</h3>

We launch EC2 clusters and run each query several times. We report the median response time here. Except for Redshift, all data is stored on HDFS in compressed SequenceFile format. Each query is run with seven frameworks:

<table class="table tight_rows" markdown="1">

<tr><td markdown="1">__Redshift__</td><td>Amazon Redshift with default options.</td></tr>
<tr><td markdown="1">__Shark - disk__</td><td>Input and output tables are on-disk compressed with gzip. OS buffer cache is cleared before each run.</td></tr>
<tr><td markdown="1">__Impala - disk__</td><td>Input and output tables are on-disk compressed with snappy. OS buffer cache is cleared before each run.</td></tr>
<tr><td markdown="1">__Shark - mem__</td><td>Input tables are stored in Spark cache. Output tables are stored in Spark cache.</td></tr>
<tr><td markdown="1">__Impala - mem__</td><td>Input tables are coerced into the OS buffer cache. Output tables are on disk (Impala has no notion of a cached table).</td></tr>
<tr><td markdown="1">__Hive__</td><td>Hive on HDP 2.0.6 with default options. Input and output tables are on disk compressed with snappy. OS buffer cache is cleared before each run.</td></tr>
<tr><td markdown="1">__Tez__</td><td markdown="1">Tez with the configuration parameters specified [here](http://public-repo-1.hortonworks.com/HDP-LABS/Projects/Stinger/StingerTechnicalPreviewInstall.pdf). Input and output tables are on disk compressed with snappy. OS buffer cache is cleared before each run.</td></tr>
</table>

<h4 id="query1">1. Scan Query </h4>

{% highlight mysql %}
SELECT pageURL, pageRank FROM rankings WHERE pageRank > X
{% endhighlight %}

<table style="width:800px" class="table tight_rows" id="Q1">
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
        index = 0;
        make_graph(get_data(index), get_olddata(index), labels, old_labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        index = 1;
        make_graph(get_data(index), get_olddata(index), labels, old_labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        index = 2;
        make_graph(get_data(index), get_olddata(index), labels, old_labels);
      </script>
    </th>
  </tr>
</table>

This query scans and filters the dataset and stores the results.

This query primarily tests the throughput with which each framework can read and write table data. The best performers are Impala (mem) and Shark (mem) which see excellent throughput by avoiding disk. For on-disk data, Redshift sees the best throughput for two reasons. First, the Redshift clusters have more disks and second, Redshift uses columnar compression which allows it to bypass a field which is not used in the query. Shark and Impala scan at HDFS throughput with fewer disks.

Both Shark and Impala outperform Hive by 3-4X due in part to more efficient task launching and scheduling. As the result sets get larger, Impala becomes bottlenecked on the ability to persist the results back to disk. Nonetheless, since the last iteration of the benchmark Impala has improved its performance in materializing these large result-sets to disk.

Tez sees about a 40% improvement over Hive in these queries. This is in part due to the container pre-warming and reuse, which cuts down on JVM initialization time.

<h4 id="query2">2. Aggregation Query</h4>

{% highlight mysql %}
SELECT SUBSTR(sourceIP, 1, X), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, X)
{% endhighlight %}

<table style="width:800px" class="table tight_rows" id="Q2">
  <tr><th></th>
      <th>Query 2A<br>2,067,313 groups</th>
      <th>Query 2B<br>31,348,913 groups</th>
      <th>Query 2C<br>253,890,330 groups</th>
  </tr>
  <tr>
    <th></th>
    <th>
      <script type="text/javascript">
        index = 3;
        make_graph(get_data(index), get_olddata(index), labels, old_labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        index = 4;
        make_graph(get_data(index), get_olddata(index), labels, old_labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        index = 5;
        make_graph(get_data(index), get_olddata(index), labels, old_labels);
      </script>
    </th>
  </tr>
</table>

This query applies string parsing to each input tuple then performs a high-cardinality aggregation.

Redshift\'s columnar storage provides greater benefit than in Query 1 since several columns of the `UserVistits` table are un-used. While Shark\'s in-memory tables are also columnar, it is bottlenecked here on the speed at which it evaluates the `SUBSTR` expression. Since Impala is reading from the OS buffer cache, it must read and decompress entire rows. Unlike Shark, however, Impala evaluates this expression using very efficient compiled code. These two factors offset each other and Impala and Shark achieve roughly the same raw throughput for in memory tables. For larger result sets, Impala again sees high latency due to the speed of materializing output tables.

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

<table style="width:800px" class="table tight_rows" id="Q3">
  <tr><th></th>
      <th>Query 3A<br>485,312 rows</th>
      <th>Query 3B<br>53,332,015 rows</th>
      <th>Query 3C<br>533,287,121 rows</th>
  </tr>
  <tr>
    <th></th>
    <th>
      <script type="text/javascript">
        index = 6;
        make_graph(get_data(index), get_olddata(index), labels, old_labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        index = 7;
        make_graph(get_data(index), get_olddata(index), labels, old_labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        index = 8;
        make_graph(get_data(index), get_olddata(index), labels, old_labels);
      </script>
    </th>
  </tr>
</table>


This query joins a smaller table to a larger table then sorts the results.

When the join is small (3A), all frameworks spend the majority of time scanning the large table and performing date comparisons. For larger joins, the initial scan becomes a less significant fraction of overall response time. For this reason the gap between in-memory and on-disk representations diminishes in query 3C. All frameworks perform partitioned joins to answer this query. CPU (due to hashing join keys) and network IO (due to shuffling data) are the primary bottlenecks. Redshift has an edge in this case because the overall network capacity in the cluster is higher.

<h4 id="query4">4. External Script Query</h4>
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

<table style="width:800px" class="table tight_rows" id="Q4">
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
        index = 10;
        make_graph(get_q4_data(index), get_q4_olddata(index), labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        index = 11;
        make_graph(get_q4_data(index), get_q4_olddata(index), labels);
      </script>
    </th>
    <th>
      <script type="text/javascript">
        index = 9;
        make_graph(get_q4_data(index), get_q4_olddata(index), labels);
      </script>
    </th>
  </tr>
</table>

This query calls an external Python function which extracts and aggregates URL information from a web crawl dataset. It then aggregates a total count per URL.

Impala and Redshift do not currently support calling this type of UDF, so they are omitted from the result set. Impala UDFs must be written in Java or C++, where as this script is written in Python. The performance advantage of Shark (disk) over Hive in this query is less pronounced than in 1, 2, or 3 because the shuffle and reduce phases take a relatively small amount of time (this query only shuffles a small amount of data) so the task-launch overhead of Hive is less pronounced. Also note that when the data is in-memory, Shark is bottlenecked by the speed at which it can pipe tuples to the Python process rather than memory throughput. This makes the speedup relative to disk around 5X (rather than 10X or more seen in other queries).

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
  <tr><td>Tez</td>
      <td>Hive QL (HQL)</td>
      <td>Tez</td>
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
      <td>Yes</td>
      <td>Yes</td>
  </tr>
  <tr>
      <td>Impala</td>
      <td>Some HQL + some extensions</td>
      <td>DBMS</td>
      <td>Yes (Java/C++)</td>
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
1. Query 4 uses a Python UDF instead of SQL/Java UDF\'s.
1. We create different permutations of queries 1-3. These permutations result in shorter or longer response times.  
1. The dataset is generated using the newer [Intel](https://github.com/intel-hadoop/HiBench) generator instead of the original C scripts. The newer tools are well supported and designed to output Hadoop datasets.

<h5>Did you consider comparing Vertica, Teradata, SAP Hana, MongoDB, Postgres, RAMCloud, SQLite, insert-dbms-or-query-engine-here... etc?</h5>

We\'ve started with a small number of EC2-hosted query engines because our primary goal is producing verifiable results. Over time we\'d like to grow the set of frameworks. We actively welcome contributions!

<h5>This workload doesn't represent queries I run -- how can I test these frameworks on my own workload?</h5>

We\'ve tried to cover a set of fundamental operations in this benchmark, but of course, it may not correspond to your own workload. The prepare scripts provided with this benchmark will load sample data sets into each framework. From there, you are welcome to run your own types of queries against these tables. Because these are all easy to launch on EC2, you can also load your own datasets.

<h5>Do these queries take advantage of different Hadoop file formats or data-layout options, such as Hive/Impala/Shark partitions or Redshift sort columns?</h5>

For now, no. The idea is to test \"out of the box\" performance on these queries even if you haven\'t done a bunch of up-front work at the loading stage to optimize for specific access patterns. For this reason we have opted to use simple storage formats across Hive, Impala and Shark benchmarking.

That being said, it is important to note that the various platforms optimize different use cases. As it stands, only Redshift can take advantage of its columnar compression. However, the other platforms could see improved performance by utilizing a columnar storage format. Specifically, Impala is likely to benefit from the usage of the Parquet columnar file format.

We may relax these requirements in the future.

<h5>Why didn't you test Hive in memory?</h5>
We did, but the results were very hard to stabilize. The reason is that it is hard to coerce the entire input into the buffer cache because of the way Hive uses HDFS: Each file in HDFS has three replicas and Hive\'s underlying scheduler may choose to launch a task at any replica on a given run. As a result, you would need 3X the amount of buffer cache (which exceeds the capacity in these clusters) and or need to have precise control over which node runs a given task (which is not offered by the MapReduce scheduler).


<h2 id="contributions">Contributing a New Framework</h2>
We plan to run this benchmark regularly and may introduce additional workloads over time. We welcome the addition of new frameworks as well. The only requirement is that running the benchmark be reproducible and verifiable in similar fashion to those already included. The best place to start is by contacting [Patrick Wendell](mailto:pwendell@gmail.com) from the U.C. Berkeley AMPLab.

<h2 id="running">Run This Benchmark Yourself</h2>
_Since Redshift, Shark, Hive, and Impala all provide tools to easily provision a cluster on EC2, this benchmark can be easily replicated._

### Hosted data sets
To allow this benchmark to be easily reproduced, we\'ve prepared various sizes of the input dataset in S3. The scale factor is defined such that each node in a cluster of the given size will hold ~25GB of the `UserVisits` table, ~1GB of the `Rankings` table, and ~30GB of the web crawl, uncompressed. The datasets are encoded in `TextFile` and `SequenceFile` format along with corresponding compressed versions. They are available publicly at `s3n://big-data-benchmark/pavlo/[text|text-deflate|sequence|sequence-snappy]/[suffix]`.

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

1. Create an Impala, Redshift, Hive/Tez or Shark cluster using their provided provisioning tools.
  * Each cluster should be created in the US East EC2 Region
  * For Redshift, use the [Amazon AWS console](https://console.aws.amazon.com/redshift/). Make sure to whitelist the node you plan to run the benchmark from in the Redshift control panel.
  * For Impala, use the [Cloudera Manager EC2 deployment instructions](http://blog.cloudera.com/blog/2013/03/how-to-create-a-cdh-cluster-on-amazon-ec2-via-cloudera-manager/). Make sure to upload your own RSA key so that you can use the same key to log into the nodes and run queries.
      * __Note:__ In order to use Ext4 as the underlying file system additional steps must be taken on each host machine. See the Ext4 section below.
  * For Shark, use [Spark/Shark EC2 launch scripts](http://spark-project.org/docs/latest/ec2-scripts.html). These are available as part of the latest Spark distribution.
      * __Note:__ In order to use the same settings that were used in the benchmark, such as Ext4, you must make a modification to the Spark EC2 script. See the Ext4 section below.
  {% highlight bash %}
    $> ec2/spark-ec2 -s 5 -k [KEY PAIR NAME] -i [IDENTITY FILE] --hadoop-major-version=2 -t "m2.4xlarge" launch [CLUSTER NAME] {% endhighlight %} **NOTE:** You must set **AWS\_ACCESS\_KEY\_ID** and **AWS\_SECRET\_ACCESS\_KEY** environment variables.

  * For Hive and Tez, use the following instructions to launch a cluster

#### Using Ext4

##### Shark

Modify ec2/spark_ec2.py:

{% highlight bash %}
Change: ssh(master, opts, "rm -rf spark-ec2 && git clone https://github.com/mesos/spark-ec2.git -b v2")
To:     ssh(master, opts, "rm -rf spark-ec2 && git clone https://github.com/ahirreddy/spark-ec2.git -b ext4-update")
{% endhighlight %}

##### Impala

Run the following commands on each node provisioned by the Cloudera Manager. These commands must be issued after an instance is provisioned but before services are installed.
{% highlight bash %}
  dev=/dev/xvdb
  sudo umount $dev
  sudo mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 $dev
  sudo mount -o defaults,noatime,nodiratime $dev

  dev=/dev/xvdc
  sudo mkdir /data0
  sudo mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 $dev
  sudo mount -o defaults,noatime,nodiratime $dev
  sudo mount -t ext4 -o defaults,noatime,nodiratime $dev /data0
{% endhighlight %}

##### Hive/Tez

By default our HDP launch scripts will format the underlying filesystem as Ext4, no additional steps are required.

#### Launching Hive and Tez Clusters
This command will launch and configure the specified number of slaves in addition to a Master and an Ambari host.
    {% highlight bash %}
          $> AWS_ACCESS_KEY_ID=[AWS ID] AWS_SECRET_ACCESS_KEY=[AWS SECRET]
          ./prepare-hdp.sh --slaves=N --key-pair=[INSTANCE KEYPAIR]
          --identity-file=[SSH PRIVATE KEY] --instance-type=[INSTANCE TYPE]
          launch [CLUSTER NAME]{% endhighlight %}

Once complete, it will report both the internal and external hostnames of each node.

  1. SSH into the Ambari node as root and run `ambari-server start`
  2. Visit port 8080 of the Ambari node and login (by default, both username and password are `admin`) to begin cluster setup.
  3. When prompted to enter hosts, you must use the interal EC2 hostnames.
  4. Install all services, and take care to install all master services on the node designated as master (not the Ambari host) by the setup script.
  5. This installation should take 10-20 minutes. Load the benchmark data once it is complete.

To install Tez on this cluster, use the following command. It will remove the ability to use normal Hive.
  {% highlight bash %}
    $> ./prepare-benchmark.sh --hive-tez --hive-host [MASTER REPORTED BY SETUP
    SCRIPT] --hive-identity-file [SSH PRIVATE KEY]{% endhighlight %}

#### Loading Benchmark Data

Scripts for preparing data are included in the [benchmark github repo](https://github.com/amplab/benchmark.git). Use the provided `prepare-benchmark.sh` to load an appropriately sized dataset into the cluster. <br><br> `./prepare-benchmark.sh --help`

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

<table style="width:1000px;margin-top:20px;table-layout: fixed;">
  <tr>
    <th>Hive/Tez</th>
  </tr>
<tr valign="top">
<td>
{% highlight bash %}
$> ./prepare-benchmark.sh
--hive
--hive-host [MASTER REPORTED BY SETUP SCRIPT]
--hive-slaves [COMMA SEPARATED LIST OF SLAVES]
--hive-identity-file [SSH PRIVATE KEY]
-d [AWS ID]
-k [AWS SECRET]
--file-format=sequence-snappy
--scale-factor=5
{% endhighlight %}
</td><td>
</td><td>
</td></tr>

</table>

<ol>
If you are adding a new framework or using this to produce your own scientific performance numbers, get in touch with us. The virtualized environment of EC2 makes eeking out the best results a bit tricky. We can help.
</ol>
