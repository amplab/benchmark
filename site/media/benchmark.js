    $( document ).ready(function() {
      $("a").on("click", function() {
        // http://stackoverflow.com/questions/9047703/fixed-position-navbar-obscures-anchors
        fromTop = 40;
        href = $(this).attr("href");

        // If href is set, points to an Anchor, and the Anchor is not simply #
        if(href && href.indexOf("#") != -1 && href.indexOf("#") != href.length - 1) {
          href = href.substring(href.indexOf("#"));
          if($(href).length > 0) { // If element exists
            $('html, body').animate({scrollTop: $(href).offset().top - fromTop}, 400);
            return false;
          }
        }
      });
    });

function make_graph(data_in, alt_data, labels, alt_labels) {
  var max_value = 0;
  for (var i=0; i < data_in.length; i++) {
    var array_max = Math.max.apply(Math, data_in[i]); // really?
    max_value = Math.max(max_value, array_max)
  }

  var h = 180;
  var w = 170;
  var bar_width = 17;
  var bar_spacing = 4;
  var total_bar_width = bar_width + bar_spacing;
  var group_spacing = 150;
  var num_groups = data_in.length;

  var vis = new pv.Panel()
      .width(w)
      .height(h)
      .margin(20)
      .left(30)
      .cursor("pointer");
  var y = pv.Scale.linear(0, max_value).range(0, h);

  vis.add(pv.Rule)
    .data(y.ticks())
    .strokeStyle("#eee")
    .bottom(y)
    .anchor("left")
    .add(pv.Label)
      .text(y.tickFormat);

  var chartPanel = vis
    .add(pv.Panel)
      .left(20)
    .add(pv.Panel)
    .data(data_in)
    .left(function() { return this.index * (total_bar_width); })

  function drawChart(data, labels) {
    chartPanel.data(data);
    var idx = -1;
    chartPanel.add(pv.Bar)
      .data(function(i) { return i; })
      .width((bar_width * data_in.length)/data.length)
      .height(y)
      .bottom(0)
      .left(function() { return this.index * group_spacing ; })
    .anchor("top").add(pv.Label)
      .textAlign(function(d) { return (d < (4.0/5.0)*max_value) ? "left" : "right"; } )
      .textBaseline("middle")
      .textAngle(-Math.PI / 2)
      .textDecoration(function(d) { return (d == 0) ? ["line-through"] : []; })
      .text(function(d) { idx++; return (d == 0) ? "" : labels[idx]; });
    vis.render();
  }

  var clicked = false;
  function onClick () {
    if (clicked) {
      clicked = false;
      drawChart(data_in, firstSub(labels));
    } else {
      clicked = true;
      max = Math.min(data_in.length, alt_data.length)
      drawChart(interleave(alt_data, data_in, max), interleaveLabels(alt_labels, labels, max));
    }
  }

  // Disabled Alt Graph
  //vis.event("click", onClick)

  drawChart(data_in, firstSub(labels));
}

function interleave(data1, data2, max) {
  var ret = []
  for (var i=0; i < max; i++) {
    ret.push(data1[i]);
    ret.push(data2[i]);
  }
  return ret;
}

function firstSub(arr) {
  var ret = []
  for (var i=0; i < arr.length; i++) {
    ret.push(arr[i][0]);
  }
  return ret;
}

function interleaveLabels(labels1, labels2, max) {
  var ret = []
  for (var i=0; i < max; i++) {
    ret.push(labels1[i][0] + " - " + labels1[i][1]);
    ret.push(labels2[i][1]);
  }
  return ret;
}

