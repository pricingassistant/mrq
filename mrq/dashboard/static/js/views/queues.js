define(["jquery", "underscore", "views/generic/datatablepage", "models"],function($, _, DataTablePage, Models) {

  return DataTablePage.extend({

    el: '.js-page-queues',

    template:"#tpl-page-queues",

    events:{
    },

    renderDatatable:function() {

      var self = this;

      var datatableConfig = self.getCommonDatatableConfig("queues");

      _.extend(datatableConfig, {
        "aoColumns": [

          {
            "sTitle": "Name",
            "sClass": "col-name",
            "sType": "string",
            "mData":function(source, type, val) {
              return "<a href='/#jobs?queue="+source.name+"&status=queued'>"+source.name+"</a>";
            }
          },
          {
            "sTitle": "MongoDB Jobs",
            "sClass": "col-mongodb-jobs",
            "sType":"numeric",
            "mData":function(source, type, val) {
              var cnt = source.jobs || 0;

              return "<a href='/#jobs?queue="+source.name+"&status=queued'>"+cnt+"</a>"
            }
          },
          {
            "sTitle": "Redis Jobs",
            "sClass": "col-jobs",
            "sType":"numeric",
            "mData":function(source, type, val) {
              var cnt = source.size || 0;

              if (type == "display") {
                if (source.jobs_to_dequeue != undefined) {
                  cnt = source.jobs_to_dequeue + " (" + cnt + "&nbsp;total)";
                }
                return "<a href='/#jobs?queue="+source.name+"&status=queued'>"+cnt+"</a>"
                 + "<br/>"
                 + '<span class="inlinesparkline"></span>';
              } else {
                return cnt;
              }
            }
          },
          {
            "sTitle": "Speed",
            "sClass": "col-speed",
            "sType":"numeric",
            "mData":function(source, type, val) {
              return (Math.round(self.getCounterSpeed("queue."+source.name) * 100) / 100) + " jobs/second";
            }
          },
          {
            "sTitle": "ETA",
            "sClass": "col-eta",
            "sType":"string",
            "mData":function(source, type, val) {

              var cnt = source.size || 0;

              if (type == "display") {
                var html = self.getCounterEta("queue."+source.name, source.size || 0);

                if (source.graph) {
                  html += "<br/>"
                       + '<span class="inlinesparkline"></span>';
                }
                return html;
              } else {
                return self.getCounterSpeed("queue."+source.name);
              }
            }
          }

        ],
        "fnDrawCallback": function (oSettings) {

          _.each(oSettings.aoData,function(row) {
            var oData = row._aData;

            var num_jobs = oData.size || 0;

            if (oData.jobs_to_dequeue != undefined) {
              num_jobs = oData.jobs_to_dequeue || 0;
            }

            $(".col-jobs .inlinesparkline", row.nTr).sparkline(self.addToCounter("queue."+oData.name, num_jobs, 50), {"width": "100px", "height": "30px", "defaultPixelsPerValue": 1});

            // ETA graph
            if (!oData.graph) return;

            var colorMap = {};
            var graph_config = oData.graph_config || {};
            if (graph_config.include_inf) {
              colorMap = ["red"];
              for (var i=0;i<graph_config.slices;i++) {
                colorMap.push("blue");
              }
              colorMap.push("green");
            }

            $(".col-eta .inlinesparkline", row.nTr).sparkline(oData.graph, {
              "width": "100px",
              "height": "30px",
              "disableHiddenCheck": true,
              "tooltipOffsetX": -100,
              "tooltipOffsetY": -80,
              "colorMap": colorMap,
              "type": "bar",
              "tooltipFormatter": function(sparkline, options, fields) {
                var val = fields[0]["value"];
                var offset = fields[0]["offset"];

                var interval = (graph_config.stop - graph_config.start) / graph_config.slices;
                var label_start, label_stop;

                if (graph_config.include_inf) {
                  offset -= 1;
                }

                var start = (graph_config.start + offset * interval);
                var stop = (graph_config.start + (offset + 1) * interval);

                if (oData.is_timed) {
                  label_start = moment(start*1000).calendar();
                  label_stop = moment(stop*1000).calendar();
                } else {
                  label_start = (Math.round(start * 100)/100);
                  label_stop = (Math.round(stop * 100)/100);
                }
                if (offset < 0) {
                  label_start = "";
                }
                if (offset == graph_config.slices) {
                  label_stop = "";
                }
                return val + " <br/><br/>"+label_start+"<br/>=><br/>"+label_stop+"";
              }
            });
          });
        },

        "aaSorting":[ [0,'asc'] ],
      });

      this.initDataTable(datatableConfig);

    }
  });

});
