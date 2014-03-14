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
                return "<a href='/#jobs?queue="+source.name+"&status=queued'>"+cnt+"</a>"
                 + "<br/>"
                 + '<span class="inlinesparkline" values="'+self.addToCounter("queue."+source.name, cnt, 50).join(",")+'"></span>';
              } else {
                return cnt;
              }
            },
            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
              setTimeout(function() {
                $(".inlinesparkline", nTd).sparkline("html", {"width": "100px", "height": "30px", "defaultPixelsPerValue": 1});
              }, 10);
            }
          },
          {
            "sTitle": "Speed",
            "sClass": "col-eta",
            "sType":"numeric",
            "mData":function(source, type, val) {
              return (Math.round(self.getCounterSpeed("queue."+source.name) * 100) / 100) + " jobs/second";
            }
          },
          {
            "sTitle": "ETA",
            "sClass": "col-eta",
            "sType":"numeric",
            "mData":function(source, type, val) {
              return self.getCounterEta("queue."+source.name, source.count || 0);
            }
          }

        ],
        "aaSorting":[ [0,'asc'] ],
      });

      this.initDataTable(datatableConfig);

    }
  });

});
