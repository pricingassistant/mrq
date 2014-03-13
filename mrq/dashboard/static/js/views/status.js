define(["jquery", "underscore", "views/generic/datatablepage", "models", "moment"],function($, _, DataTablePage, Models, moment) {

  return DataTablePage.extend({

    el: '.js-page-status',

    template:"#tpl-page-status",

    events:{
    },

    renderDatatable:function() {

      var self = this;

      var datatableConfig = self.getCommonDatatableConfig("status");

      _.extend(datatableConfig, {
        "aoColumns": [

          {
            "sTitle": "Status",
            "sClass": "col-status",
            "sType":"string",
            "sWidth":"150px",
            "mData":function(source, type/*, val*/) {
              return "<a href='/#jobs?status="+source._id+"'>"+source._id+"</a>";
            }
          },
          {
            "sTitle": "Jobs",
            "sClass": "col-jobs",
            "sType":"numeric",
            "sWidth":"120px",
            "mData":function(source, type/*, val*/) {
              var cnt = (source.jobs || 0);
              if (type == "display") {
                return "<a href='/#jobs?status="+source._id+"'>"+cnt+"</a>"
                 + "<br/>"
                 + '<span class="inlinesparkline" values="'+self.addToCounter("index.status."+source._id, cnt, 50).join(",")+'"></span>';
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
              console.log()
              return (Math.round(self.getCounterSpeed("index.status."+source._id) * 100) / 100) + " jobs/second";
            }
          },
          {
            "sTitle": "ETA",
            "sClass": "col-eta",
            "sType":"numeric",
            "mData":function(source, type, val) {
              return self.getCounterEta("index.status."+source._id, source.jobs || 0);
            }
          }

        ],
        "aaSorting":[ [0,'asc'] ],
      });

      this.initDataTable(datatableConfig);

    }
  });

});
