define(["jquery", "underscore", "views/generic/datatablepage", "models"],function($, _, DataTablePage, Models) {

  return DataTablePage.extend({

    el: '.js-page-taskexceptions',

    template:"#tpl-page-taskexceptions",

    events:{
    },

    renderDatatable:function() {

      var self = this;

      var datatableConfig = self.getCommonDatatableConfig("taskexceptions");

      _.extend(datatableConfig, {
        "aoColumns": [

          {
            "sTitle": "Name",
            "sClass": "col-name",
            "sType": "string",
            "mData":function(source, type, val) {
              // console.log(source)
              return "<a href='/#jobs?path="+source._id.path+"'>"+source._id.path+"</a>";
            }
          },
          {
            "sTitle": "Exception",
            "sClass": "col-exception",
            "sType":"numeric",
            "mData":function(source, type, val) {
              return "<a href='/#jobs?path="+source._id.path+"&status=failed&exceptiontype="+source._id.exceptiontype+ "'>"+source._id.exceptiontype+"</a>"
            }
          },
          {
            "sTitle": "Jobs",
            "sClass": "col-jobs",
            "sType":"numeric",
            "mData":function(source, type, val) {
              var cnt = source.jobs || 0;

              if (type == "display") {
                return "<a href='/#jobs?path="+source._id.path+"&status=failed&exceptiontype="+source._id.exceptiontype+"'>"+cnt+"</a>"
                 + "<br/>"
                 + '<span class="inlinesparkline" values="'+self.addToCounter("taskexceptions."+source._id.path+" "+source._id.exceptiontype, cnt, 50).join(",")+'"></span>';
              } else {
                return cnt;
              }
            }
          },
          {
            "sTitle": "Speed",
            "sClass": "col-eta",
            "sType":"numeric",
            "mData":function(source, type, val) {
              return (Math.round(self.getCounterSpeed("taskexceptions."+source._id.path+" "+source._id.exceptiontype) * 100) / 100) + " jobs/second";
            }
          },
          {
            "sTitle": "ETA",
            "sClass": "col-eta",
            "sType":"numeric",
            "mData":function(source, type, val) {
              return self.getCounterEta("taskexceptions."+source._id.path+" "+source._id.exceptiontype, source.jobs || 0);
            }
          }

        ],
        "fnDrawCallback": function (oSettings) {

          _.each(oSettings.aoData,function(row) {
            var oData = row._aData;

            $(".col-jobs .inlinesparkline", row.nTr).sparkline("html", {"width": "100px", "height": "30px", "defaultPixelsPerValue": 1});

          });
        },
        "aaSorting":[ [0,'asc'] ],
      });

      this.initDataTable(datatableConfig);

    }
  });

});
