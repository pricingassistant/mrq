define(["jquery", "underscore", "views/generic/datatablepage", "models", "moment"],function($, _, DataTablePage, Models, moment) {

  return DataTablePage.extend({

    el: '.js-page-workers',

    template:"#tpl-page-workers",

    events:{
    },

    renderDatatable:function() {

      var self = this;

      var datatableConfig = self.getCommonDatatableConfig("workers");

      _.extend(datatableConfig, {
        "aoColumns": [

          {
            "sTitle": "Name",
            "sClass": "col-name",
            "sType":"string",
            "mData":function(source, type/*, val*/) {
              return "<a href='/#jobs?worker="+source.name+"'>"+source.name+"</a>";
            }
          },
          {
            "sTitle": "Queues",
            "sClass": "col-queues",
            "sType":"string",
            "mData":function(source, type/*, val*/) {
              return _.map(source.config.queues||[], function(q) {
                return "<a href='/#jobs?queue="+q+"'>"+q+"</a>";
              }).join(" ");
            }
          },
          {
            "sTitle": "Status",
            "sClass": "col-status",
            "sType":"string",
            "mData":function(source, type/*, val*/) {
              return source.status;
            }
          },
          {
            "sTitle": "Last report",
            "sClass": "col-last-report",
            "sType":"string",
            "mData":function(source, type/*, val*/) {
              if (!source.datereported) return "Never";
              return moment.utc(source.datereported).fromNow();
            }
          },
          {
            "sTitle": "CPU sys/usr",
            "sClass": "col-cpu",
            "sType":"string",
            "mData":function(source, type/*, val*/) {
              return source.process.cpu.system+" / "+source.process.cpu.user;
            }
          },
          {
            "sTitle": "RSS",
            "sClass": "col-mem",
            "sType":"numeric",
            "mData":function(source, type/*, val*/) {
              return Math.round((source.process.mem.rss / (1024*1024)) *10)/10 + "M";
            }
          },
          {
            "sTitle": "Done Jobs",
            "sClass": "col-done-jobs",
            "sType":"numeric",
            "mData":function(source, type/*, val*/) {
              var cnt = (source.done_jobs || 0);
              if (type == "display") {
                return "<a href='/#jobs?worker="+source.name+"'>"+cnt+"</a>";
              } else {
                return cnt;
              }
            }
          },
          {
            "sTitle": "Current Jobs",
            "sClass": "col-current-jobs",
            "sType":"numeric",
            "mData":function(source, type/*, val*/) {
              var cnt = (source.jobs || []).length;
              if (type == "display") {
                return "<a href='/#jobs?worker="+source.name+"&status=started'>"+cnt+"</a>";
              } else {
                return cnt;
              }
            }
          }

        ],
        "aaSorting":[ [0,'asc'] ],
      });

      this.initDataTable(datatableConfig);

    }
  });

});
