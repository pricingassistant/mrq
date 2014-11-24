define(["jquery", "underscore", "views/generic/datatablepage", "models"],function($, _, DataTablePage, Models) {

  return DataTablePage.extend({

    el: '.js-page-scheduledjobs',

    template:"#tpl-page-scheduledjobs",

    events:{
    },

    renderDatatable:function() {

      var self = this;

      var datatableConfig = self.getCommonDatatableConfig("scheduled_jobs");

      _.extend(datatableConfig, {
        "aoColumns": [

          {
            "sTitle": "Name",
            "sClass": "col-jobs-path",
            "mDataProp": "path",
            "fnRender": function ( o /*, val */) {
              return "<a href='/#jobs?path="+o.aData.path+"'>"+o.aData.path+"</a>"+
                "<br/><br/><small>"+o.aData._id+"</small>";
            }
          },

          {
            "sTitle": "Interval",
            "sType":"numeric",
            "sClass": "col-jobs-interval",
            "mData":function(source, type) {
              if (type == "display") {
                return moment.duration(source.interval*1000).humanize();
              }
              return source.interval;
            }
          },
          // {
          //   "sTitle": "Daily time",
          //   "sType":"string",
          //   "sClass": "col-jobs-dailytime",
          //   "mData":function(source, type) {
          //     if (type == "display") {
          //       return moment.duration(source.interval*1000).humanize();
          //     }
          //     return source.interval;
          //   }
          // },
          {
            "sTitle": "Last Queued",
            "sType":"string",
            "sClass": "col-jobs-lastqueued",
            "mData":function(source, type) {
              return moment.utc(source.datelastqueued).fromNow();
            }
          },
          {
            "sTitle": "Params",
            "sClass": "col-jobs-params",
            "mDataProp": "params",
            "fnRender": function ( o /*, val */) {
              return "<pre class='js-oxpre'>"+_.escape(JSON.stringify(o.aData.params, null, 2))+"</pre>";
            }
          },

        ],
        "aaSorting":[ [0,'asc'] ],
      });

      this.initDataTable(datatableConfig);

    }
  });

});
