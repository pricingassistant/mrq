define(["jquery", "underscore", "views/generic/datatablepage", "models"],function($, _, DataTablePage, Models) {

  return DataTablePage.extend({

    el: '.js-page-jobs',

    template:"#tpl-page-jobs",

    events:{
      "click .js-datatable-filters-submit": "filterschanged"
    },

    init: function() {

      this.filters = {
        "worker": this.options.params.worker||"",
        "queue": this.options.params.queue||"",
        "path": this.options.params.path||"",
        "status": this.options.params.status||"",
      };

    },

    setOptions:function(options) {
      this.options = options;
      this.init();
      this.flush();
    },

    renderDatatable:function() {

      var self = this;

      var datatableConfig = self.getCommonDatatableConfig("jobs");

      _.extend(datatableConfig, {
        "aoColumns": [

          {
            "sTitle": "Name",
            "sClass": "col-path",
            "mDataProp": "path",
            "fnRender": function ( o /*, val */) {
              return "<a href='/#jobs?path="+o.aData.path+"'>"+o.aData.path+"</a>";
            }
          },
          {
            "sTitle": "Status",
            "sType":"string",
            "sClass": "col-status",
            "mData":function(source, type/*, val*/) {
              if (type == "display") {
                return ("<a href='/#jobs?status="+(source.status || "queued")+"'>"+(source.status || "queued")+"</a>");
              } else {
                return source.status || "queued";
              }
            }
          },
          {
            "sTitle": "Queue",
            "sType":"string",
            "sClass": "col-queue",
            "mData":function(source, type/*, val*/) {
              if (type == "display") {
                return source.queue?("<a href='/#jobs?queue="+source.queue+"'>"+source.queue+"</a>"):"";
              } else {
                return source.queue || "";
              }

            }
          },
          {
            "sTitle": "Worker",
            "sType":"string",
            "sClass": "col-worker",
            "mData":function(source, type/*, val*/) {
              if (type == "display") {
                return source.worker?("<a href='/#jobs?worker="+source.worker+"'>"+source.worker+"</a>"):"";
              } else {
                return source.worker || "";
              }
            }
          },
          {
            "sTitle": "Params &amp; logs",
            "sClass": "col-params",
            "mDataProp": "params",
            "fnRender": function ( o /*, val */) {
              return "<pre>"+JSON.stringify(o.aData.params, null, 2)+"</pre>";
            }
          }

        ],
        "aaSorting":[ [0,'asc'] ],
      });

      this.initDataTable(datatableConfig);

    }
  });

});
