define(["jquery", "underscore", "views/generic/datatablepage", "models", "moment"],function($, _, DataTablePage, Models, moment) {

  return DataTablePage.extend({

    el: '.js-page-agents',

    template:"#tpl-page-agents",

    events:{
      "change .js-datatable-filters-showstopped": "filterschanged"
    },

    initFilters: function() {

      this.filters = {
        "showstopped": this.options.params.showstopped||""
      };

    },

    setOptions:function(options) {
      this.options = options;
      this.initFilters();
      this.flush();
    },

    renderDatatable:function() {

      var self = this;

      var datatableConfig = self.getCommonDatatableConfig("agents");

      _.extend(datatableConfig, {
        "aoColumns": [

          {
            "sTitle": "ID",
            "sClass": "col-name",
            "sType":"string",
            "sWidth":"150px",
            "mData":function(source, type/*, val*/) {
              return source._id || "";
            }
          }, {
            "sTitle": "Worker Group",
            "sClass": "col-workergroup",
            "sType":"string",
            "sWidth":"150px",
            "mData":function(source, type/*, val*/) {
              return source.worker_group;
            }
          },
          {
            "sTitle": "Status",
            "sClass": "col-status",
            "sType":"string",
            "sWidth":"80px",
            "mData":function(source, type/*, val*/) {
              return source.status;
            }
          },
          {
            "sTitle": "Last report",
            "sClass": "col-last-report",
            "sType":"string",
            "sWidth":"150px",
            "mData":function(source, type/*, val*/) {
              if (type == "display") {

                return "<small>" + (source.datereported?moment.utc(source.datereported).fromNow():"Never")
                   + "<br/>"
                   + "started " + moment.utc(source.datestarted).fromNow() + "</small>";
              } else {
                return source.datereported || "";
              }
            }
          },
          {
            "sTitle": "CPU",
            "sClass": "col-cpu",
            "sType":"numeric",
            "sWidth":"120px",
            "mData":function(source, type/*, val*/) {
              return source.total_cpu + " total<br/>" + (source.free_cpu || "N/A") + " free";
            }
          },
          {
            "sTitle": "Memory",
            "sClass": "col-mem",
            "sType":"numeric",
            "sWidth":"130px",
            "mData":function(source, type/*, val*/) {
                return source.total_memory + " total<br/>" + (source.free_memory || "N/A") + " free";
            }
          }, {
            "sTitle": "Current Workers",
            "sClass": "col-mem",
            "sType":"string",
            "sWidth":"100%",
            "mData":function(source, type/*, val*/) {
                return "<pre>" + source.current_workers.join("<br/>") + "</pre>";
            }
          },
        ],
        "aaSorting":[ [0,'asc'] ],
      });

      this.initDataTable(datatableConfig);

    }
  });

});
