define(["jquery", "underscore", "views/generic/datatablepage", "models"],function($, _, DataTablePage, Models) {

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
            "mDataProp": "name",
            "fnRender": function ( o /*, val */) {
              return "<a href='/#jobs?worker="+o.aData.name+"'>"+o.aData.name+"</a>";
            }
          },
          {
            "sTitle": "Jobs",
            "sClass": "col-jobs",
            "sType":"numeric",
            "mData":function(source /*, type, val*/) {
              return source.count || 0;
            }
          }

        ],
        "aaSorting":[ [0,'asc'] ],
      });

      this.initDataTable(datatableConfig);

    }
  });

});
