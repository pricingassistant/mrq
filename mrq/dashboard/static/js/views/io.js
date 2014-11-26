define(["jquery", "underscore", "views/generic/datatablepage", "models"],function($, _, DataTablePage, Models) {

  return DataTablePage.extend({

    el: '.js-page-io',

    template:"#tpl-page-io",

    events:{
    },

    renderDatatable:function() {

      var self = this;

      var unit_name = "ops";

      var datatableConfig = {
        "aoColumns": [

          {
            "sTitle": "Type",
            "sClass": "col-type",
            "sType": "string",
            "sWidth":"150px",
            "mData":function(source, type, val) {
              // console.log(source)
              return source.io.type;
            }
          },
          {
            "sTitle": "Data",
            "sClass": "col-data",
            "sType":"string",
            "mData":function(source, type, val) {
              return "<pre class='js-oxpre'>"+_.escape(JSON.stringify(source.io.data, null, 0))+"</pre>";
            }
          },
          {
            "sTitle": "Time",
            "sType":"string",
            "sWidth":"160px",
            "sClass": "col-jobs-time",
            "mData":function(source, type/*, val*/) {

              if (type == "display") {
                return "i/o started "+moment.utc(source.io.started*1000).fromNow();
              } else {
                return source.io.started || "";
              }
            }
          },
          {
            "sTitle": "Path &amp; ID",
            "sClass": "col-jobs-path",
            "sWidth":"300px",
            "mDataProp": "path",
            "mData": function ( source /*, val */) {
              return "<a href='/#jobs?path="+source.path+"'>"+source.path+"</a>"+
                "<br/><br/><a href='/#jobs?id="+source.id+"'><small>"+source.id+"</small></a>";
            }
          },
          {
            "sTitle": "Worker",
            "sType":"string",
            "sWidth":"200px",
            "sClass": "col-jobs-worker",
            "mData":function(source, type/*, val*/) {
              if (type == "display") {
                return source.worker?("<small><a href='/#jobs?worker="+source.worker+"'>"+source.worker+"</a></small>"):"";
              } else {
                return source.worker || "";
              }
            }
          }
        ],
        "fnDrawCallback": function (oSettings) {

          _.each(oSettings.aoData,function(row) {
            var oData = row._aData;

            $(".col-jobs .inlinesparkline", row.nTr).sparkline("html", {"width": "100px", "height": "30px", "defaultPixelsPerValue": 1});

          });
        },
        "aaSorting":[ [2,'desc'] ],
        "sPaginationType": "full_numbers",
        "iDisplayLength":100,
        "sDom":"iprtipl",
        "oLanguage":{
          "sSearch":"",
          "sInfo": "Showing _START_ to _END_ of _TOTAL_ "+unit_name,
          "sEmptyTable":"No "+unit_name,
          "sInfoEmpty":"Showing 0 "+unit_name,
          "sInfoFiltered": "",
          "sLengthMenu": '<select style="width:60px;font-size:13px;height:26px;">'+
            '<option value="20">20</option>'+
            '<option value="50">50</option>'+
            '<option value="100">100</option>'+
            '</select>'
        },
        "sPaginationType": "bs_full",
        "bProcessing": true,
        "bServerSide": true,
        "bDeferRender": true,
        "bDestroy": true,
        "fnServerData": function (sSource, aoData, fnCallback) {
          self.loading = true;

          var params = {};
          _.each(aoData, function(v) {
            params[v.name] = v.value;
          });

          self.app.getJobsDataFromWorkers(function(err, data) {
            self.loading = false;
            self.trigger("loaded");

            data = _.filter(data, function(row) {
              return (row.io || {}).type;
            });

            if (!err) {
              fnCallback({
                "aaData": data,
                "iTotalDisplayRecords": data.length,
                "sEcho": params.sEcho
              })
            }
          });
        }
      };

      this.initDataTable(datatableConfig);
    }
  });

});
