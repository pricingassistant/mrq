define(["jquery", "underscore", "views/generic/datatablepage", "models", "moment"],function($, _, DataTablePage, Models, moment) {

  return DataTablePage.extend({

    el: '.js-page-workers',

    template:"#tpl-page-workers",

    events:{
      "change .js-datatable-filters-showstopped": "filterschanged",
      "click .js-workers-io": "showworkerio",
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

    showworkerio: function(evt) {
      var self = this;

      var worker_id = $(evt.currentTarget).data("workerid");

      var worker_data = _.find(this.dataTableRawData.aaData, function(worker) {
        return worker._id == worker_id;
      });

      var html_modal = _.template($("#tpl-modal-workers-io").html())({"worker": worker_data});

      self.$(".js-workers-modal .js-workers-modal-content").html(html_modal);
      self.$(".js-workers-modal h4").html("I/O for this worker, by task &amp; by type");
      self.$(".js-workers-modal").modal({});

      return false;
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
            "sWidth":"150px",
            "mData":function(source, type/*, val*/) {
              return "<a href='/#jobs?worker="+source._id+"'>"+source.name+"</a><br/><small>"+source.config.local_ip + " " + source._id+"</small>";
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
            "sTitle": "CPU usr/sys",
            "sClass": "col-cpu",
            "sType":"string",
            "sWidth":"120px",
            "mData":function(source, type/*, val*/) {

              var usage = (source.process.cpu.user + source.process.cpu.system) * 1000 / (moment.utc(source.datereported || null).valueOf() - moment.utc(source.datestarted).valueOf());

              var html = Math.round(source.process.cpu.user) + "s / " + Math.round(source.process.cpu.system) + "s"
                + "<br/>"
                + (Math.round(usage * 100)) + "% use";

              if (((source.io || {}).types || []).length) {
                html += "<br/>I/O: <a data-workerid='"+source._id+"' href='#' class='js-workers-io'>"+Math.round(source.io.total)+"s </a>";
              }

              return html;
            }
          },
          {
            "sTitle": "Memory",
            "sClass": "col-mem",
            "sType":"numeric",
            "sWidth":"130px",
            "mData":function(source, type/*, val*/) {
              if (type == "display") {

                return Math.round((source.process.mem.total / (1024*1024)) *10)/10 + "M"
                 + "<br/>"
                 + '<span class="inlinesparkline" values="'+self.addToCounter("worker.mem."+source._id, source.process.mem.total / (1024*1024), 50).join(",")+'"></span>';
              } else {
                return source.process.mem.total
              }
            }
          },
          {
            "sTitle": "Done Jobs",
            "sClass": "col-done-jobs",
            "sType":"numeric",
            "sWidth":"120px",
            "mData":function(source, type/*, val*/) {
              var cnt = (source.done_jobs || 0);
              if (type == "display") {
                return "<a href='/#jobs?worker="+source._id+"'>"+cnt+"</a>"
                 + "<br/>"
                 + '<span class="inlinesparkline" values="'+self.addToCounter("worker.donejobs."+source._id, cnt, 50).join(",")+'"></span>';
              } else {
                return cnt;
              }
            }
          },
          {
            "sTitle": "Speed",
            "sClass": "col-eta",
            "sType":"numeric",
            "sWidth":"120px",
            "mData":function(source, type, val) {
              return (Math.round(self.getCounterSpeed("worker.donejobs."+source._id) * 100) / 100) + " j/s";
            }
          },
          {
            "sTitle": "Current Jobs",
            "sClass": "col-current-jobs",
            "sType":"numeric",
            "sWidth":"120px",
            "mData":function(source, type/*, val*/) {
              var cnt = (source.jobs || []).length;
              if (type == "display") {
                return "<a href='/#jobs?worker="+source._id+"&status=started'>"+cnt+"</a> / "+source.config.greenlets
                 + "<br/>"
                 + '<span class="inlinesparkline" values="'+self.addToCounter("worker.currentjobs."+source._id, cnt, 50).join(",")+'"></span>';
              } else {
                return cnt;
              }
            }
          }

        ],
        "fnDrawCallback": function (oSettings) {
          $(".inlinesparkline", oSettings.nTable).sparkline("html", {"width": "100px", "height": "30px", "defaultPixelsPerValue": 1});
        },
        "aaSorting":[ [0,'asc'] ],
      });

      this.initDataTable(datatableConfig);

    }
  });

});
