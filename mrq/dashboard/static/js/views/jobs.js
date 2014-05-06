define(["jquery", "underscore", "views/generic/datatablepage", "models"],function($, _, DataTablePage, Models) {

  return DataTablePage.extend({

    el: '.js-page-jobs',

    template:"#tpl-page-jobs",

    events:{
      "click .js-datatable-filters-submit": "filterschanged",
      "click .js-datatable .js-actions button": "row_jobaction",
      "click button.js-jobs-groupaction": "groupaction"
    },

    initFilters: function() {
      this.filters = {
        "worker": this.options.params.worker||"",
        "queue": this.options.params.queue||"",
        "path": this.options.params.path||"",
        "status": this.options.params.status||"",
        "exceptiontype": this.options.params.exceptiontype||"",
        "params": this.options.params.params||"",
        "id": this.options.params.id||"",
      };
    },

    setOptions:function(options) {
      this.options = options;
      this.initFilters();
      this.flush();
    },

    refresh_logs:function(job_id) {

      var self = this;

      $.ajax("/api/logs?job="+job_id+"&last_log_id="+self.last_log_id, {
        "type": "GET",
        "success": function(data) {
          if (!self.last_log_id) {
            self.$(".js-jobs-modal .js-jobs-modal-content").html("");
          }
          self.$(".js-jobs-modal .js-jobs-modal-content")[0].innerHTML += data.logs;
          self.last_log_id = data.last_log_id;
        },
        "error": function(xhr, status, error) {
          alert("Error: "+error);
        }
      });

    },

    refreshStackTrace: function(jobId)  {
      $.ajax("/api/job/"+jobId+"/traceback", {
        "type": "GET",
        "success": function(data) {
          self.$(".js-jobs-modal .js-jobs-modal-content").html(data["traceback"].replace(/\\n/g, "<br/>"));
          self.$(".js-jobs-modal h4").html("Stack Trace");
          self.$(".js-jobs-modal").modal({});
        },
        "error": function(xhr, status, error) {
          alert("Error: "+error);
        }
      });
    },


    groupaction: function(evt) {
      evt.preventDefault();
      evt.stopPropagation();

      var self = this;

      var action = $(evt.target).data("action");
      var data = _.clone(this.filters);

      data["action"] = action;
      self.jobaction(evt, data);

    },

    row_jobaction:function(evt) {
      evt.preventDefault();
      evt.stopPropagation();

      var self = this;

      var job_id = $(evt.currentTarget).closest(".js-actions").data("jobid");
      var action = $(evt.currentTarget).data("action");

      self.$(".js-jobs-modal").unbind();

      if (action == "viewresult") {

        $.ajax("/api/job/"+job_id+"/result", {
          "type": "GET",
          "success": function(data) {
            self.$(".js-jobs-modal .js-jobs-modal-content").html(JSON.stringify(data, null, 2));
            self.$(".js-jobs-modal h4").html("Job result");
            self.$(".js-jobs-modal").modal({});
          },
          "error": function(xhr, status, error) {
            alert("Error: "+error);
          }
        });

      } else if (action == "viewlogs") {

        self.last_log_id = "";

        self.$(".js-jobs-modal .js-jobs-modal-content").html("Loading...");
        self.$(".js-jobs-modal h4").html("Job logs");
        self.$(".js-jobs-modal").modal({});

        // These poll events are sent by the generic datatable refresh() method
        self.$(".js-jobs-modal").on("poll", function() {
          self.refresh_logs(job_id);
        });
        self.refresh_logs(job_id);

      } else if (action == "copycommand") {
        window.prompt("There is you command", $(evt.currentTarget).data("command"))
      } else if (action == "viewstacktrace") {

        self.$(".js-jobs-modal .js-jobs-modal-content").html("Loading...");
        self.$(".js-jobs-modal h4").html("Stack Trace");
        self.$(".js-jobs-modal").modal({});

        self.$(".js-jobs-modal").on("poll", function() {
          self.refreshStackTrace(job_id);
        });
        self.refreshStackTrace(job_id);
      } else {

        self.jobaction(evt, {
          "id": job_id,
          "action": action
        });

      }

    },

    jobaction:function(evt, data) {

      $(evt.target).find(".glyphicon").addClass("spin");

      $.ajax("/api/jobaction", {
        "type": "POST",
        "data": data,
        "success": function(data) {
        },
        "error": function(xhr, status, error) {
          alert("Error: "+error);
        },
        "complete": function() {
          setTimeout(function() {
            $(evt.target).find(".glyphicon").removeClass("spin");
          }, 500);
        }
      });
    },

    renderDatatable:function() {

      var self = this;

      var datatableConfig = self.getCommonDatatableConfig("jobs");

      _.extend(datatableConfig, {
        "aoColumns": [

          {
            "sTitle": "Path &amp; ID",
            "sClass": "col-jobs-path",
            "sWidth":"35%",
            "mDataProp": "path",
            "mData": function ( source /*, val */) {
              return "<a href='/#jobs?path="+source.path+"'>"+source.path+"</a>"+
                "<br/><br/><a href='/#jobs?id="+source._id+"'><small>"+source._id+"</small></a>";
            }
          },
          {
            "sTitle": "Params",
            "sWidth":"65%",
            "sClass": "col-jobs-params",
            "mDataProp": "params",
            "mData": function ( source /*, val */) {
              return "<pre class='js-oxpre'>"+JSON.stringify(source.params, null, 2)+"</pre>";
            }
          },
          {
            "sTitle": "Status",
            "sType":"string",
            "sWidth":"100px",
            "sClass": "col-jobs-status",
            "mData":function(source, type/*, val*/) {
              if (type == "display") {

                var status_classes = {
                  'started': "label-success",
                  'success': "label-success",
                  'timeout': "label-danger",
                  'failed': "label-danger",
                  'interrupt': "label-danger",
                  'cancel': "label-warning",
                  'retry': "label-warning"
                };
                var css_class = status_classes[source.status] || "label-info";

                html = "<div class='js-actions' data-jobid="+source._id+"><a href='/#jobs?status=" + (source.status || "queued")+ "'>" + "<span class='label " + css_class + "'>" + (source.status || "queued") + "</span></a>";
                html += "<br/><br/>";

                if (source.progress) {
                  var progress = (Math.round(source.progress*10000)/100);
                  html += '<div class="progress"><div class="progress-bar progress-bar-success" role="progressbar" aria-valuenow="'+progress+'" aria-valuemin="0" aria-valuemax="100" style="width: '+progress+'%;">'+progress+'%</div></div>';
                }

                html += "<button class='btn btn-xs btn-default' data-action='viewstacktrace'><span class='glyphicon glyphicon-align-left'></span> Trace</button>";
                html += "</div>";
                return (html);
              } else {
                return source.status || "queued";
              }
            }
          },
          {
            "sTitle": "Time",
            "sType":"string",
            "sWidth":"100px",
            "sClass": "col-jobs-time",
            "mData":function(source, type/*, val*/) {

              if (type == "display") {
                var display = [
                  "queued "+moment.utc(1000 * parseInt(source._id.substring(0, 8), 16)).fromNow()
                  //"updated "+moment.utc(source.dateupdated).fromNow()
                ];

                if (source.datestarted) {
                  display.push("started "+moment.utc(source.datestarted).fromNow());
                }
                if (source.totaltime) {
                  display.push("totaltime "+String(source.totaltime).substring(0,6)+"s");
                }
                if (source.time) {
                  display.push("cputime "+String(source.time).substring(0,6)+"s ("+source.switches+" switches)");
                }

                return "<small>" + display.join("<br/>") + "</small>";

              } else {
                return source.datestarted || "";
              }
            }
          },
          {
            "sTitle": "Queue",
            "sType":"string",
            "sWidth":"100px",
            "sClass": "col-jobs-queue",
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
            "sWidth":"140px",
            "sClass": "col-jobs-worker",
            "mData":function(source, type/*, val*/) {
              if (type == "display") {
                return source.worker?("<a href='/#jobs?worker="+source.worker+"'>"+source.worker+"</a>"):"";
              } else {
                return source.worker || "";
              }
            }
          },
          {
            "sTitle": "Actions",
            "sType":"string",
            "sWidth":"200px",
            "sClass": "col-jobs-action",
            "mData":function(source, type) {
              if (type == "display") {
                return "<div class='js-actions' data-jobid='"+source._id+"'>"+
                  "<button class='btn btn-xs btn-default' data-action='viewlogs'><span class='glyphicon glyphicon-align-left'></span> Logs</button>"+
                  "<button class='pull-right btn btn-xs btn-default' data-action='viewresult'><span class='glyphicon glyphicon-file'></span> Result</button>"+
                  "<br/><br/>"+
                  "<button class='pull-right btn btn-xs btn-default' data-action='copycommand' data-command='"+
                  "mrq-run " + source.path + " &#39" + JSON.stringify(source.params) + "&#39" +
                  "'><span class='glyphicon glyphicon-floppy-save'></span> Command</button>"+
                  "<br/><br/>"+
                  "<button class='btn btn-xs btn-danger pull-right' data-action='cancel'><span class='glyphicon glyphicon-remove-circle'></span> Cancel</button>"+
                  "<button class='btn btn-xs btn-warning' data-action='requeue'><span class='glyphicon glyphicon-refresh'></span> Requeue</button>"+
                "</div>";
              }
              return "";
            }
          }


        ],
        "aaSorting":[ [0,'asc'] ]
      });

      this.initDataTable(datatableConfig);

      if (!_.any(this.filters, function(v, k) {
        return v;
      })) {
        this.$(".js-jobs-groupactions").hide();
      }

    },

    filterschanged:function(evt) {

      var self = this;

      if (evt) {
        evt.preventDefault();
        evt.stopPropagation();
      }

      _.each(self.filters, function(v, k) {
        self.filters[k] = self.$(".js-datatable-filters-"+k).val();
      });

      window.location = "/#jobs?"+$.param(self.filters, true).replace(/\+/g, "%20");
    },

  });

});
