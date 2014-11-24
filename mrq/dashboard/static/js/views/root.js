/**
 * @fileoverview Defines the view container view that contains all views
 */
define(["views/generic/page", "jquery",
        "views/queues", "views/workers", "views/jobs", "views/scheduledjobs", "views/index", "views/taskpaths", "views/status", "views/taskexceptions", "views/io"],

      function(
        Page, $,
        QueuesView, WorkersView, JobsView, ScheduledJobsView, IndexView, TaskPathsView, StatusView, TaskExceptionsView, IOView
  ) {

  return Page.extend({

    template:"#tpl-page-root",
    events:{
      "change .js-store-select":"changestore"
    },

    isTabVisible: true,

    init: function() {

      var self = this;

      // We want to reload once when the autorefresh rate changes
      $(".js-autorefresh").on("change", function() {
        self.trigger("visibilitychange");
      });


      // http://stackoverflow.com/questions/1060008/is-there-a-way-to-detect-if-a-browser-window-is-not-currently-active
      (function() {

        var onchange = function(evt) {

            var prevVisible = self.isTabVisible;
            var v = true, h = false,
                evtMap = {
                    focus:v, focusin:v, pageshow:v, blur:h, focusout:h, pagehide:h
                };

            evt = evt || window.event;
            if (evt.type in evtMap)
                self.isTabVisible = evtMap[evt.type];
            else
                self.isTabVisible = this[hidden] ? false : true;

            if (prevVisible != self.isTabVisible) {
              self.trigger("visibilitychange");
            }
        };

        var hidden = "hidden";

        // Standards:
        if (hidden in document)
            document.addEventListener("visibilitychange", onchange);
        else if ((hidden = "mozHidden") in document)
            document.addEventListener("mozvisibilitychange", onchange);
        else if ((hidden = "webkitHidden") in document)
            document.addEventListener("webkitvisibilitychange", onchange);
        else if ((hidden = "msHidden") in document)
            document.addEventListener("msvisibilitychange", onchange);
        // IE 9 and lower:
        else if ('onfocusin' in document)
            document.onfocusin = document.onfocusout = onchange;
        // All others:
        else
            window.onpageshow = window.onpagehide = window.onfocus = window.onblur = onchange;

      })();
    },

    /**
     * Shows a non blocking message to the user during a certain amount of time.
     *
     * @param {String} type : Type of alert ("warning", "error", "success", "info")
     * @param {String} message : Message to display
     * @param {Number} [timeout=5000] : Milliseconds before the alert is closed, if negative, will never close
     */
    alert:function(type, message, timeout) {

      if (type=="clear") {
        $(".wizard-content .alert").alert('close');
        return;
      }

      var html = '<div class="fade in alert alert-'+type+'">'+
        '<a class="close" data-dismiss="alert">×</a>'+
        '<p>'+message+'</p>'+
      '</div>';

      var $alert = $(".app-content").prepend(html).children().first();
      $alert.alert();

      if (timeout===undefined) {
        timeout = 5000;
      }
      if (timeout>0) {
        setTimeout(function() {
          $alert.alert('close');
        },timeout);
      }
    },

    render: function() {

      this.renderTemplate();

      this.addChildPage('queues', new QueuesView());
      this.addChildPage('io', new IOView());
      this.addChildPage('workers', new WorkersView());
      this.addChildPage('jobs', new JobsView());
      this.addChildPage('scheduledjobs', new ScheduledJobsView());
      this.addChildPage('taskpaths', new TaskPathsView());
      this.addChildPage('taskexceptions', new TaskExceptionsView());
      this.addChildPage('index', new IndexView());
      this.addChildPage('status', new StatusView());

      return this;
    }
  });

});
