define(["jquery", "underscore", "views/generic/page", "models", "moment", "circliful"]
  ,function($, _, Page, Models, moment) {

  return Page.extend({

    el: '.js-page-index',

    template:"#tpl-page-index",

    events:{
    },

    initialDoneJobs: 0,

    fetchStats: function(cb) {
      var self = this;
      $.get("/workers").done(function (data) {
        workers = data.workers;
        var poolSize = 0;
        var currentJobs = 0;
        var doneJobs = 0;

        for (var i in workers) {
          poolSize += workers[i].config.greenlets
          currentJobs += workers[i].jobs.length
          doneJobs += workers[i].done_jobs
        }

        if (poolSize == 0)
          var utilization = 0;
        else
          var utilization = Math.round((currentJobs / poolSize) * 100)

        if (self.initialDoneJobs == 0) {
          self.initialDoneJobs = doneJobs
        }

        cb.apply(self, [poolSize, currentJobs, utilization, doneJobs - self.initialDoneJobs]);
      });
    },

    buildCircle: function (selector, text, percent, info, animation) {
      if (animation) {
        animation = 1;
      } else {
        animation = percent;
      }

      return  '<div class="stat"\
                    id="' + selector + '"\
                    data-dimension="250"\
                    data-text="' + text + '"\
                    data-info="' + info + '"\
                    data-width="30"\
                    data-fontsize="38"\
                    data-percent="' + percent + '"\
                    data-fgcolor="#61a9dc"\
                    data-bgcolor="#eee"\
                    data-animation-step="' + animation + '"\
                    data-fill="#ddd"></div>';

    },

    renderCircleStats: function (scope, selector, text, percent, info, animation) {
      self.$(scope).append(this.buildCircle(selector, text, percent, info, animation));
      self.$("#" + selector).circliful();
    },


    renderStats: function (poolSize, currentJobs, utilization, doneJobs) {
      var self = this;
      var scope = ".js-circle-row";
      var jobSpeed = Math.round(self.getCounterSpeed("overall-done-jobs") * 100) / 100;
      var values = self.addToCounter("overall-done-jobs", jobSpeed, 50).join(",");

      this.renderCircleStats(scope, "poolSizeStat", poolSize, 100, "Pool Size", true);
      this.renderCircleStats(scope, "utilizationStat", utilization + "%", utilization, "Utilization (" + currentJobs + " jobs)")
      this.renderCircleStats(scope, "jobspeed", jobSpeed, 100, "jobs/sec");

      self.$(scope).append('<div class=stat><span class="inlinesparkline" values="' + values + '"></span><span class="sparkline-title">Done Jobs</span></div>');
      self.$(".inlinesparkline").sparkline("html", {"width": "250px", "height": "200px", "defaultPixelsPerValue": 1});
    },

    refreshCircleStats: function (poolSize, currentJobs, utilization) {
      var self = this;
      var jobSpeed = Math.round(self.getCounterSpeed("overall-done-jobs") * 100) / 100;

      if (poolSize != $("#poolSizeStat").data("text")) {
        $("#poolSizeStat").html("");
        $("#poolSizeStat").data("text", poolSize);
        $("#poolSizeStat").circliful();
      }

      $("#utilizationStat").html("");
      $("#utilizationStat").data("percent", utilization);
      $("#utilizationStat").data("text", utilization + "%");
      $("#utilizationStat").data("info", "Utilization (" + currentJobs + " jobs)");
      $("#utilizationStat").circliful();

      $("#jobspeed").html("");
      $("#jobspeed").data("text", jobSpeed);
      $("#jobspeed").circliful();


      },

    refreshStats: function (poolSize, currentJobs, utilization, doneJobs) {
      var self = this;
      var values = self.addToCounter("overall-done-jobs", doneJobs, 50).join(",");

      self.$(".inlinesparkline").attr("values", values);
      self.$(".inlinesparkline").sparkline("html", {"width": "250px", "height": "200px", "defaultPixelsPerValue": 1});
      self.refreshCircleStats(poolSize, currentJobs, utilization);
      self.autoUpdateStats();
    },

    render: function() {
      var self = this;

      self.renderTemplate();
      self.fetchStats(self.renderStats);
      self.autoUpdateStats();
    },

    autoUpdateStats: function() {
      var self = this;
      var refreshInterval = parseInt($(".js-autorefresh").val(), 10) * 1000;

      if(refreshInterval > 0) {
        setTimeout(function () {
          self.fetchStats(self.refreshStats);
        }, refreshInterval);
      } else {
        // Check if the option has changed again in 2 seconds
        setTimeout(function () {
          self.autoUpdateStats();
        }, 2000);
      }
    }

  });

});
