define(["jquery", "underscore", "views/generic/page", "models", "moment", "circliful"]
  ,function($, _, Page, Models, moment) {

  return Page.extend({

    el: '.js-page-index',

    template:"#tpl-page-index",

    events:{
    },


    fetchStats: function(cb) {
      var self = this;
      $.get("/workers").done(function (data) {
        workers = data.workers;
        var poolSize = 0;
        var currentJobs = 0;
        var doneJobs = 0;

        for (var i in workers) {
          poolSize += workers[i].config.gevent
          currentJobs += workers[i].jobs.length
          doneJobs += workers[i].done_jobs
        }

        if (poolSize == 0)
          var utilization = 0;
        else
          var utilization = Math.round((currentJobs / poolSize) * 100)

        cb.apply(self, [poolSize, currentJobs, utilization, doneJobs]);
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
      $(scope).append(this.buildCircle(selector, text, percent, info, animation));
      $("#" + selector).circliful();
    },


    renderStats: function (poolSize, currentJobs, utilization, doneJobs) {
      var self = this;
      var scope = ".js-circle-row";
      var values = self.addToCounter("overall-done-jobs", 0, 50).join(",");
      var jobSpeed = Math.round(self.getCounterSpeed("overall-done-jobs") * 100) / 100;

      this.renderCircleStats(scope, "poolSizeStat", poolSize, 100, "Pool Size", true);
      this.renderCircleStats(scope, "utilizationStat", utilization + "%", utilization, "Utilization (" + currentJobs + " jobs)")
      this.renderCircleStats(scope, "jobspeed", jobSpeed, 100, "jobs/sec");

      values = self.addToCounter("overall-done-jobs", doneJobs, 50).join(",")
      $(scope).append('<div class=stat><span class="inlinesparkline" values="' + values + '"></span><span class="sparkline-title">Done Jobs</span></div>');
      $(".inlinesparkline").sparkline("html", {"width": "250px", "height": "200px", "defaultPixelsPerValue": 1});
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
      var refreshInterval = parseInt($(".js-autorefresh").val(), 10) * 1000;

      $(".inlinesparkline").attr("values", values);
      $(".inlinesparkline").sparkline("html", {"width": "250px", "height": "200px", "defaultPixelsPerValue": 1});
      self.refreshCircleStats(poolSize, currentJobs, utilization);

      setTimeout(function () {
        self.fetchStats(self.refreshStats);
      }, refreshInterval);
    },

    render: function() {
      var self = this;
      var refreshInterval = parseInt($(".js-autorefresh").val(), 10) * 1000;

      self.renderTemplate();
      self.fetchStats(self.renderStats);

      setTimeout(function () {
        self.fetchStats(self.refreshStats);
      }, refreshInterval);
    }

  });

});
