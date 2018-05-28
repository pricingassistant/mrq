define([
  "jquery", "underscore", "backbone",
  "router",
  "models",
  "views/root"
],function(
    $, _, Backbone,
    Router,
    Models,
    RootView
  ) {


  var app = {

    genericviews: {},

    views:{},

    models: {},

    options: {},

    user:false,

    /**
     * Main views controlled by the app object through its router:
     * account view, bootstrap tool view, app view.
     *
     * The views are initialized in when the app object is started,
     * or when the first in-app route is executed for the appView
     * (and each time the user switches to another application)
     */
    rootView: null,

    start: function(options) {

      app.options = _.defaults(options,{
        "container":"body"
      });

      // app.user = new User();

      app.rootView = new RootView({el:app.options.container, app:this});
      app.rootView.setApp(app);
      app.rootView.show();

      var self = this;

      app.router = new Router(self);
      Backbone.history.start();

    },

    getWorkersData: function(cb) {
      $.ajax({
        "url":"api/datatables/workers?iDisplayLength=1000&iDisplayStart=0&sEcho=0",
        "type":"GET",
        success:function( body ) {
          cb(false, body.aaData);
        },
        error: function() {
          cb(true);
        }
      });
    },

    getJobsDataFromWorkers: function(cb) {
      $.ajax({
        "url":"api/datatables/workers?iDisplayLength=1000&iDisplayStart=0&sEcho=0",
        "type":"GET",
        success:function( body ) {

          var jobs = [];

          _.each(body.aaData || [], function(worker) {
            _.each(worker.jobs || [], function(job) {
              job.worker = worker._id;
              jobs.push(job);
            });
          });

          cb(false, jobs);
        },
        error: function() {
          cb(true);
        }
      });
    },



    // if realtime=true, cb may be called multiple times.
    getTask:function(id,realtime,cb) {
      var cnt=true;
      var task = new Task({_id:id});
      task.bind("change",function() {
        if (task.get("finished")) {
          cnt=false;
          cb(null,task);
        }
      });
      if (realtime) {
        //TODO replace with proper socket.io
        var fetch = function() {
          if (!cnt) return;
          task.fetch({success:function() {
            setTimeout(fetch,3000);
          },error:function() {
            setTimeout(fetch,10000);
          }});
        };
        fetch();
      } else {
        task.fetch();
      }
    }
  };

  //Mixin event support
  _.extend(app, Backbone.Events);


  return app;


});
