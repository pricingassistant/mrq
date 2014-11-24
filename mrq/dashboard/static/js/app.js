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

    root:"/",

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

    login:function(email,pwd,cb) {
      this.checkAuth(email,pwd,function(err, user) {
        if (err) {
          alert("Error while logging in. Please retry later!");
          return cb(err);
        }
        if (!user) {
          cb(null,false);
          app.router.navigate("login", {trigger: true});
        } else {
          app.user.set(user);

          cb(null, true);

          if (window.location.hash.length<2 || window.location.hash=="#login") {
            app.router.navigate("", {trigger: true});
          }


        }
      });
    },

    checkAuth:function(email,pwd,cb) {
      var opts = {
        dataType:"json",
        success:function(body) {
          if (body=="0") return cb(null,false);
          cb(null,body);
        },
        error:function(err) {
          if (err.status==403) return cb(null,false);
          cb(err);
        }
      };

      if (email) {
        opts.url = "/api/auth/login";
        opts.type = "POST";
        opts.data = {"userEmail":email,"userPassword":pwd};
      } else {
        opts.url = "/api/auth/me";
      }
      return $.ajax(opts);
    },

    logout:function() {
      $.ajax({
        "url":"/api/auth/logout",
        "type":"POST",
        success:function( /*body*/ ) {
          app.user.clear();
          app.router.navigate("login",true);
        }
      });
    },

    getWorkersData: function(cb) {
      $.ajax({
        "url":"/api/datatables/workers?iDisplayLength=1000&iDisplayStart=0&sEcho=0",
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
        "url":"/api/datatables/workers?iDisplayLength=1000&iDisplayStart=0&sEcho=0",
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
