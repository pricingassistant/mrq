define(["backbone", "underscore", "jquery"],function(Backbone, _, $) {

  return Backbone.Router.extend({

    routes: {

      '': 'index',
      'queues': 'queues',
      'workers': 'workers',
      'taskpaths': 'taskpaths',
      'taskexceptions': 'taskexceptions',
      'jobs': 'jobs',
      'io': 'io',
      'scheduledjobs': 'scheduledjobs',
      'status': 'status',
      'workergroups': 'workergroups',
      'agents': 'agents'
    },

    /**
     * Whenever a route is matched by the Router, a route:[name] event is triggered
     * ([name] is the name of the matched route).
     */
    initialize: function(app) {

      this.app = app;
      this.bind("all", this.change );

      var self = this;
      setTimeout(function() {
        self.change(false);
      }, 2000);

    },

    /**
     * Executed at every route.
     *
     * Scrolls the viewport back to the top of the content.
     */
    change: function(evt) {
      if (evt) this.goToTop();
    },


    getFromQueryString:function(qs,names) {

      var ret = {};
      _.each(names,function(name) {
        var match = (new RegExp('(^|[?&])' + name + '=([^&]*)')).exec(qs);

        ret[name]=match ?
            decodeURIComponent(match[2].replace(/\+/g, ' '))
            : null;
      });

      return ret;

    },

    navigateTo: function(path, query) {

      var filteredQuery = {};
      _.each(query,function(item,key) {
        if (item!==null && item!==undefined && item!=="") {
          filteredQuery[key] = item;
        }
      });

      var queryString = '';
      if (_.size(filteredQuery)) {
        queryString = '?' + $.param(filteredQuery);
      }

      var route = '/' + path + queryString;

      return this.navigate(route, true);
    },

    goToTop: function() {

      var docViewTop = $(window).scrollTop();
      //var elemTop = $('nav').offset().top;
      var elemTop = 0;
      if (elemTop <= docViewTop) {
        $('html, body').animate({ scrollTop: 0 }, 0); // set speed to anything else than 0 if an animation is wanted
      }
    },

    setNavbar:function(id) {
      $("#navbar-main li").removeClass("active");
      $("#navbar-main li.js-nav-"+id).addClass("active");
    },

    queues: function() {
      this.setNavbar("queues");
      this.app.rootView.showChildPage('queues');
    },

    index: function() {
      this.setNavbar("index");
      this.app.rootView.showChildPage('index');
    },

    status: function() {
      this.setNavbar("status");
      this.app.rootView.showChildPage('status');
    },

    workers: function(params) {
      this.setNavbar("workers");
      this.app.rootView.showChildPage('workers', {"options": {"params": params || {}}});
    },

    scheduledjobs: function() {
      this.setNavbar("scheduledjobs");
      this.app.rootView.showChildPage('scheduledjobs');
    },

    jobs: function(params) {
      this.setNavbar("jobs");
      this.app.rootView.showChildPage('jobs', {"options": {"params": params || {}}});
    },

    io: function(params) {
      this.setNavbar("io");
      this.app.rootView.showChildPage('io', {"options": {"params": params || {}}});
    },

    workergroups: function(params) {
      this.setNavbar("workergroups");
      this.app.rootView.showChildPage('workergroups', {"options": {"params": params || {}}});
    },

    agents: function(params) {
      this.setNavbar("agents");
      this.app.rootView.showChildPage('agents', {"options": {"params": params || {}}});
    },

    taskpaths: function(params) {
      this.setNavbar("taskpaths");
      this.app.rootView.showChildPage('taskpaths', {"options": {"params": params || {}}});
    },

    taskexceptions: function(params) {
      this.setNavbar("taskexceptions");
      this.app.rootView.showChildPage('taskexceptions', {"options": {"params": params || {}}});
    },

    worker: function(id) {
      this.setNavbar("workers");
      this.app.rootView.showChildPage('worker', {"options": {"id": id}});
    }

  });

});
