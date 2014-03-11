// Set the require.js configuration for your application.
require.config({

  // Initialize the application with the main application file.
  deps: ["main"],

  paths: {

    // Libraries.
    circliful: "/static/js/vendor/jquery.circliful.min",
    jquery: "/static/js/vendor/jquery-2.1.0.min",
    underscore: "/static/js/vendor/underscore.min",
    backbone: "/static/js/vendor/backbone.min",
    backbonequeryparams: "/static/js/vendor/backbone.queryparams",
    bootstrap: "/static/js/vendor/bootstrap.min",
    datatables: "/static/js/vendor/jquery.dataTables.min",
    datatablesbs3: "/static/js/vendor/datatables.bs3",
    moment: "/static/js/vendor/moment.min",
    sparkline: "/static/js/vendor/jquery.sparkline.min"
  },

  urlArgs: "bust=" +  (new Date()).getTime(),


  shim: {

    backbone: {
      deps: ["underscore", "jquery"],
      exports: "Backbone"
    },
    bootstrap: {
      deps: ["jquery"]
    },
    datatables: {
      deps: ["jquery"]
    },
    datatablesbs3: {
      deps: ["datatables"]
    },
    backbonequeryparams: {
      deps: ["backbone"]
    },
    sparkline: {
      deps: ["jquery"]
    }

  }

});
