// Set the require.js configuration for your application.
require.config({

  // Initialize the application with the main application file.
  deps: ["main"],

  paths: {

    // Libraries.
    jquery: "/static/js/vendor/jquery-2.1.0.min",
    underscore: "/static/js/vendor/underscore.min",
    backbone: "/static/js/vendor/backbone.min",
    bootstrap: "/static/js/vendor/bootstrap.min",
    datatables: "/static/js/vendor/jquery.dataTables.min",
    datatablesbs3: "/static/js/vendor/datatables.bs3"
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
    }

  }

});
