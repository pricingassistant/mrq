define(["backbone", "underscore", "jquery"],function(Backbone, _, $) {


  var isMaintenance = false;

  // Uncomment this to put the interface in maintenance mode
  // Go to /#bypass at first page to still access the website
  // isMaintenance = window.location.toString().indexOf("bypass")==-1;

  var isFirstLoginError = true;


  var loaded = false;
  var loadedCallbacks = [];

  return Backbone.Router.extend({

    routes: isMaintenance?{'*splat': 'maintenance'}:{

      '': 'root',
      'login': 'login',
      'logout': 'logout',
      'store/:id': 'store_dashboard',
      'store/:id/products': 'store_products_index',
      'store/:id/products/:productid': 'store_products_detail',
      'store/:id/products/:productid/:outlierids': 'store_products_detail',
      'store/:id/competitors': 'store_competitors_index',
      'store/:id/competitors/:competitorid': 'store_competitors_detail',
      'store/:id/alerts/:type': 'store_alerts',
      'store/:id/alerts': 'store_alerts',
      'store/:id/pricechanges': 'store_pricechanges',
      'store/:id/pricechanges/product/:productid': 'store_pricechanges_product',

      'store/:id/settings': 'store_settings'

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

      if (typeof window.ClickTaleExec=='function') {
        var path = window.location.hash.replace(/^#/,"");
        window.ClickTaleExec("OXApp.router.navigate('"+path+"', {trigger: true});");
      }

      this.app.log_woopra_event("pv", {
        url: "/"+window.location.hash.replace(/^\#store\/[a-z0-9]+\//, ""),
        title: window.location.hash.replace(/^\#store\/[a-z0-9]+\//, "")
      });

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



    /**
     * Wrapper around the router's navigate function to force the application
     * to navigate to the given relative path within a store, possibly
     * passing an optional list of key/value parameters to the router.
     *
     * From its "path" and "query" parameters, the function builds the route:
     *  app/[current store id]/[path](?[params])
     * where [params] is a query string serialization of "query".
     *
     * The function then tells the router to navigate to and execute the
     * computed route.
     *
     * @function
     * @param {String} path Relative path to navigate the application to.
     * @param {Object} query key/value parameters to send as query parameters.
     */
    navigateTo: function(path, query) {
      //Y NO reject on objects?
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

      var route = 'store/' + this.app.currentStore.id + '/' + path + queryString;

      return this.navigate(route, true);
    },

    /**
     * Force the window to go back to the top of the nav bar.
     *
     * This function should typically called whenever the current page
     * has changed, unless the new page is so close to the former one
     * that scrolling to the top of the screen would actually confuse
     * the user.
     */
    goToTop: function() {

      var docViewTop = $(window).scrollTop();
      //var elemTop = $('nav').offset().top;
      var elemTop = 0;
      if (elemTop <= docViewTop) {
        $('html, body').animate({ scrollTop: 0 }, 0); // set speed to anything else than 0 if an animation is wanted
      }
    },


    /**
     * Creates, retrieves or updates the Wizard view and the application
     * model instance that matches the given application.
     *
     * Asynchronous function. The callback function gets called when the
     * information about the given application has been fetched from the
     * backend.
     *
     * This function must be called before anything else for all routes
     * that start with "app/:id".
     *
     * @function
     * @param {String} storeid The store ID under processing
     * @param {function} cb Callback function called when all app info has
     *  been loaded.
     */
    switchStore: function(storeid, cb) {

      // we have to manage a global callback list because multiple switchStore() calls may be made before
      // the wizard is populated.
      if (cb) loadedCallbacks.push(cb);

      if (this.app.rootView &&
        this.app.currentStore &&
        (this.app.currentStore.id === storeid)) {
        // The wizard's view is already the right one
        // (although the app may not have been populated yet)
        if (loaded) {
          // Done loading the app, run registered callbacks
          // (our callback function at a minimum)
          for (var i=0;i<loadedCallbacks.length;i++) {
            loadedCallbacks[i]();
          }
          loadedCallbacks=[];
        }
        return;
      }

      this.app.setStoreId(storeid,function(err) {
        if (err) {
          if (isFirstLoginError) {
            isFirstLoginError=false;
          } else {
            console.error(err);
            alert("Loading failed. Please retry later a contact us if this continues!");
          }

          return;
        }

        loaded = true;
        for (var i=0;i<loadedCallbacks.length;i++) {
          loadedCallbacks[i]();
        }
        loadedCallbacks=[];
      });
    },

    root: function() {

      this.app.rootView.showChildPage('stores');

    },


    maintenance: function() {
      this.app.rootView.showChildPage('maintenance');
    },

    login: function() {
      this.app.rootView.showChildPage('login');
    },
    logout: function() {

      this.app.logout();
    },

    store_dashboard: function(storeid) {
      var self = this;

      this.switchStore(storeid, function() {
        self.app.rootView.showChildPage('store').showChildPage('dashboard');
      });
    },

    store_settings: function(storeid) {
      var self = this;

      this.switchStore(storeid, function() {
        self.app.rootView.showChildPage('store').showChildPage('settings');
      });
    },

    store_products_index: function(storeid, qs) {
      var self = this;

      var params = this.getFromQueryString(qs,["search","page"]);

      this.switchStore(storeid, function() {

        self.app.rootView.showChildPage('store').showChildPage('products').showChildPage('index',{options:{query:params}});

      });
    },

    store_products_detail: function(storeid, productid, outlierids) {
      var self = this;

      this.switchStore(storeid, function() {
         self.app.rootView.showChildPage('store').showChildPage('products').showChildPage('detail',{options:{contentid:productid,
                                                                                                             outlierids:outlierids}});
      });
    },


    store_competitors_index: function(storeid, qs) {
      var self = this;

      var params = this.getFromQueryString(qs,["search","page"]);

      this.switchStore(storeid, function() {

        self.app.rootView.showChildPage('store').showChildPage('competitorstores').showChildPage('index',{options:{query:params}});

      });
    },

    store_competitors_detail: function(storeid, competitorid) {
      var self = this;

      this.switchStore(storeid, function() {
         self.app.rootView.showChildPage('store').showChildPage('competitorstores').showChildPage('detail',{options:{contentid:competitorid}});
      });
    },

    store_alerts: function(storeid, type) {
      var self = this;

      var options = {};
      if (type !== undefined)
        options["type"] = type;
      this.switchStore(storeid, function() {
        self.app.rootView.showChildPage('store').showChildPage('alerts', {"options": options});
      });
    },

    store_pricechanges: function(storeid) {
      var self = this;

      this.switchStore(storeid, function() {
        self.app.rootView.showChildPage('store').showChildPage('pricechanges');
      });
    },
    store_pricechanges_product: function(storeid, productid) {
      var self = this;

      this.switchStore(storeid, function() {
        self.app.rootView.showChildPage('store').showChildPage('pricechanges', {"options": {"product": productid}});
      });
    }

  });

});
