define(["jquery", "underscore", "models", "views/generic/page"],function($, _, Models, Page) {

  return Page.extend({

    el: '.js-page-workergroups',

    template:"#tpl-page-workergroups",

    events:{
      "click .submit": "submit"
    },

    render: function() {
      var self = this;
      $.get("api/workergroups").done(function(data) {
        self.renderTemplate();
        self.$("textarea").val(JSON.stringify(data["workergroups"], null, 8));
      });
    },

    submit: function(el) {
      var self = this;

      self.$("button")[0].innerHTML = "Wait...";

      var val = self.$("textarea").val();

      $.post("api/workergroups", {"workergroups": val}).done(function(data) {
        if (data.status != "ok") {
          return alert("There was an error while saving!");
        }
        self.$("button")[0].innerHTML = "Save";
      });
    }
  });

});
