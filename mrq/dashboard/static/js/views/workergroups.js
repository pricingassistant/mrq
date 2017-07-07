define(["jquery", "underscore", "models", "views/generic/page", "quicksettings"],function($, _, Models, Page, QuickSettings) {

  return Page.extend({

    el: '.js-page-workergroups',

    template:"#tpl-page-workergroups",

    events:{
      "click .submit": "submit"
    },

    render: function() {
      var _this = this;

      self.$("button")[0].setAttribute('style', 'top:80px;left:10px;');
      self.workers_panel = []

      self.command_panel = QuickSettings.create(50, 80, "Actions", self.$(_this.el)[0])
                                        .addButton("Save")
                                        .addButton("Reload")
                                        .setWidth(100);

      $.get("/api/workergroups").done(function(data) {
        var i = 0;
        _.forEach(data["workergroups"], function(workgroup, workgroup_name) {
          var worker_panel = QuickSettings.create(200 + i * 350, 80, "Worker group configuration", self.$(_this.el)[0])
                                          .addText("Workgroup Name", workgroup_name)
                                          .hideTitle("Workgroup Name")
                                          .setDraggable(false)
                                          .setHeight(850)
                                          .setWidth(300);
          _.forEach(workgroup["profiles"], function(profile, profile_name) {
            worker_panel.addHTML("separator", "")
                         .hideTitle("separator")
                         .addText("Profile Name", profile_name)
                         .addText("Memory", profile["memory"])
                         .addRange("MinCount", 0, 100, profile["min_count"], 1)
                         .addRange("MaxCount", 0, 100, profile["max_count"], 1)
                         .addRange("CPU", 0, 1000, profile["cpu"], 100)
                         .addTextArea("Command", profile["command"])
          });
          self.workers_panel.push(worker_panel);
          console.log(workgroup_name, workgroup);
          i++;
        });
      });
    },

    submit: function(el) {
      var self = this;

      self.$("button")[0].innerHTML = "Wait...";

      var val = self.$("textarea").val();

      $.post("/api/workergroups", {"workergroups": val}).done(function(data) {
        if (data.status != "ok") {
          return alert("There was an error while saving!");
        }
        self.$("button")[0].innerHTML = "Save";
      });
    }
  });
});
