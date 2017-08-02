define(["jquery", "underscore", "models", "views/generic/page", "quicksettings"],function($, _, Models, Page, QuickSettings) {

  return Page.extend({

    el: '.js-page-workergroups',

    template:"#tpl-page-workergroups",

    events:{
      "click .submit": "submit"
    },

    addCommandPanel: function() {
      var _this = this;

      if (typeof this.commandPanel === "undefined")
      {
        this.commandPanel = QuickSettings.create(25, 80, "Actions", $(this.el)[0])
                                         .addButton("Add a Worker Group", function() {
                                           _this.addPanel();
                                         })
                                         .addButton("Save", function() {
                                           _this.save();
                                         })
                                         .addButton("Reload", function() {
                                           _this.reload();
                                         })
                                         .addHTML("Status", "<font color=\"green\">OK</font>")
                                         .setDraggable(false)
                                         .setWidth(150);
      }
    },

    addPanel: function(workergroup = null, workergroupName = "") {
      var _this = this;
      var panelIndex = this.workergroupPanels.length;
      var workerPanel = QuickSettings.create(225 + this.workergroupPanels.length * 350, 80, "Worker group configuration", $(this.el)[0])
                                      .addText("Workgroup Name", workergroupName)
                                      .hideTitle("Workgroup Name")
                                      .addButton("Remove this Worker Group", function() {
                                        _this.workergroupPanels[panelIndex].destroy();
                                        try {
                                          delete _this.workergroupPanels[panelIndex];
                                        }
                                        catch (e) {}
                                        _this.workergroupPanels[panelIndex] = null;
                                      })
                                      .addText("Process Termination Timeout", workergroup ? workergroup["process_termination_timeout"] : 0)
                                      .addButton("Add a Profile", function() {
                                        _this.addProfileToPanel(_this.workergroupPanels[panelIndex]);
                                      })
                                      .setDraggable(false)
                                      .setHeight(850)
                                      .setWidth(300);
      workerPanel.profilesNumber = 0;
      this.workergroupPanels.push(workerPanel);
      if (workergroup != null)
      {
        this.serials[workergroupName] = workergroup["serial"]
        _.forEach(workergroup["profiles"], function(profile, profileName) {
          this.addProfileToPanel(workerPanel, profile, profileName);
        }, this);
      }
    },

    addProfileToPanel: function(workerPanel, profile = null, profileName = "") {
      workerPanel.profilesNumber += 1;
      header = "Profile " + String(workerPanel.profilesNumber) + " - ";
      workerPanel.addHTML("separator", "<br>")
                 .hideTitle("separator")
                 .addText(header + "Profile Name", profileName)
                 .addText(header + "Memory", profile ? profile["memory"]: 0)
                 .addText(header + "CPU", profile ? profile["cpu"] : 0)
                 .addRange(header + "MinCount", 0, 100, profile ? profile["min_count"] : 0, 1)
                 .addRange(header + "MaxCount", 0, 100, profile ? profile["max_count"] : 0, 1)
                 .addTextArea(header + "Command", profile ? profile["command"]: "")
                 .addButton("Remove this Profile");
    },

    remove_profile: function() {
    },

    reload: function(force = false) {
      if (force || confirm('It will discard every changes that hasn\'t be saved. Are you sure?')) {
        _.forEach(this.workergroupPanels, function(panel) {
          if (panel !== null && panel !== undefined)
            panel.destroy();
            delete panel;
        })
        this.render();
      }
    },


    // Check for "continue" usage instead of nested ifs
    save: function() {
      var _this = this;
      this.commandPanel._controls["Status"].setValue("<font color=\"orange\">Saving...</font>");

      data = {};
      _.forEach(this.workergroupPanels, function(panel) {
        // Equivalent of python's "not in"
        if ($.inArray(panel, [null, undefined]) == -1)
        {
          panelJSON = panel.getValuesAsJSON();
          if ($.inArray(panelJSON["Workgroup Name"], [null, ""]) == -1)
          {
            workergroup = {
              "profiles" : {},
              "process_termination_timeout": parseInt(panelJSON["Process Termination Timeout"], 10)
            }

            if (panelJSON["Workgroup Name"] in _this.serials)
              workergroup["serial"] = _this.serials[panelJSON["Workgroup Name"]];

            _.forEach(_.range(1, panel.profilesNumber + 1), function(index) {
              header = "Profile " + String(index) + " - ";
              if ($.inArray(panelJSON[header + "Profile Name"], [null, ""]) == -1)
              {
                profile = {};
                profile["memory"] = parseInt(panelJSON[header + "Memory"], 10);
                profile["cpu"] = parseInt(panelJSON[header + "CPU"], 10);
                profile["min_count"] = parseInt(panelJSON[header + "MinCount"], 10);
                profile["max_count"] = parseInt(panelJSON[header + "MaxCount"], 10);
                profile["command"] = panelJSON[header + "Command"];
                workergroup["profiles"][panelJSON[header + "Profile Name"]] = profile;
              }
            })
            data[panelJSON["Workgroup Name"]] = workergroup;
          }
        }
      })

      $.post("/api/workergroups", {"workergroups": JSON.stringify(data)}).done(function(result) {
        if (result.status === "ok")
        {
          _this.commandPanel._controls["Status"].setValue("<font color=\"green\">Saved</font>");
          _this.reload(true);
        }
        else if (result.status === "outdated")
        {
          string = "";
          _.forEach(result.outdated_wgcs, function(wgc) {
            string += "- " + wgc + "<br>";
          })
          _this.commandPanel._controls["Status"].setValue("<font color=\"red\">These configurations were outdated and were not saved:<br>" + string + "</font><br>The others were saved.");
        }
        else
        {
          _this.commandPanel._controls["Status"].setValue("<font color=\"red\">FAILED</font>");
          return alert("There was an error while saving!");
        }
      });
    },

    render: function() {
      var _this = this;

      this.workergroupPanels = [];
      this.addCommandPanel();
      this.serials = {};

      $.get("/api/workergroups").done(function(data) {
        _.forEach(data["workergroups"], function(workergroup, workergroupName) {
          _this.addPanel(workergroup, workergroupName);
        });
      });
    }
  });
});
