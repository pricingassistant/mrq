define(["jquery", "underscore", "views/generic/datatablepage", "models"], function ($, _, DataTablePage, Models) {

    return DataTablePage.extend({

        el: '.js-page-taskpaths',

        template: "#tpl-page-taskpaths",

        events: {
            "click .js-datatable-filters-submit": "filterschanged"
        },

        initFilters: function () {
            this.filters = {
                "name": this.options.params.name || "",
            };
        },

        setOptions: function (options) {
            this.options = options;
            this.initFilters();
            this.flush();
        },

        renderDatatable: function () {

            var self = this;

            var datatableConfig = self.getCommonDatatableConfig("taskpaths");

            _.extend(datatableConfig, {
                "aoColumns": [

                    {
                        "sTitle": "Name",
                        "sClass": "col-name",
                        "sType": "string",
                        "mData": function (source, type, val) {
                            return "<a href='/#jobs?path=" + source._id + "'>" + source._id + "</a>";
                        }
                    },
                    {
                        "sTitle": "Jobs",
                        "sClass": "col-jobs",
                        "sType": "numeric",
                        "mData": function (source, type, val) {
                            var cnt = source.jobs || 0;

                            if (type == "display") {
                                return "<a href='/#jobs?path=" + source._id + "'>" + cnt + "</a>"
                                    + "<br/>"
                                    + '<span class="inlinesparkline" values="' + self.addToCounter("taskpath." + source._id, cnt, 50).join(",") + '"></span>';
                            } else {
                                return cnt;
                            }
                        }
                    },
                    {
                        "sTitle": "Speed",
                        "sClass": "col-eta",
                        "sType": "numeric",
                        "mData": function (source, type, val) {
                            return (Math.round(self.getCounterSpeed("taskpath." + source._id) * 100) / 100) + " jobs/second";
                        }
                    },
                    {
                        "sTitle": "ETA",
                        "sClass": "col-eta",
                        "sType": "numeric",
                        "mData": function (source, type, val) {
                            return self.getCounterEta("taskpath." + source._id, source.jobs || 0);
                        }
                    }

                ],
                "fnDrawCallback": function (oSettings) {

                    _.each(oSettings.aoData, function (row) {
                        var oData = row._aData;

                        $(".col-jobs .inlinesparkline", row.nTr).sparkline("html", {
                            "width": "100px",
                            "height": "30px",
                            "defaultPixelsPerValue": 1
                        });

                    });
                },
                "aaSorting": [[0, 'asc']],
            });

            this.initDataTable(datatableConfig);

        },

        filterschanged: function (evt) {
            var self = this;

            if (evt) {
                evt.preventDefault();
                evt.stopPropagation();
            }

            _.each(self.filters, function (v, k) {
                self.filters[k] = self.$(".js-datatable-filters-" + k).val();
            });
            this.updateTableData();
            window.location = "/#taskpath?" + $.param(self.filters, true).replace(/\+/g, "%20");
        },

        getFilterData: function () {
            return {
                sEcho: 1,
                name: $('#taskpath-form-name').val()
            };
        },

        updateTableData: function () {
            this.setTableData('/api/datatables/taskpaths', this.getFilterData());
        }
    });

});
