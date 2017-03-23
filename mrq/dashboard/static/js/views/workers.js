define(["jquery", "underscore", "views/generic/datatablepage", "models", "moment"], function ($, _, DataTablePage, Models, moment) {

    return DataTablePage.extend({

        el: '.js-page-workers',

        template: "#tpl-page-workers",

        events: {
            "change .js-datatable-filters-showstopped": "filterschanged",
            "click .js-workers-io": "showworkerio",
            "click .hide-time-filter": "hidetimefilter",
            "click .show-time-filter": "showtimefilter",
        },

        initFilters: function () {
            this.filters = {
                "showstopped": this.options.params.showstopped || "",
                "startTime": this.options.params.startTime || "",
                "endTime": this.options.params.endTime || ""
            };
            this.initTimeFilter();
        },

        initTimeFilter: function () {
            var self = this;
            $('.time-filter-group').click(function () {
                self.timeFilter.typeChanged(this);
            });
            $('.time-filter-tag').click(function () {
                self.filterRequest(this);
            });
        },

        hidetimefilter: function () {
            $(".time-filter-container").css({"position": "relative"});
            $(".time-filter-container").animate({
                bottom: "+265",
            }, {
                duration: 300,
                complete: function () {
                    $('.hide-time-filter').hide();
                    $('.show-time-filter').show();
                }
            });
        },

        showtimefilter: function () {
            $(".time-filter-container").css({"position": "relative"});
            $(".time-filter-container").animate({
                bottom: "0",
            }, {
                duration: 300,
                complete: function () {
                    $('.hide-time-filter').show();
                    $('.show-time-filter').hide();
                    $(".time-filter-container").css({"position": "static"});
                }
            });
        },

        setOptions: function (options) {
            this.options = options;
            this.initFilters();
            this.flush();
        },

        showworkerio: function (evt) {
            var self = this;

            var worker_id = $(evt.currentTarget).data("workerid");

            var worker_data = _.find(this.dataTableRawData.aaData, function (worker) {
                return worker._id == worker_id;
            });

            var html_modal = _.template($("#tpl-modal-workers-io").html())({"worker": worker_data});

            self.$(".js-workers-modal .js-workers-modal-content").html(html_modal);
            self.$(".js-workers-modal h4").html("I/O for this worker, by task &amp; by type");
            self.$(".js-workers-modal").modal({});

            return false;
        },

        renderDatatable: function () {

            var self = this;
            this.initFilters();

            var datatableConfig = self.getCommonDatatableConfig("workers");

            _.extend(datatableConfig, {
                "aoColumns": [

                    {
                        "sTitle": "Name",
                        "sClass": "col-name",
                        "sType": "string",
                        "sWidth": "150px",
                        "mData": function (source, type/*, val*/) {
                            return "<a href='/#jobs?worker=" + source._id + "'>" + source.name + "</a><br/><small>" + source.config.local_ip + " " + source._id + "</small>";
                        }
                    },
                    {
                        "sTitle": "Queues",
                        "sClass": "col-queues",
                        "sType": "string",
                        "mData": function (source, type/*, val*/) {
                            return _.map(source.config.queues || [], function (q) {
                                return "<a href='/#jobs?queue=" + q + "'>" + q + "</a>";
                            }).join(" ");
                        }
                    },
                    {
                        "sTitle": "Status",
                        "sClass": "col-status",
                        "sType": "string",
                        "sWidth": "80px",
                        "mData": function (source, type/*, val*/) {
                            return source.status;
                        }
                    },
                    {
                        "sTitle": "Last report",
                        "sClass": "col-last-report",
                        "sType": "string",
                        "sWidth": "150px",
                        "mData": function (source, type/*, val*/) {
                            if (type == "display") {
                                console.log(source);
                                return "<small>" + (source.datereported ? moment.utc(source.datereported).fromNow() : "Never")
                                    + "<br/>"
                                    + "started " + moment.utc(source.datestarted).fromNow() + "</small>";
                            } else {
                                return source.datereported || "";
                            }
                        }
                    },
                    {
                        "sTitle": "CPU usr/sys",
                        "sClass": "col-cpu",
                        "sType": "string",
                        "sWidth": "120px",
                        "mData": function (source, type/*, val*/) {

                            var usage = (source.process.cpu.user + source.process.cpu.system) * 1000 / (moment.utc(source.datereported || null).valueOf() - moment.utc(source.datestarted).valueOf());

                            var html = Math.round(source.process.cpu.user) + "s / " + Math.round(source.process.cpu.system) + "s"
                                + "<br/>"
                                + (Math.round(usage * 100)) + "% use";

                            if (((source.io || {}).types || []).length) {
                                html += "<br/>I/O: <a data-workerid='" + source._id + "' href='#' class='js-workers-io'>" + Math.round(source.io.total) + "s </a>";
                            }

                            return html;
                        }
                    },
                    {
                        "sTitle": "Memory",
                        "sClass": "col-mem",
                        "sType": "numeric",
                        "sWidth": "130px",
                        "mData": function (source, type/*, val*/) {
                            if (type == "display") {

                                return Math.round((source.process.mem.total / (1024 * 1024)) * 10) / 10 + "M"
                                    + "<br/>"
                                    + '<span class="inlinesparkline" values="' + self.addToCounter("worker.mem." + source._id, source.process.mem.total / (1024 * 1024), 50).join(",") + '"></span>';
                            } else {
                                return source.process.mem.total
                            }
                        }
                    },
                    {
                        "sTitle": "Done Jobs",
                        "sClass": "col-done-jobs",
                        "sType": "numeric",
                        "sWidth": "120px",
                        "mData": function (source, type/*, val*/) {
                            var cnt = (source.done_jobs || 0);
                            if (type == "display") {
                                return "<a href='/#jobs?worker=" + source._id + "'>" + cnt + "</a>"
                                    + "<br/>"
                                    + '<span class="inlinesparkline" values="' + self.addToCounter("worker.donejobs." + source._id, cnt, 50).join(",") + '"></span>';
                            } else {
                                return cnt;
                            }
                        }
                    },
                    {
                        "sTitle": "Speed",
                        "sClass": "col-eta",
                        "sType": "numeric",
                        "sWidth": "120px",
                        "mData": function (source, type, val) {
                            return (Math.round(self.getCounterSpeed("worker.donejobs." + source._id) * 100) / 100) + " j/s";
                        }
                    },
                    {
                        "sTitle": "Current Jobs",
                        "sClass": "col-current-jobs",
                        "sType": "numeric",
                        "sWidth": "120px",
                        "mData": function (source, type/*, val*/) {
                            var cnt = (source.jobs || []).length;
                            if (type == "display") {
                                return "<a href='/#jobs?worker=" + source._id + "&status=started'>" + cnt + "</a> / " + source.config.greenlets
                                    + "<br/>"
                                    + '<span class="inlinesparkline" values="' + self.addToCounter("worker.currentjobs." + source._id, cnt, 50).join(",") + '"></span>';
                            } else {
                                return cnt;
                            }
                        }
                    }

                ],
                "fnDrawCallback": function (oSettings) {
                    $(".inlinesparkline", oSettings.nTable).sparkline("html", {
                        "width": "100px",
                        "height": "30px",
                        "defaultPixelsPerValue": 1
                    });
                },
                "aaSorting": [[0, 'asc']],
            });

            this.initDataTable(datatableConfig);

        },

        getFilterData: function () {
            return {
                id: $('#jobs-form-id').val(),
                queue: $('#jobs-form-queue').val(),
                worker: $('#jobs-form-worker').val(),
                path: $('#jobs-form-path').val(),
                status: $('#jobs-form-status').val(),
                params: $('#jobs-form-params').val(),
                exceptiontype: $('#jobs-form-exceptiontype').val(),
                startTime: this.filters.startTime,
                endTime: this.filters.endTime,
                sEcho: 1
            };
        },

        timefiltergroupchanged: function () {
            this.timeFilter.typeChanged();
        },

        filterRequest: function (component) {
            var result = this.timeFilter.filterRequest(component);
            this.filters.startTime = result[0];
            this.filters.endTime = result[1];
            this.updateTableData();
        },

        updateTableData: function () {
            this.setTableData('/api/datatables/workers', this.getFilterData());
        },

        timeFilter: {
            show: function () {

            },
            hide: function () {

            },
            typeChanged: function (component) {
                $('.time-filter-group').removeClass('active');
                $(component).addClass('active');

            },
            filterRequest: function (component) {
                var criteria = $(component).attr('data-filter');
                var dateStart = new Date();
                var dateEnd = new Date();
                if (criteria != '') {
                    if (criteria.contains('last')) {
                        if (criteria.contains('min')) {
                            var mins = parseInt(criteria.split('_')[1]);
                            dateStart.setMinutes(dateStart.getMinutes() - mins);
                        }
                        if (criteria.contains('days')) {
                            var days = parseInt(criteria.split('_')[1]);
                            dateStart.setDate(dateStart.getDate() - days);
                        }
                        if (criteria.contains('hours')) {
                            var hours = parseInt(criteria.split('_')[1]);
                            dateStart.setHours(dateStart.getHours() - hours);
                        }
                    }
                    if (criteria == 'today') {
                        dateStart = this.getBeginingOfDay(dateStart);
                        dateEnd = this.getEndOfDay(dateEnd);
                    }
                    if (criteria == 'yesterday') {
                        dateStart.setDate(dateStart.getDate() - 1);
                        dateStart = this.getBeginingOfDay(dateStart);

                        dateEnd.setDate(dateEnd.getDate() - 1);
                        dateEnd = this.getEndOfDay(dateEnd);
                    }
                    if (criteria == '2_days_ago') {
                        dateStart.setDate(dateStart.getDate() - 2);
                        dateStart = this.getBeginingOfDay(dateStart);

                        dateEnd.setDate(dateEnd.getDate() - 2);
                        dateEnd = this.getEndOfDay(dateEnd);
                    }
                    if (criteria == '7_days_ago') {
                        dateStart.setDate(dateStart.getDate() - 7);
                        dateStart = this.getBeginingOfDay(dateStart);

                        dateEnd.setDate(dateEnd.getDate() - 7);
                        dateEnd = this.getEndOfDay(dateEnd);
                    }
                    if (criteria == 'this_week') {
                        dateStart.setDate(dateStart.getDate() +
                            (0 - dateStart.getDay()));
                        dateStart = this.getBeginingOfDay(dateStart);

                        dateEnd.setDate(dateEnd.getDate() + (6 - dateEnd.getDay()));
                        dateEnd = this.getEndOfDay(dateEnd);
                    }
                    if (criteria == 'week_until_now') {
                        dateStart.setDate(dateStart.getDate() +
                            (0 - dateStart.getDay()));
                        dateStart = this.getBeginingOfDay(dateStart);
                    }
                    if (criteria == 'prev_week') {
                        dateStart.setDate(dateStart.getDate() - 7);
                        dateStart.setDate(dateStart.getDate() +
                            (0 - dateStart.getDay()));
                        dateStart = this.getBeginingOfDay(dateStart);

                        dateEnd.setDate(dateEnd.getDate() - 7);
                        dateEnd.setDate(dateEnd.getDate() + (6 - dateEnd.getDay()));
                        dateEnd = this.getEndOfDay(dateEnd);
                    }
                    if (criteria == 'this_month') {
                        dateStart = this.getBeginingOfMonth(dateStart);

                        dateEnd = this.getEndOfMonth(dateEnd);
                    }
                    if (criteria == 'month_until_now') {
                        dateStart = this.getBeginingOfMonth(dateStart);
                    }
                    if (criteria == 'last_6_months') {
                        dateStart.setMonth(dateStart.getMonth() - 6);
                        dateStart = this.getBeginingOfMonth(dateStart);

                        dateEnd = this.getEndOfMonth(dateEnd);
                    }
                    if (criteria == 'prev_month') {
                        dateStart.setMonth(dateStart.getMonth() - 1);
                        dateStart = this.getBeginingOfMonth(dateStart);

                        dateEnd.setMonth(dateEnd.getMonth() - 1);
                        dateEnd = this.getEndOfMonth(dateEnd);
                    }
                    if (criteria == 'prev_year') {
                        dateStart.setFullYear(dateStart.getFullYear() - 1);
                        dateStart = this.getBeginingOfYear(dateStart);

                        dateEnd.setFullYear(dateEnd.getFullYear() - 1);
                        dateEnd = this.getEndOfYear(dateEnd);
                    }
                    if (criteria == 'this_year') {
                        dateStart = this.getBeginingOfYear(dateStart);

                        dateEnd = this.getEndOfYear(dateEnd);
                    }
                    if (criteria == 'last_year') {
                        dateStart.setFullYear(dateStart.getFullYear() - 1);
                        dateStart = this.getBeginingOfYear(dateStart);
                    }
                    if (criteria == 'year_until_now') {
                        dateStart = this.getBeginingOfYear(dateStart);
                    }
                    if (criteria == 'last_2_years') {
                        dateStart.setFullYear(dateStart.getFullYear() - 2);
                        dateStart = this.getBeginingOfYear(dateStart);
                    }
                    if (criteria == 'last_5_years') {
                        dateStart.setFullYear(dateStart.getFullYear() - 5);
                        dateStart = this.getBeginingOfYear(dateStart);
                    }
                    if (criteria == 'today_until_now') {
                        dateStart = this.getBeginingOfDay(dateStart);
                    }
                    console.log(dateStart);
                    console.log(dateEnd);
                    return [this.getISODate(dateStart), this.getISODate(dateEnd)];
                }
            },
            getBeginingOfDay: function (date) {
                date.setHours(0);
                date.setMinutes(0);
                date.setSeconds(0);
                date.setMilliseconds(0);
                return date;
            },
            getEndOfDay: function (date) {
                date.setHours(23);
                date.setMinutes(59);
                date.setSeconds(59);
                date.setMilliseconds(999);
                return date;
            },
            getBeginingOfMonth: function (date) {
                date.setDate(1);
                date = this.getBeginingOfDay(date);
                return date;
            },
            getEndOfMonth: function (date) {
                date.setMonth(date.getMonth() + 1);
                date = this.getBeginingOfMonth(date);
                date.setMilliseconds(-1);
                return date;
            },
            getBeginingOfYear: function (date) {
                date.setMonth(0);
                date = this.getBeginingOfMonth(date);
                return date;
            },
            getEndOfYear: function (date) {
                date.setFullYear(date.getFullYear() + 1);
                date = this.getBeginingOfYear(date);
                date.setMilliseconds(-1);
                return date;
            },
            getISODate: function (date) {
                return date.getFullYear() + "-" + (date.getMonth() + 1) + "-" +
                    date.getDate() + "T" + date.getHours() + ":" +
                    date.getMinutes() + ":" + date.getSeconds() + "." +
                    date.getMilliseconds();
            }

        }
    });

});
