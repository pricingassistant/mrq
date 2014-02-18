define(["views/generic/page", "underscore", "jquery"],function(Page, _, $) {

  var noop = function() {};

 /**
   * A page with a main DataTable instance bound to this.col
   *
   */
  return Page.extend({

    initDataTable:function() {

      if (this.col) {
        this.col.on("remove",function(m,c,options) {
          noop(m,c); //required for jshint :/
          if (this.dataTable) {
            this.dataTable.fnDeleteRow(options.index);
          }
        },this);
        this.col.on("add",function(m,c/*,options*/) {
          noop(c); //required for jshint :/
          if (this.dataTable) {
            this.dataTable.fnAddData([m.toJSON()]);
          }
        },this);
      }

    },

    refreshDataTable:function() {
      if (!this.dataTable) return this.flush();

      this.dataTable.fnReloadAjax();
    },

    renderFilters: function() {
      var self = this;

      if (!this._rendered) return;

      if (this.filters["manufacturer"]) {
        this.$('.js-filter-manufacturer .js-filter-txt').html("Manufacturer: "+this.filters["manufacturer"]["name"]);
      }

      if (this.filters["category"]) {
        this.$('.js-filter-category .js-filter-txt').html("Category: "+this.filters["category"]["name"]);
      }

      if (this.filters["order"]) {
        this.$('.js-filter-order .js-filter-txt').html("Order: "+this.filters["order"]["name"]);
      }

      if (this.filters["onlyStarred"]) {
        self.$("#js-store-products-display-only-starred").attr("checked",true);
      }

      if (this.filters["withCompetitors"]) {
        self.$("#js-store-products-filters-withcompetitors").attr("checked",true);
      }

      if (this.filters["withSales"]) {
        self.$("#js-store-products-filters-withsales").attr("checked",true);
      }

      if (this.filters["flagHidden"]) {
        self.$("#js-store-products-filters-flagvisible").attr("checked",false);
      } else {
        self.$("#js-store-products-filters-flagvisible").attr("checked",true);
      }

      if (this.filters["flagActive"]) {
        self.$("#js-store-products-filters-flagactive").attr("checked",true);
      }

      if (this.filters["flagCompetitive"]) {
        self.$("#js-store-products-filters-flagcompetitive").attr("checked",true);
      }

      if (this.filters["flagTracked"]) {
        self.$("#js-store-products-filters-flagtracked").attr("checked",true);
      }

      if (this.filters["flagWithWarning"]) {
        self.$("#js-store-products-filters-flagwithwarning").attr("checked",true);
      }

      var shown = 0;

      this.$('.js-filter-manufacturer ul').html('<li><a href="/#store/'+self.categories.store.id+'/products">All</a></li><li class="divider"></li>');
      self.categories.each(function(category) {
        if (category.get("type")!="manufacturer") return;
        if (!category.get("name")) return;
        shown++;
        self.$('.js-filter-manufacturer ul').append('<li data-filterid="'+category.id+'"><a href="/#store/'+self.categories.store.id+'/products">'+category.get("name")+'</a></li>');
      });

      if (shown === 0) {
        this.$('.js-filter-manufacturer').hide();
      } else {
        this.$('.js-filter-manufacturer').show();
      }

      shown = 0;

      this.$('.js-filter-category ul').html('<li><a href="/#store/'+self.categories.store.id+'/products">All</a></li><li class="divider"></li>');

      // TODO won't sort well for >9 depth

      var parentUl = self.$('.js-filter-category ul');

      _.each(self.categories.sortBy(function(x) { return x.get("depth")+' '+x.get("name"); }),function(category) {

        if (category.get("type")!="category") return;
        if (!category.get("name")) return;

        var depth = 0;
        var parentCategory = [];
        var subUl = false;
        if (category.get("parent")) {
          parentCategory = self.$('.js-filter-category ul li[data-filterid="'+category.get("parent")+'"]');
          if (parentCategory.length) {
            depth = parseInt(parentCategory.attr("data-depth"), 10)+1 || 0;

            subUl = parentCategory.children("ul");

            if (!subUl.length) {
              parentCategory.append("<ul style='list-style-type:none;padding-left:0px;margin-left:10px;'></ul>");
              subUl = $("ul", parentCategory);
            }
          }
          //console.log(category.get("type"), category.get("name"), "found "+parentCategory.html());
        }

        var paddingLeft = (depth || 0)*10;
        var toInsert = '<li data-depth="'+depth+'" data-filterid="'+category.id+'"><a href="/#store/'+self.categories.store.id+'/products">'+category.get("name")+'</a></li>'; /*  style="padding-left:'+(20+paddingLeft)+'px" */

        if (parentCategory.length) {

          //console.log(parentCategory.html());
          //console.log(category.get("name"));
          subUl.first().append(toInsert);
          //$(toInsert).insertAfter(parentCategory[0]);
        } else {
          parentUl.append(toInsert);
        }

        shown++;

      });

      if (shown === 0) {
        this.$('.js-filter-category').hide();
      } else {
        this.$('.js-filter-category').show();
      }

    },

    resetorderdropdown:function() {
      delete this.filters["order"];
      //TODO update name
      this.renderFilters();
    },

    dropdownchanged:function(evt) {

      evt.preventDefault();
      evt.stopPropagation();

      var categoryId = $(evt.currentTarget).data("filterid") || "";
      var button = $(evt.currentTarget).closest(".js-filter-dropdown");
      var type = button.data("filtertype");

      this.filters[type] = {"_id":categoryId, "name": $("a",evt.currentTarget).html()};

      this.flush();
    }

      /*

      TODO

      this.col.on("change",function() {
        if (this.dataTable) this.dataTable.fnReloadAjax();
      },this);
      */

  });

});
