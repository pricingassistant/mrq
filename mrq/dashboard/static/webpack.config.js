const path = require('path');
const UglifyJSPlugin = require('uglifyjs-webpack-plugin');
const webpack = require('webpack');

module.exports = {
    entry: './js/main.js',
    resolve: {
        modules: ['./'],
        alias: {
            circliful: "js/vendor/jquery.circliful.min",
            jquery: "js/vendor/jquery-2.1.0.min",
            underscore: "js/vendor/underscore.min",
            backbone: "js/vendor/backbone.min",
            backbonequeryparams: "js/vendor/backbone.queryparams",
            bootstrap: "js/vendor/bootstrap.min",
            datatables: "js/vendor/jquery.dataTables.min",
            datatablesbs3: "js/vendor/datatables.bs3",
            moment: "js/vendor/moment.min",
            sparkline: "js/vendor/jquery.sparkline.min",
        }
    },
    output: {
        path: path.resolve(__dirname, 'bin'),
        publicPath: '/static/bin/',
        filename: 'bundle.js'
    },
    plugins: [
        new webpack.IgnorePlugin(/^\.\/lang$/, /.*/),
        new webpack.ProvidePlugin({
            $: "jquery",
            jQuery: "jquery",
            "window.jQuery": "jquery",
            _: "underscore",
            Backbone: "backbone",
            moment: "moment"
        }),
        new UglifyJSPlugin({compress: {warnings: false}})
    ],
    module: {
        loaders: [
            {test: /\.css$/, use: [ 'style-loader', 'css-loader' ]},
            {test: /\.(png|woff|woff2|eot|ttf|svg)$/, loader: 'file-loader'}
        ]
    }
};
