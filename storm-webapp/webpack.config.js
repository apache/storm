/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const path = require('path');
const webpack = require('webpack');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const CssMinimizerPlugin = require('css-minimizer-webpack-plugin');
const TerserPlugin = require('terser-webpack-plugin');

module.exports = {
  mode: 'production',
  entry: {
    main: './src/main/webapp/js/main-entry.js',
    visualize: './src/main/webapp/js/visualize-entry.js',
    flux: './src/main/webapp/js/flux-entry.js'
  },
  output: {
    path: path.resolve(__dirname, 'target/generated-resources/WEB-INF/dist'),
    filename: '[name].bundle.js',
    clean: true
  },
  module: {
    rules: [
      {
        test: /\.css$/,
        use: [
          MiniCssExtractPlugin.loader,
          { loader: 'css-loader', options: { url: false } }
        ]
      }
    ]
  },
  plugins: [
    new MiniCssExtractPlugin({
      filename: '[name].bundle.css'
    }),
    // Note: we do NOT use ProvidePlugin for jQuery — it conflicts with
    // DataTables' factory pattern that receives $ as a parameter.
    // Instead, the entry points set window.$ and window.jQuery explicitly.
    new webpack.DefinePlugin({
      PACKAGE_TIMESTAMP: JSON.stringify(
        process.env.PACKAGE_TIMESTAMP || Date.now().toString()
      )
    })
  ],
  resolve: {
    fallback: {
      // js-yaml references Node.js buffer for binary type handling,
      // which is not needed in browser usage
      buffer: false
    }
  },
  performance: {
    // Bundled vendor libraries exceed the default 244 KiB limit;
    // this is expected for a traditional jQuery UI with many plugins
    maxAssetSize: 1000000,
    maxEntrypointSize: 1200000
  },
  optimization: {
    minimizer: [
      new TerserPlugin(),
      new CssMinimizerPlugin()
    ]
  }
};
