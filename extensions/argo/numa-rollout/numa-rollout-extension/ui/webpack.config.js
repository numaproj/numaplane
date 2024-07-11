const path = require("path");
const webpack = require("webpack");
const TerserWebpackPlugin = require("terser-webpack-plugin");
// What are the options for groupKind
const groupKind = "argoproj.io/Rollout";
const extName = "Numarollout";

const config = {
  entry: {
    extension: "./src/index.tsx",
  },
  output: {
    filename: `extensions-${extName}.js`,
    path: __dirname + `/dist/resources/extension-${extName}.js`,
    libraryTarget: "window",
    library: ["tmp", "extensions"],
  },
  resolve: {
    extensions: [".ts", ".tsx", ".js", ".json", ".ttf"],
    fallback: {
      url: require.resolve("url/"),
      // You can add other polyfills if needed
    },
  },
  externals: {
    react: "React",
    "react-dom": "ReactDOM",
    moment: "Moment",
  },
  optimization: {
    minimize: true,
  },
  module: {
    rules: [
      {
        test: /\.(ts|js)x?$/,
        exclude: /node_modules/,
        use: [
          {
            loader: "ts-loader",
            options: {
              // specify TypeScript compiler options
              compilerOptions: {
                target: "es5",
              },
              appendTsSuffixTo: [/\.vue$/],
              appendTsxSuffixTo: [/\.vue$/],
              transpileOnly: true,
            },
          },
        ],
      },
      {
        test: /\.tsx?$/,
        include: [/argo-rollouts\/ui/, /argo-ui/], // Add argo-ui package here
        use: [
          {
            loader: "ts-loader",
            options: {
              transpileOnly: true,
            },
          },
        ],
      },
      {
        test: /\.scss$/,
        use: ["style-loader", "raw-loader", "sass-loader"],
      },
      {
        test: /\.css$/,
        use: ["style-loader", "raw-loader"],
      },
    ],
  },
};

module.exports = config;
