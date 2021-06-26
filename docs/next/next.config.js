const redirectUrls = require("./util/redirectUrls.json");

module.exports = {
  async redirects() {
    return [
      {
        source: "/docs",
        destination: "/docs/latest",
        permanent: true,
      },
      {
        source: "/",
        destination: "/getting-started",
        permanent: true,
      },
      ...redirectUrls,
    ];
  },
  images: {
    domains: ["dagster-docs-versioned-content.s3.us-west-1.amazonaws.com"],
  },
};
