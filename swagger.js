const swaggerJSDoc = require("swagger-jsdoc");
const swaggerUi = require("swagger-ui-express");

// Configurazione Swagger
const swaggerOptions = {
  swaggerDefinition: {
    openapi: "3.0.0",
    info: {
      title: "Frinder API",
      version: "1.0.0",
      description: "Frinder APIs",
      contact: {
        name: "Davide Bettero",
        email: "davide.bettero@horsa.it",
      },
    },
    servers: [
      {
        url: "http://localhost:8000/api",
        description: "Frinder APIs",
      },
    ],
  },
  apis: ["./src/routes/*.js"], // Questo indica dove Swagger cercherà i commenti/documentazione delle API
};

const swaggerDocs = swaggerJSDoc(swaggerOptions);

module.exports = (app) => {
  app.use("/api-docs", swaggerUi.serve, swaggerUi.setup(swaggerDocs));
};
