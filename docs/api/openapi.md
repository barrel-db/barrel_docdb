# OpenAPI Specification

The Barrel DocDB HTTP API is documented using OpenAPI 3.0.

## Interactive Documentation

<div id="swagger-ui"></div>

<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />

<style>
  /* Dark theme adjustments for Material theme */
  .swagger-ui {
    background: transparent;
  }
  .swagger-ui .topbar {
    display: none;
  }
  .swagger-ui .info {
    margin: 20px 0;
  }
  .swagger-ui .scheme-container {
    background: transparent;
    box-shadow: none;
  }
  [data-md-color-scheme="slate"] .swagger-ui,
  [data-md-color-scheme="slate"] .swagger-ui .info .title,
  [data-md-color-scheme="slate"] .swagger-ui .info p,
  [data-md-color-scheme="slate"] .swagger-ui .info li,
  [data-md-color-scheme="slate"] .swagger-ui table thead tr th,
  [data-md-color-scheme="slate"] .swagger-ui table tbody tr td,
  [data-md-color-scheme="slate"] .swagger-ui .opblock-tag,
  [data-md-color-scheme="slate"] .swagger-ui .opblock .opblock-summary-description,
  [data-md-color-scheme="slate"] .swagger-ui .opblock .opblock-section-header h4,
  [data-md-color-scheme="slate"] .swagger-ui .opblock-description-wrapper p,
  [data-md-color-scheme="slate"] .swagger-ui .response-col_status,
  [data-md-color-scheme="slate"] .swagger-ui .response-col_description,
  [data-md-color-scheme="slate"] .swagger-ui .parameter__name,
  [data-md-color-scheme="slate"] .swagger-ui .parameter__type,
  [data-md-color-scheme="slate"] .swagger-ui .model-title,
  [data-md-color-scheme="slate"] .swagger-ui section.models h4 {
    color: #fff;
  }
  [data-md-color-scheme="slate"] .swagger-ui .opblock .opblock-section-header {
    background: rgba(255,255,255,0.05);
  }
  [data-md-color-scheme="slate"] .swagger-ui section.models {
    border-color: rgba(255,255,255,0.1);
  }
  [data-md-color-scheme="slate"] .swagger-ui section.models.is-open h4 {
    border-color: rgba(255,255,255,0.1);
  }
</style>

<script>
  window.onload = function() {
    SwaggerUIBundle({
      url: "../openapi.yaml",
      dom_id: '#swagger-ui',
      deepLinking: true,
      presets: [
        SwaggerUIBundle.presets.apis,
        SwaggerUIBundle.SwaggerUIStandalonePreset
      ],
      layout: "BaseLayout",
      defaultModelsExpandDepth: 1,
      defaultModelExpandDepth: 1,
      docExpansion: "list",
      filter: true,
      showExtensions: true,
      showCommonExtensions: true
    });
  };
</script>

## Download

- [openapi.yaml](https://github.com/barrel-db/barrel_docdb/blob/main/openapi.yaml) - OpenAPI 3.0 specification

## Using the Specification

### Generate Client SDKs

Use [OpenAPI Generator](https://openapi-generator.tech/) to generate client libraries:

```bash
# Python client
openapi-generator generate -i openapi.yaml -g python -o ./python-client

# JavaScript/TypeScript client
openapi-generator generate -i openapi.yaml -g typescript-fetch -o ./ts-client

# Go client
openapi-generator generate -i openapi.yaml -g go -o ./go-client
```

### Import into Tools

The specification can be imported into:

- **Postman** - Import > File > openapi.yaml
- **Insomnia** - Import/Export > Import Data > From File
- **Bruno** - Import Collection > OpenAPI
- **curl** - Use with tools like [openapi2curl](https://github.com/openapi-generator/openapi-generator)
