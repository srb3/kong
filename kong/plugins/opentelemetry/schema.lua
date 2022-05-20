local typedefs = require "kong.db.schema.typedefs"

return {
  name = "opentelemetry",
  fields = {
    { protocols = typedefs.protocols_http }, -- TODO: support stream mode
    { config = {
      type = "record",
      fields = {
        { http_endpoint = typedefs.url{ required = true } }, -- OTLP/HTTP /v1/traces
        { http_headers = typedefs.headers }, -- Extra HTTP headers
        { batch_span_count = { type = "integer", required = true, default = 200 } },
        { batch_flush_delay = { type = "integer", required = true, default = 3 } },
      },
    }, },
  },
}
