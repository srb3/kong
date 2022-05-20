local encode_traces = require "kong.plugins.opentelemetry.otlp".encode_traces
local transform_span = require "kong.plugins.opentelemetry.otlp".transform_span
local new_tab = require "table.new"
local http = require "resty.http"

local ngx = ngx
local kong = kong
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_DEBUG = ngx.DEBUG
local ngx_now = ngx.now
local timer_at = ngx.timer.at
local clone = table.clone
local clear = table.clear

local OpenTelemetryHandler = {
  VERSION = "0.1.0",
  -- We want to run first so that timestamps taken are at start of the phase
  -- also so that other plugins might be able to use our structures
  PRIORITY = 100000,
}

local default_headers = {
  ["Content-Type"] = "application/x-protobuf",
}

local http_headers_cache = setmetatable({}, { __mode = "k" })
local spans_cache = new_tab(5000, 0)

local function http_export_request(conf, pb_data, headers)
  local httpc = http.new()
  local res, err = httpc:request_uri(conf.http_endpoint, {
    method = "POST",
    body = pb_data,
    headers = headers,
  })
  if not res then
    ngx_log(ngx_ERR, "request failed: ", err)
  end

  if res and res.status ~= 200 then
    ngx_log(ngx_ERR, "failed to exporter traces to backend server: ", res.body)
  end
end

local function http_export(premature, conf)
  if premature then
    return
  end

  local spans = spans_cache
  if spans.n == nil or spans.n == 0 then
    return
  end

  local start = ngx_now()

  -- cache http headers
  local headers = default_headers
  if conf.http_headers then
    headers = http_headers_cache[conf.http_headers]
  end

  if not headers then
    headers = clone(default_headers)
    if conf.http_headers ~= nil then
      for k, v in pairs(conf.http_headers) do
        headers[k] = v and v[1]
      end
    end
    http_headers_cache[conf.http_headers] = headers
  end

  -- batch send spans
  local batch_spans = new_tab(conf.batch_span_count, 0)

  for i = 1, spans_cache.n do
    local len = (batch_spans.n or 0) + 1
    batch_spans[len] = spans_cache[i]

    if len >= conf.batch_span_count then
      local pb_data = encode_traces(batch_spans)
      clear(batch_spans)

      http_export_request(conf, pb_data, headers)
    end
  end

  clear(spans_cache)

  -- remain spans
  local pb_data = encode_traces(batch_spans)
  http_export_request(conf, pb_data, headers)

  ngx.update_time()
  local duration = ngx.now() - start
  ngx_log(ngx_DEBUG, "opentelemetry exporter sent " .. #spans .. " traces in " .. duration .. " seconds")
end

local function process_span(span)
  local pb_span = transform_span(span)

  local len = spans_cache.n or 0
  len = len + 1

  spans_cache[len] = pb_span
  spans_cache.n = len
end

local last_run_cache = setmetatable({}, { __mode = "k" })

function OpenTelemetryHandler:log(conf)
  ngx_log(ngx_DEBUG, "total spans in current request: ", ngx.ctx.KONG_SPANS and #ngx.ctx.KONG_SPANS)

  -- transform spans
  kong.tracing.process_span(process_span)

  local last = last_run_cache[conf] or 0
  local now = ngx_now()
  if now - last >= conf.batch_flush_delay then
    last_run_cache[conf] = now
    timer_at(0, http_export, conf)
  end

end

return OpenTelemetryHandler
