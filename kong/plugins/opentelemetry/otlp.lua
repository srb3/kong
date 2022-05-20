require "kong.plugins.opentelemetry.proto"
local pb = require "pb"
local new_tab = require "table.new"
local nkeys = require "table.nkeys"
local tablepool = require "tablepool"

local kong = kong
local insert = table.insert
local clone = table.clone
local tablepool_fetch = tablepool.fetch
local tablepool_release = tablepool.release

local POOL_OTLP = "KONG_OTLP"
local EMPTY_TAB = {}

local function transform_attributes(attr)
  if type(attr) ~= "table" then
    error("invalid attributes", 2)
  end

  local pb_attributes = new_tab(nkeys(attr), 0)
  for k, v in pairs(attr) do
    local typ = type(v)
    local pb_val

    if typ == "string" then
      pb_val = { string_value = v }

    elseif typ == "number" then
      pb_val = { double_value = v }

    elseif typ == "boolean" then
      pb_val = { bool_value = v }
    else
      pb_val = EMPTY_TAB -- considered empty
    end

    insert(pb_attributes, {
      key = k,
      value = pb_val,
    })
  end

  return pb_attributes
end

local function transform_events(events)
  if type(events) ~= "table" then
    return nil
  end

  local pb_events = new_tab(#events, 0)
  for _, evt in ipairs(events) do
    local pb_evt = {
      name = evt.name,
      time_unix_nano = evt.time_ns,
      -- dropped_attributes_count = 0,
    }

    if evt.attributes then
      pb_evt.attributes = transform_attributes(evt.attributes)
    end

    insert(pb_events, pb_evt)
  end

  return pb_events
end

local function transform_span(span)
  assert(type(span) == "table")

  local pb_span = {
    trace_id = span.trace_id,
    span_id = span.span_id,
    -- trace_state = "",
    parent_span_id = span.parent_span_id or "",
    name = span.name,
    kind = span.kind or 0,
    start_time_unix_nano = span.start_time_ns,
    end_time_unix_nano = span.end_time_ns,
    attributes = span.attributes and transform_attributes(span.attributes),
    -- dropped_attributes_count = 0,
    events = span.events and transform_events(span.events),
    -- dropped_events_count = 0,
    -- links = EMPTY_TAB,
    -- dropped_links_count = 0,
    status = span.status,
  }
  return pb_span
end

local function to_pb(data)
  return pb.encode("opentelemetry.proto.trace.v1.TracesData", data)
end

local encode_traces
do
  local pb_memo = {
    resource_spans = {
      {
        resource = {
          attributes = {
            { key = "service.name", value = { string_value = "kong" } },
            { key = "service.instance.id", value = { string_value = kong and kong.node.get_id() } },
            { key = "service.version", value = { string_value = kong and kong.version } },
          }
        },
        instrumentation_library_spans = {
          {
            instrumentation_library = {
              name = "opentelemetry-plugin",
              version = "0.0.1",
            },
            spans = {},
          }
        },
      }
    }
  }

  encode_traces = function(spans)
    local tab = tablepool_fetch(POOL_OTLP, 0, 2)
    if not tab.resource_spans then
      tab.resource_spans = clone(pb_memo.resource_spans)
    end

    tab.resource_spans[1].instrumentation_library_spans[1].spans = spans
    local pb_data = to_pb(tab)

    -- remove reference
    tab.resource_spans[1].instrumentation_library_spans[1].spans = nil
    tablepool_release(POOL_OTLP, tab, true) -- no clear

    return pb_data
  end
end

return {
  transform_span = transform_span,
  encode_traces = encode_traces,
}
