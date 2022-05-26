local pb = require "pb"
local grpc = require "kong.tools.grpc"

local grpc_new = grpc.new
local pb_encode = pb.encode

local _M = {}
_M.__index = _M
setmetatable(_M, _M)

local wrpc_proto_name = "wrpc.wrpc"

local default_proto_path = { "kong/include/", "/usr/include/" }

local function parse_annotation(annotation)
    local ret = {}
    for kv_pair in annotation:gmatch("[^;]+=[^;]+") do
        local key, value = kv_pair:match("^%s*(%S-)=(%S+)%s*$")
        ret[key] = value
    end
    return ret
end

-- TODO: better way to do this
-- +wrpc: key1=val1; key2=val2; ...
-- service-id and rpc-id to get their id
function _M:parse_annotations(proto_f)
    local service_ids = self.service_ids
    local rpc_ids = self.rpc_ids
    local annotations = self.annotations

    local service = ""
    for line in proto_f:lines() do
        local annotation = line:match("//%s*%+wrpc:%s*(.-)%s*$")
        if annotation then
            local nextline = proto_f:read("*l")
            local keyword, identifier = nextline:match("^%s*(%a+)%s+(%w+)")

            if keyword and identifier then
                local name, id_tag_name, ids
                if keyword == "service" then
                    name = identifier
                    id_tag_name = "service-id"
                    service = identifier;
                    ids = service_ids
                elseif keyword == "rpc" then
                    id_tag_name = "rpc-id"
                    name = service .. '.' .. identifier
                    ids = rpc_ids
                else
                    error("unknown type of protobuf identity")
                end

                annotations[name] = parse_annotation(annotation)
                local id = assert(annotations[name][id_tag_name],
                        keyword .. "with no id assigned")
                ids[name] =
                    assert(tonumber(id), keyword .. "'s id should be a number")
            end
        end
    end
end

function _M.new()
    local ret = setmetatable({
        grpc_instance = grpc_new(),
        service_ids = {},
        rpc_ids = {},
        annotations = {},
        name_to_mthd = {},
    }, _M)

    ret:addpath(default_proto_path)
    ret:import_proto(wrpc_proto_name)
    return ret
end

function _M:addpath(proto_path)
    self.grpc_instance:addpath(proto_path)
end

-- throw when error occurs
-- pcall if you do not want it throw
function _M:import_proto(name)
    local fname = name:gsub('%.', '/') .. '.proto'

    local fh = assert(self.grpc_instance:name_search(fname),
        "module " .. name .. " cannot be found or cannot be opened")
    self:parse_annotations(fh)
    fh:close()

    local service_ids = self.service_ids
    local rpc_ids = self.rpc_ids
    -- throwable
    self.grpc_instance:each_method(fname,
        function(_, srvc, mthd)
        self.name_to_mthd[srvc.name .. "." .. mthd.name] = mthd
        local srvc_id = assert(service_ids[srvc.name], "service " .. srvc.name .. " has no id assigned")
        local rpc_id = assert(service_ids[mthd.name], "rpc " .. mthd.name .. " has no id assigned")
        self.name_to_mthd[srvc_id .. "." .. rpc_id] = mthd
    end
    )
end

function _M:get_rpc(srvc, mthd)
    return self.name_to_mthd[srvc..mthd]
end

--- sets a service handler for the givern rpc method
--- @param rpc_name string Full name of the rpc method
--- @param handler function Function called to handle the rpc method.
--- @param response_handler function Fallback function called to handle responses.
function _M:set_handler(rpc_name, handler, response_handler)
  local rpc = self:get_method(rpc_name)
  if not rpc then
    return nil, string.format("unknown method %q", rpc_name)
  end

  rpc.handler = handler
  rpc.response_handler = response_handler
  return rpc
end


--- Part of wrpc_peer:call()
--- If calling the same method with the same args several times,
--- (to the same or different peers), this method returns the
--- invariant part, so it can be cached to reduce encoding overhead
function _M:encode_args(name, ...)
  local rpc = self:get_method(name)
  if not rpc then
    return nil, string.format("unknown method %q", name)
  end

  local num_args = select('#', ...)
  local payloads = table.new(num_args, 0)
  for i = 1, num_args do
    payloads[i] = assert(pb_encode(rpc.input_type, select(i, ...)))
  end

  return rpc, payloads
end

return _M