local _M = {}
local mt = { __index = _M }

function _M.new(opts)
  local ok, auth_strategy = pcall(require, "resty.kafka.auth.strategies." .. opts.strategy)
  if not ok then
    return nil, ("Strategy %s is not implemented. Error: %s"):format(opts.strategy, auth_strategy)
  end

  local auth = auth_strategy.new(opts)
  local self = {
    strategy = auth,
  }

  return setmetatable(self, mt)
end

function _M:authenticate(sock)
  return self.strategy:authenticate(sock)
end

return _M
