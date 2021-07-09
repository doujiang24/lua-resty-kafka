#!/usr/local/openresty/bin/resty

-- script to run Busted tests using Openresty while setting some extra flags.
--
-- This script should be specified as:
--   busted --lua=<this-file>
--
-- Alternatively specify it in the `.busted` config file


-- These flags are passed to `resty` by default, to allow for more connections
-- and disable the Global variable write-guard. Override it by setting the
-- environment variable `BUSTED_RESTY_FLAGS`.
local RESTY_FLAGS=os.getenv("BUSTED_RESTY_FLAGS") or "-c 4096 -e 'setmetatable(_G, nil)'"

-- rebuild the invoked commandline, while inserting extra resty-flags
local cmd = {
  "exec",
  arg[-1],
  RESTY_FLAGS,
}
for i, param in ipairs(arg) do
  table.insert(cmd, "'" .. param .. "'")
end

local _, _, rc = os.execute(table.concat(cmd, " "))
os.exit(rc)
