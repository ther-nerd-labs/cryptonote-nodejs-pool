local sum = 0
redis.call('SELECT', 3)
local m = redis.call('KEYS', 'ARQ:workers*')
for _,key in ipairs(m) do
    local val = redis.call('HGET', key, 'lastShare')
    sum = sum + tonumber(val)
end
return sum
