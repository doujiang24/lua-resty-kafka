local producer = require "resty.kafka.producer"

local broker_list_plain = BROKER_LIST
local key = KEY
local message = MESSAGE

describe("Test producers: ", function()

  before_each(function()
      create_topics()
  end)

  it("only allows ProducerAPI version <=2", function()
    local p, err = producer:new(broker_list_plain, {api_version=3})
    assert.is_nil(p)
    assert.is_not_nil(err)
    assert.is.same(err, "The highest supported Producer API version is 2")
  end)

  it("sends two messages and the offset is one apart", function()
    local p, err = producer:new(broker_list_plain)
    assert.is_nil(err)
    local offset1, err = p:send(TEST_TOPIC, key, message)
    assert.is_nil(err)
    local offset2, err = p:send(TEST_TOPIC, key, message)
    assert.is_nil(err)
    local diff = tonumber(offset2) - tonumber(offset1)
    assert.is.equal(diff, 1)
  end)

  it("sends two messages to two different topics", function()
    local p, err = producer:new(broker_list_plain)
    assert.is_nil(err)
    local offset1, err = p:send(TEST_TOPIC, key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset1))
    local offset2, err = p:send(TEST_TOPIC_1, key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset2))
  end)

  it("fails when topic_partitions are empty", function()
    local p, err = producer:new(broker_list_plain)
    p.client.topic_partitions.test = { [2] = { id = 2, leader = 0 }, [1] = { id = 1, leader = 0 }, [0] = { id = 0, leader = 0 }, num = 3 }
    local offset, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(err)
    assert.is_nil(offset)
    assert.is_same("not found broker", err)
  end)

  it("sends a lot of messages", function()
    local producer_config = { producer_type = "async", flush_time = 100}
    local p, err = producer:new(broker_list_plain, producer_config)
    assert.is_nil(err)
    -- init offset
    p:send(TEST_TOPIC, key, message)
    p:flush()
    local offset,_ = p:offset()
    local i = 0
    while i < 2000 do
          p:send(TEST_TOPIC, key, message..tostring(i))
          i = i + 1
    end
    ngx.sleep(0.2)
    local offset2, _ = p:offset()
    local diff = tostring(offset2 - offset)
    assert.is.equal(diff, "2000LL")
  end)

  it("test message buffering", function()
    local p = producer:new(broker_list_plain, { producer_type = "async", flush_time = 1000 })
    ngx.sleep(0.1) -- will have an immediately flush by timer_flush
    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    ngx.sleep(1.1)
    local offset = p:offset()
    assert.is_true(tonumber(offset) > 0)
    p:flush()
    local offset0 = p:offset()

    local ok, err = p:send(TEST_TOPIC, key, message)
    assert.is_nil(err)
    assert.is_not_nil(ok)

    p:flush()
    local offset1 = p:offset()

    assert.is.equal(tonumber(offset1 - offset0), 1)
  end)

  it("timer flush", function()
    local p = producer:new(broker_list_plain, { producer_type = "async", flush_time = 1000 })
    ngx.sleep(0.1) -- will have an immediately flush by timer_flush

    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    ngx.sleep(1.1)
    local offset = p:offset()
    assert.is_true(tonumber(offset) > 0)
  end)

  it("multi topic batch send", function()
    local p = producer:new(broker_list_plain, { producer_type = "async", flush_time = 10000})
    ngx.sleep(0.01)
    -- 2 message
    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    local size, err = p:send(TEST_TOPIC_1, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    p:flush()
    local offset0 = p:offset()

    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    local size, err = p:send(TEST_TOPIC_1, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    p:flush()

    local offset1 = p:offset()

    local offset_diff = tonumber(offset1 - offset0)
    assert.is.equal(offset_diff, 2)
  end)

  it("is not retryable ", function() 
    local p = producer:new(broker_list_plain, { producer_type = "async", flush_time = 10000})
    ngx.sleep(0.01)
    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    p:flush()
    local offset0 = p:offset()

    p.sendbuffer.topics.test[0].retryable = false

    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    p:flush()

    local offset1 = p:offset()
    local offset_diff = tonumber(offset1 - offset0)

    assert.is.equal(offset_diff, 1)
  end)

  it("sends in batches to two topics", function()
    local p = producer:new(broker_list_plain, { producer_type = "async", flush_time = 10000})
    ngx.sleep(0.01)
    -- 2 message
    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    p:flush()
    local offset0 = p:offset()
    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    p:flush()

    local offset1 = p:offset()
    local offset_diff = tonumber(offset1 - offset0)
    assert.is.equal(offset_diff, 2)
  end)

  it("buffer flush", function()
    local p = producer:new(broker_list_plain, { producer_type = "async", batch_num = 1, flush_time = 10000})
    ngx.sleep(0.1) -- will have an immediately flush by timer_flush

    local ok, err = p:send(TEST_TOPIC, nil, message)
    assert.is_not_nil(ok)
    assert.is_nil(err)
    ngx.sleep(1)
    local offset0 = p:offset()
    p:flush()
    local offset1 = p:offset()
    local offset_diff = tonumber(offset1) - tonumber(offset0)
    assert.is.equal(offset_diff, 0)
  end)



end)
