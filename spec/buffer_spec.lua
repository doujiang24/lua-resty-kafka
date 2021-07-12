local ringbuffer = require "resty.kafka.ringbuffer"
local sendbuffer = require "resty.kafka.sendbuffer"

describe("Test buffers: ", function()
  it("overflow sendbuffer", function()
    local buffer = sendbuffer:new(2, 20)

    local topic = "test"
    local partition_id = 1
    local key = "key"
    local message = "halo world"

    local overflow = buffer:add(topic, partition_id, key, message)
    assert.is_nil(overflow)

    local overflow = buffer:add(topic, partition_id, key, message)
    assert.is_true(overflow)
  end)

  it("offset?", function()
    local buffer = sendbuffer:new(2, 20)

    local topic = "test"
    local partition_id = 1
    local key = "key"
    local message = "halo world"

    local overflow = buffer:add(topic, partition_id, key, message)
    assert.is_nil(overflow)

    local offset = buffer:offset(topic, partition_id)
    assert.is.equal(0, offset)
    local offset = buffer:offset(topic, partition_id, 100)
    assert.is_nil(offset)
    local offset = buffer:offset(topic, partition_id)
    assert.is.equal(101, offset)
  end)

  it("verify if buffer clear works", function()
    local buffer = sendbuffer:new(2, 20)

    local topic = "test"
    local partition_id = 1
    local key = "key"
    local message = "halo world"

    local overflow = buffer:add(topic, partition_id, key, message)
    assert.is_nil(overflow)
    assert.is.equal(buffer.topics[topic][partition_id].used, 0)

    -- 1 item in the queue
    assert.is.equal(buffer.queue_num, 1)

    -- clearing buffer
    buffer:clear(topic, partition_id)

    assert.is_true(buffer:done())

    assert.is.equal(buffer.queue_num, 0)

    for i = 1, 10000 do
        buffer:clear(topic, partition_id)
    end

    assert.is.equal(buffer.topics[topic][partition_id].used, 1)
  end)

  it("test buffer:loop", function()
    local buffer = sendbuffer:new(2, 20)

    local topic = "test"
    local topic_2 = "test2"
    local partition_id = 1
    local key = "key"
    local message = "halo world"

    local overflow = buffer:add(topic, partition_id, key, message)
    local overflow = buffer:add(topic_2, partition_id, key, message)

    local res = {}
    for t, p in buffer:loop() do
      res[t] = p
    end
    assert.is.same(res, { test = 1, test2 = 1 })
  end)


  it("ringbuffers TODO", function()
    local buffer = ringbuffer:new(2, 3)

    local topic = "test"
    local key = "key"
    local message = "halo world"

    local ok, err = buffer:add(topic, key, message)
    assert.is_nil(err)
    assert.is_true(ok)
    assert.is_false(buffer:need_send())

    local ok, err = buffer:add(topic, key, message)
    assert.is_nil(err)
    assert.is_true(ok)
    assert.is_true(buffer:need_send())

    -- overflowing buffer
    buffer:add(topic, key, message)
    local ok, err = buffer:add(topic, key, message)
    assert.is_nil(ok)
    assert.not_nil(err)
    assert.is.equal(err, "buffer overflow")
    assert.is_true(buffer:need_send())
  end)

  it("pop buffer", function()
    local buffer = ringbuffer:new(2, 3)

    local base_key_buf_1 = "key"
    local base_message_buf_1 = "message"
    local base_key_buf_2 = "key"
    local base_message_buf_2 = "message"
    for i = 1, 2 do
      local key1 = base_key_buf_1 .. i
      local msg1 = base_message_buf_1 .. i
      local key2 = base_key_buf_2 .. i
      local msg2 = base_message_buf_2 .. i

      buffer:add(TEST_TOPIC, key1, msg1)
      buffer:add(TEST_TOPIC_1, key2, msg2)
      local topic_out, key_out, msg_out = buffer:pop()
      assert.is.equal(topic_out, TEST_TOPIC)
      assert.is.equal(key_out, key1)
      assert.is.equal(msg_out, msg1)
      local topic_out_2, key_out_2, msg_out_2 = buffer:pop()
      assert.is.equal(topic_out_2, TEST_TOPIC_1)
      assert.is.equal(key_out_2, key2)
      assert.is.equal(msg_out_2, msg2)
    end
  end)


end)