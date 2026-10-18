-- Appends entries for one (producer, epoch, seq), applying dedupe, fencing, seq order and close,
-- and refreshes the stream's time-to-live. Every append and close goes through this script; an
-- entry written to the log directly bypasses all of these checks. The Go Redis test backend runs a
-- copy of this file, which must stay identical.
--
-- Epochs, seqs and workflow task IDs are Lua numbers (doubles), and cjson stores them with 14
-- significant digits, so values with more than 14 digits, such as a client epoch taken from a
-- nanosecond clock, are compared wrongly.
--
-- KEYS: meta, log, producers, tombstone, current (the run of the owner ID that most recently
-- created a stream).
-- ARGV: owner's run ID (a workflow's first run ID, or the activity's run ID), producer, producer's
-- workflow run ID, workflow task, workflow task attempt, epoch, seq, digest, closing ("1" or "0"),
-- retention in milliseconds, entry count, then for each entry a field count followed by that many
-- field/value pairs.
local meta, log, producers, tombstone, current = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local first_run, producer, run = ARGV[1], ARGV[2], ARGV[3]
local wt, wta, epoch, seq = tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6]), tonumber(ARGV[7])
local digest, closing, retention = ARGV[8], ARGV[9] == '1', tonumber(ARGV[10])

-- A removed stream stays removed while its tombstone lives, rather than starting again empty under
-- the same keys.
local exists = redis.call('EXISTS', meta) == 1
if not exists and redis.call('EXISTS', tombstone) == 1 then
  return redis.error_reply('EXPIRED stream has been removed')
end

local function compare(st)
  if wt ~= st.wt then return wt < st.wt and -1 or 1 end
  if wta ~= st.wta then return wta < st.wta and -1 or 1 end
  if epoch ~= st.epoch then return epoch < st.epoch and -1 or 1 end
  return 0
end

-- Only the producer's latest (epoch, seq) is kept: a lower epoch is always fenced, so it needs no
-- state, and only the latest seq is deduplicated.
local next_seq = 0
local raw = redis.call('HGET', producers, producer)
if raw then
  local st = cjson.decode(raw)
  local c = compare(st)
  if c < 0 then
    return redis.error_reply('EPOCH_FENCED producer has appended at a higher epoch')
  end
  if c == 0 then
    if seq == st.seq then
      if digest == st.digest then
        return st.ids
      end
      return redis.error_reply('SEQ_DIVERGENT seq is stored with different content')
    end
    if seq < st.seq then
      return redis.error_reply('SEQ_STALE seq is lower than the latest')
    end
    next_seq = st.seq + 1
  end
end
if seq ~= next_seq then
  return redis.error_reply('SEQ_GAP seq is not the next seq')
end
-- After the dedupe check, so that a retry of a stored append, the close included, succeeds after
-- close.
if redis.call('HGET', meta, 'closed') == '1' then
  return redis.error_reply('CLOSED stream is closed')
end

local ids = {}
local i = 12
for _ = 1, tonumber(ARGV[11]) do
  local nfields = tonumber(ARGV[i])
  i = i + 1
  local args = {'XADD', log, '*', 'producer', producer, 'run', run, 'wt', wt, 'wta', wta,
    'epoch', epoch, 'seq', seq}
  for j = 0, 2 * nfields - 1 do
    args[#args + 1] = ARGV[i + j]
    -- A close's fields are copied into meta, so a reader finds the close without scanning the log.
    if closing and j % 2 == 0 then
      redis.call('HSET', meta, 'close_' .. ARGV[i + j], ARGV[i + j + 1])
    end
  end
  i = i + 2 * nfields
  ids[#ids + 1] = redis.call(unpack(args))
end
redis.call('HSET', producers, producer,
  cjson.encode({wt = wt, wta = wta, epoch = epoch, seq = seq, digest = digest, ids = ids}))
redis.call('HSET', meta, 'exists', '1')
if closing then
  redis.call('HSET', meta, 'closed', '1')
end
-- Set only when this append creates the stream, so a newer run that has not appended yet is not
-- the one readers without a run ID read.
if not exists then
  redis.call('SET', current, first_run)
end
for _, key in ipairs({meta, log, producers}) do
  redis.call('PEXPIRE', key, retention)
end
redis.call('SET', tombstone, '1', 'PX', 2 * retention)
redis.call('PEXPIRE', current, 2 * retention)
return ids
