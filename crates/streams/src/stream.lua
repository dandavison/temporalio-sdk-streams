-- The operations on a stream in Redis: create, append, close, close_if_idle and end_owner, named by
-- ARGV[1]. Every create, append and close goes through this script, which checks and writes in one
-- step; an entry written to the log directly bypasses all of its checks. The Go Redis test backend
-- runs a copy of this file, which must stay identical.
--
-- A stream's keys are meta (existence, settings and close), log, producers and tombstone. A stream
-- of an owner also has its owner run's keys, streams (the names of the run's streams) and ended (set
-- once the owner has ended), and its owner's key, current (the run of the owner ID whose first
-- stream was created most recently). A standalone stream has no owner run: its calls pass those keys,
-- and the script does not use them.
--
-- Workflow tasks, workflow task attempts, epochs and seqs must be integers from 0 to 2^53-1, which
-- Lua numbers (doubles) hold exactly; anything else is rejected. They are compared as numbers, and
-- stored as the decimal strings received, because cjson encodes numbers with 14 significant digits.
--
-- A standalone stream closes, as timed out, once it has had no create or append for its idle
-- timeout. Nothing in Redis runs at that time, so the close is written by the next create, append,
-- close or close_if_idle call; readers call close_if_idle before they report whether the stream is
-- closed.
--
-- create: KEYS meta, log, producers, tombstone, current, streams, ended. ARGV op, owner run ID (a
-- workflow's first run ID, or the activity's run ID; empty for a standalone stream), name,
-- standalone ("1" or "0"), retention in milliseconds, idle timeout in milliseconds (0 for a stream of
-- an owner), request ID. Returns 1 if this create, or an earlier one with the same request ID,
-- created the stream, and 0 if the stream existed with the same settings.
--
-- append: KEYS and ARGV[1..6] as for create, then producer, producer's workflow run ID, workflow
-- task, workflow task attempt, epoch, seq, digest, closing ("1" or "0"), entry count, then for each
-- entry a field count followed by that many field/value pairs. Returns the entry IDs.
--
-- close: closes a standalone stream. KEYS meta, tombstone. ARGV op, retention in milliseconds,
-- state, failure, reason. A close of a closed stream succeeds if it is the same close.
--
-- close_if_idle: KEYS meta. ARGV op. Returns 1 if the stream is closed, closing it first if it has
-- reached its idle timeout.
--
-- end_owner: closes every stream of an owner run that is not closed, and makes later creates of its
-- streams fail as closed. KEYS ended, streams. ARGV op, the prefix of the run's stream keys,
-- retention in milliseconds, state, failure, reason. It reads and writes the streams' meta keys,
-- which it derives from the prefix and the names in streams, so it does not declare them; they share
-- the hash tag of its keys.
local op = ARGV[1]
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

-- A standalone stream that has reached its idle timeout is closed now. Returns whether the stream is
-- closed.
local function closed_or_idle(meta)
  if redis.call('HGET', meta, 'closed') == '1' then
    return true
  end
  local idle = tonumber(redis.call('HGET', meta, 'idle_timeout_ms') or '0')
  if idle == 0 or now - tonumber(redis.call('HGET', meta, 'last_write_ms')) < idle then
    return false
  end
  redis.call('HSET', meta, 'closed', '1', 'close_state', '5', 'close_failure', '', 'close_reason', 'idle timeout')
  return true
end

local function close_fields(meta, state, failure, reason)
  redis.call('HSET', meta, 'closed', '1', 'close_state', state, 'close_failure', failure, 'close_reason', reason)
end

if op == 'close_if_idle' then
  if redis.call('EXISTS', KEYS[1]) == 0 then
    return 0
  end
  return closed_or_idle(KEYS[1]) and 1 or 0
end

if op == 'close' then
  local meta, tombstone = KEYS[1], KEYS[2]
  local retention, state, failure, reason = tonumber(ARGV[2]), ARGV[3], ARGV[4], ARGV[5]
  if redis.call('EXISTS', meta) == 0 then
    if redis.call('EXISTS', tombstone) == 1 then
      return redis.error_reply('EXPIRED stream has been removed')
    end
    return redis.error_reply('NOT_FOUND stream does not exist')
  end
  if closed_or_idle(meta) then
    local c = redis.call('HMGET', meta, 'close_state', 'close_failure', 'close_reason')
    if c[1] == state and (c[2] or '') == failure and (c[3] or '') == reason then
      return {}
    end
    return redis.error_reply('CLOSED stream is closed')
  end
  close_fields(meta, state, failure, reason)
  redis.call('PEXPIRE', meta, retention)
  redis.call('SET', tombstone, '1', 'PX', 2 * retention)
  return {}
end

if op == 'end_owner' then
  local ended, streams = KEYS[1], KEYS[2]
  local prefix, retention, state, failure, reason = ARGV[2], tonumber(ARGV[3]), ARGV[4], ARGV[5], ARGV[6]
  if redis.call('EXISTS', ended) == 1 then
    return 0
  end
  redis.call('HSET', ended, 'state', state)
  for _, name in ipairs(redis.call('SMEMBERS', streams)) do
    local meta = prefix .. name .. ':meta'
    if redis.call('EXISTS', meta) == 1 and redis.call('HGET', meta, 'closed') ~= '1' then
      close_fields(meta, state, failure, reason)
    end
  end
  redis.call('PEXPIRE', ended, 2 * retention)
  redis.call('PEXPIRE', streams, 2 * retention)
  return 1
end

local meta, log, producers, tombstone = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local current, streams, ended = KEYS[5], KEYS[6], KEYS[7]
local owner_run, name, standalone = ARGV[2], ARGV[3], ARGV[4] == '1'
local retention, idle = tonumber(ARGV[5]), ARGV[6]

-- A removed stream stays removed while its tombstone lives, rather than starting again empty under
-- the same keys.
local exists = redis.call('EXISTS', meta) == 1
if not exists and redis.call('EXISTS', tombstone) == 1 then
  return redis.error_reply('EXPIRED stream has been removed')
end

local function create(request_id)
  if not standalone and redis.call('EXISTS', ended) == 1 then
    return redis.error_reply("CLOSED the stream's owner has ended")
  end
  redis.call('HSET', meta, 'exists', '1', 'request_id', request_id, 'idle_timeout_ms', idle)
  -- current is set only when the run's first stream is created, so a newer run that has created no
  -- stream yet is not the one readers without a run ID read.
  if not standalone and redis.call('SADD', streams, name) == 1 and redis.call('SCARD', streams) == 1 then
    redis.call('SET', current, owner_run)
  end
end

local function written()
  redis.call('HSET', meta, 'last_write_ms', now)
  for _, key in ipairs({meta, log, producers}) do
    redis.call('PEXPIRE', key, retention)
  end
  redis.call('SET', tombstone, '1', 'PX', 2 * retention)
  if not standalone then
    for _, key in ipairs({current, streams, ended}) do
      redis.call('PEXPIRE', key, 2 * retention)
    end
  end
end

if op == 'create' then
  local request_id = ARGV[7]
  local created = 0
  if exists then
    if request_id ~= '' and redis.call('HGET', meta, 'request_id') == request_id then
      created = 1
    elseif closed_or_idle(meta) then
      return redis.error_reply('CLOSED stream is closed')
    elseif (redis.call('HGET', meta, 'idle_timeout_ms') or '0') ~= idle then
      return redis.error_reply('SETTINGS_MISMATCH stream exists with other settings')
    end
  else
    local err = create(request_id)
    if err then
      return err
    end
    created = 1
  end
  written()
  return created
end

if op ~= 'append' then
  return redis.error_reply('INVALID_ARGUMENT unknown operation ' .. op)
end

local producer, run = ARGV[7], ARGV[8]
local wt, wta, epoch, seq = tonumber(ARGV[9]), tonumber(ARGV[10]), tonumber(ARGV[11]), tonumber(ARGV[12])
local digest, closing = ARGV[13], ARGV[14] == '1'

for _, v in ipairs({wt, wta, epoch, seq}) do
  if not v or v < 0 or v > 9007199254740991 or v ~= math.floor(v) then
    return redis.error_reply('INVALID_ARGUMENT workflow tasks, epochs and seqs must be integers from 0 to 2^53-1')
  end
end

local function compare(st)
  local st_wt, st_wta, st_epoch = tonumber(st.wt), tonumber(st.wta), tonumber(st.epoch)
  if wt ~= st_wt then return wt < st_wt and -1 or 1 end
  if wta ~= st_wta then return wta < st_wta and -1 or 1 end
  if epoch ~= st_epoch then return epoch < st_epoch and -1 or 1 end
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
    local st_seq = tonumber(st.seq)
    if seq == st_seq then
      if digest == st.digest then
        return st.ids
      end
      return redis.error_reply('SEQ_DIVERGENT seq is stored with different content')
    end
    if seq < st_seq then
      return redis.error_reply('SEQ_STALE seq is lower than the latest')
    end
    next_seq = st_seq + 1
  end
end
if seq ~= next_seq then
  return redis.error_reply('SEQ_GAP seq is not the next seq')
end
-- After the dedupe check, so that a retry of a stored append, the close included, succeeds after
-- close.
if exists then
  if closed_or_idle(meta) then
    return redis.error_reply('CLOSED stream is closed')
  end
else
  local err = create('')
  if err then
    return err
  end
end

local ids = {}
local i = 16
for _ = 1, tonumber(ARGV[15]) do
  local nfields = tonumber(ARGV[i])
  i = i + 1
  local args = {'XADD', log, '*', 'producer', producer, 'run', run, 'wt', ARGV[9], 'wta', ARGV[10],
    'epoch', ARGV[11], 'seq', ARGV[12]}
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
  cjson.encode({wt = ARGV[9], wta = ARGV[10], epoch = ARGV[11], seq = ARGV[12], digest = digest, ids = ids}))
if closing then
  redis.call('HSET', meta, 'closed', '1')
end
written()
return ids
