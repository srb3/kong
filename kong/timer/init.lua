local semaphore = require("ngx.semaphore")
local job_module = require("kong.timer.job")
local utils = require("kong.timer.utils")
local wheel_group = require("kong.timer.wheel.group")
local constants = require("kong.timer.constants")

-- TODO: use it to readuce overhead
-- local new_tab = require "table.new"

local ngx = ngx

local math_max = math.max
local math_min = math.min
local math_random = math.random
local math_modf = math.modf
local math_huge = math.huge
local math_abs = math.abs

local string_format = string.format

-- luacheck: push ignore
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_DEBUG = ngx.DEBUG
local ngx_NOTICE = ngx.NOTICE
-- luacheck: pop

local ngx_timer_at = ngx.timer.at
local ngx_timer_every = ngx.timer.every
local ngx_sleep = ngx.sleep
local ngx_worker_exiting = ngx.worker.exiting
local ngx_now = ngx.now
local ngx_update_time = ngx.update_time

local pairs = pairs
local ipairs = ipairs
local tostring = tostring
local type = type
local next = next

local assert = utils.assert

local _M = {}


local function log_notice(...)
    ngx_log(ngx_NOTICE, "[timer] ", ...)
end


local function log_error(...)
    ngx_log(ngx_ERR, "[timer] ", ...)
end


local function native_timer_at(delay, callback, ...)
    local ok, err = ngx_timer_at(delay, callback, ...)
    assert(ok,
        constants.MSG_FATAL_FAILED_CREATE_NATIVE_TIMER
        -- `err` maybe `nil`
        .. tostring(err))
end


-- post some resources until `self.semaphore_super:count() == 1`
-- TODO: rename the first argument ?
local function wake_up_super_timer(self)
    local semaphore_super = self.semaphore_super

    local count = semaphore_super:count()

    if count <= 0 then
        semaphore_super:post(math_abs(count) + 1)
    end
end


-- post some resources until `self.semaphore_mover:count() == 1`
-- TODO: rename the first argument ?
local function wake_up_mover_timer(self)
    local semaphore_mover = self.semaphore_mover

    local count = semaphore_mover:count()

    if count <= 0 then
        semaphore_mover:post(math_abs(count) + 1)
    end
end


-- move all jobs from `self.wheels.ready_jobs` to `self.wheels.pending_jobs`
-- wake up the worker timer
local function mover_timer_callback(premature, self)
    log_notice("mover timer has been started")

    local semaphore_worker = self.semaphore_worker
    local semaphore_mover = self.semaphore_mover
    local opt_threads = self.opt.threads
    local wheels = self.wheels

    if premature then
        log_notice("exit mover timer due to `premature`")
        return
    end

    while not ngx_worker_exiting() and not self.destory do
        log_notice("waiting on `semaphore_mover` for 1 second")

        local ok, err = semaphore_mover:wait(1)

        if not ok and err ~= "timeout" then
            log_error("failed to wait on `semaphore_mover`: " .. err)
        end

        local is_no_pending_jobs =
            utils.table_is_empty(wheels.pending_jobs)

        local is_no_ready_jobs =
            utils.table_is_empty(wheels.ready_jobs)

        if not is_no_pending_jobs then
            semaphore_worker:post(opt_threads)
            goto continue
        end

        if not is_no_ready_jobs then
            local temp = wheels.pending_jobs
            wheels.pending_jobs = wheels.ready_jobs
            wheels.ready_jobs = temp

            semaphore_worker:post(opt_threads)
        end

        ::continue::
    end

    log_notice("exit mover timer")
end


-- exec all expired jobs
-- re-insert the recurrent job
-- delete once job from `self.jobs`
-- wake up the super timer
local function worker_timer_callback(premature, self, thread_index)
    log_notice("thread #", thread_index, " has been started")

    if premature then
        log_notice("exit thread #", thread_index, " due to `premature`")
        return
    end

    local semaphore_worker = self.semaphore_worker
    local thread = self.threads[thread_index]
    local wheels = self.wheels
    local jobs = self.jobs

    while not ngx_worker_exiting() and not self.destory do
        log_notice("waiting on `semaphore_worker` for 1 second in thread #"
            .. thread_index)
        local ok, err = semaphore_worker:wait(1)

        if not ok and err ~= "timeout" then
            log_error(string_format(
                "failed to wait on `semaphore_worker` in thread #%d: %s",
                thread_index, err))
        end

        while not utils.table_is_empty(wheels.pending_jobs) do
            thread.counter.runs = thread.counter.runs + 1

            log_notice(string_format(
                "thread #%d was run %d times",
                thread_index, thread.counter.runs
            ))

            local _, job = next(wheels.pending_jobs)

            wheels.pending_jobs[job.name] = nil

            log_notice(string_format(
                "timer %s is expected to be executed by thread #%d",
                job.name, thread_index
            ))

            if not job:is_runnable() then
                log_notice(string_format(
                    "timer %s is not runnable",
                    job.name
                ))

                goto continue
            end

            log_notice(string_format(
                "execute timer %s in thread #",
                job.name, thread_index
            ))
            job:execute()

            if job:is_oneshot() then
                log_notice(string_format(
                    "timer %s need to be executed only once",
                    job.name
                ))
                jobs[job.name] = nil
                goto continue
            end

            if job:is_runnable() then
                log_notice(string_format(
                    "reschedule timer %s in thread #%d",
                    job.name, thread_index
                ))
                wheels:sync_time()
                job:re_cal_next_pointer(wheels)
                wheels:insert_job(job)
                wake_up_super_timer(self)
            end

            ::continue::
        end

        if not utils.table_is_empty(wheels.ready_jobs) then
            wake_up_mover_timer(self)
        end

        if thread.counter.runs > self.opt.restart_thread_after_runs then
            thread.counter.runs = 0

            -- Since the native timer only releases resources
            -- when it is destroyed,
            -- including resources created by `job:execute()`
            -- it needs to be destroyed and recreated periodically.
            log_notice(string_format(
                "re-create thread #%d",
                thread_index
            ))
            native_timer_at(0, worker_timer_callback, self, thread_index)
            break
        end

    end -- the top while

    log_notice(string_format(
        "exit thread #%d",
        thread_index
    ))
end


-- do the following things
-- * create all worker timer
-- * wake up mover timer
-- * update the status of all wheels
-- * calculate wait time for `semaphore_super`
local function super_timer_callback(premature, self)
    log_notice("super timer has been started")

    if premature then
        log_notice("exit super timer due to `premature`")
        return
    end

    local semaphore_super = self.semaphore_super
    local threads = self.threads
    local opt_threads = self.opt.threads
    local opt_resolution = self.opt.resolution
    local wheels = self.wheels

    self.super_timer = true

    for i = 1, opt_threads do
        if not threads[i].alive then
            log_notice(string_format(
                "creating thread #%d of %d",
                i, opt_threads
            ))
            native_timer_at(0, worker_timer_callback, self, i)
        end
    end

    ngx_sleep(opt_resolution)

    ngx_update_time()
    wheels.real_time = ngx_now()
    wheels.expected_time = wheels.real_time - opt_resolution

    while not ngx_worker_exiting() and not self.destory do
        if self.enable then
            wheels:sync_time()

            if not utils.table_is_empty(wheels.ready_jobs) then
                wake_up_mover_timer(self)
            end

            wheels:update_closest()
            local closest = math_max(wheels.closest, 0.1)
            wheels.closest = math_huge

            log_notice(string_format(
                "waiting on `semaphore_super` for %f second",
                closest))

            local ok, err = semaphore_super:wait(closest)

            if not ok and err ~= "timeout" then
                log_error("failed to wait on `semaphore_super`: " .. err)
            end

        else
            ngx_sleep(0.1)
        end
    end
end


local function create(self ,name, callback, delay, once, args)
    local wheels = self.wheels
    local jobs = self.jobs
    if not name then
        name = tostring(math_random())
    end

    if jobs[name] then
        return false, "already exists timer"
    end

    wheels:sync_time()

    local job = job_module.new(wheels, name, callback, delay, once, args)
    job:enable()
    jobs[name] = job

    log_notice("trying to create a new timer: " .. tostring(job))

    if job:is_immediate() then
        wheels.ready_jobs[name] = job
        wake_up_mover_timer(self)

        return true, nil
    end

    local ok, err = wheels:insert_job(job)

    wake_up_super_timer(self)

    if ok then
        return name, nil
    end

    return false, err
end


function _M.configure(timer_sys, options)
    assert(type(timer_sys) == "table")

    if timer_sys.configured then
        return false, "already configured"
    end

    if options then
        assert(type(options) == "table", "expected `options` to be a table")

        if options.restart_thread_after_runs then
            assert(type(options.restart_thread_after_runs) == "number",
                "expected `restart_thread_after_runs` to be a number")

            assert(options.restart_thread_after_runs > 0,
                "expected `restart_thread_after_runs` to be greater than 0")

            local _, tmp = math_modf(options.restart_thread_after_runs)

            assert(tmp == 0,
                "expected `restart_thread_after_runs` to be an integer")
        end

        if options.threads then
            assert(type(options.threads) == "number",
                "expected `threads` to be a number")

            assert(options.threads > 0,
            "expected `threads` to be greater than 0")

            local _, tmp = math_modf(options.threads)
            assert(tmp == 0, "expected `threads` to be an integer")
        end

        if options.resolution then
            assert(type(options.resolution) == "number",
                "expected `resolution` to be a number")

            assert(utils.float_compare(options.resolution, 0.1) >= 0,
            "expected `resolution` to be greater than or equal to 0.1")
        end

        if options.wheel_setting then
            local wheel_setting = options.wheel_setting
            local level = wheel_setting.level

            assert(type(wheel_setting) == "table",
                "expected `wheel_setting` to be a number")

            assert(type(wheel_setting.level) == "number",
                "expected `wheel_setting.level` to be a number")

            assert(type(wheel_setting.slots) == "table",
                "expected `wheel_setting.slots` to be a table")

            local slots_length = #wheel_setting.slots

            assert(level == slots_length,
                "expected `wheel_setting.level`"
             .. " is equal to "
             .. "the length of `wheel_setting.slots`")

            for i, v in ipairs(wheel_setting.slots) do
                assert(type(v) == "number",string_format(
                    "expected `wheel_setting.slots[%d]` to be a number", i))

                assert(v >= 1, string_format(
                    "expected `wheel_setting.slots[%d]`"
                 .. "to be greater than 1", i))

                local _, tmp = math_modf(v)

                assert(tmp == 0, string_format(
                    "expected `wheel_setting.slots[%d]` to be an integer", i))
            end

        end
    end

    local opt = {
        wheel_setting = options
            and options.wheel_setting
            or constants.DEFAULT_WHEEL_SETTING,

        resolution = options
            and options.resolution
            or  constants.DEFAULT_RESOLUTION,

        -- restart the thread after every n jobs have been run
        restart_thread_after_runs = options
            and options.restart_thread_after_runsor
            or constants.DEFAULT_RESTART_THREAD_AFTER_RUNS,

        -- number of timer will be created by OpenResty API
        threads = options
            and options.threads
            or constants.DEFAULT_THREADS,

        -- call function `ngx.update_time` every run of timer job
        force_update_time = options
            and options.force_update_time
            or constants.DEFAULT_FORCE_UPDATE_TIME,
    }

    timer_sys.opt = opt

    timer_sys.max_expire = opt.resolution
    for _, v in ipairs(opt.wheel_setting.slots) do
        timer_sys.max_expire = timer_sys.max_expire * v
    end
    timer_sys.max_expire = timer_sys.max_expire - 2

    -- enable/diable entire timing system
    timer_sys.enable = false

    timer_sys.threads = utils.table_new(opt.threads, 0)

    timer_sys.jobs = {}

    -- has the super timer already been created?
    timer_sys.super_timer = false

    -- has the mover timer already been created?
    timer_sys.mover_timer = false

    timer_sys.destory = false

    timer_sys.semaphore_super = semaphore.new()

    timer_sys.semaphore_worker = semaphore.new()

    timer_sys.semaphore_mover = semaphore.new()

    timer_sys.wheels = wheel_group.new(opt.wheel_setting, opt.resolution)

    for i = 1, timer_sys.opt.threads do
        timer_sys.threads[i] = {
            index = i,
            alive = false,
            counter = {
                -- number of runs
                runs = 0,
            },
        }
    end

    timer_sys.configured = true

    timer_sys.once = _M.once
    timer_sys.every = _M.every
    timer_sys.run = _M.run
    timer_sys.pause = _M.pause
    timer_sys.cancel = _M.cancel

    return true, nil
end


function _M.start(timer_sys)
    local ok, err = true, nil

    if not timer_sys.super_timer then
        native_timer_at(0, super_timer_callback, timer_sys)
        native_timer_at(0, mover_timer_callback, timer_sys)
        timer_sys.super_timer = true
    end

    if not timer_sys.enable then
        ngx_update_time()
        timer_sys.wheels.expected_time = ngx_now()
    end

    timer_sys.enable = true

    return ok, err
end


function _M.freeze(timer_sys)
    timer_sys.enable = false
end


-- TODO: rename this method
function _M.unconfigure(timer_sys)
    timer_sys.destory = true
    timer_sys.configured = false
end



function _M:once(name, callback, delay, ...)
    assert(self.configured, "the timer module is not configured")
    assert(self.enable, "the timer module is not started")
    assert(type(callback) == "function", "expected `callback` to be a function")

    assert(type(delay) == "number", "expected `delay to be a number")
    assert(delay >= 0, "expected `delay` to be greater than or equal to 0")

    if delay >= self.max_expire
        or (delay ~= 0 and delay < self.opt.resolution)then

        log_notice("fallback to ngx.timer.every [delay = " .. delay .. "]")
        local ok, err = ngx_timer_at(delay, callback, ...)
        return ok ~= nil, err
    end

    -- TODO: desc the logic and add related tests
    local name_or_false, err =
        create(self, name, callback, delay, true, { ... })

    return name_or_false, err
end


function _M:every(name, callback, interval, ...)
    assert(self.configured, "the timer module is not configured")
    assert(self.enable, "the timer module is not started")
    assert(type(callback) == "function", "expected `callback` to be a function")

    assert(type(interval) == "number", "expected `interval to be a number")
    assert(interval > 0, "expected `interval` to be greater than or equal to 0")

    if interval >= self.max_expire
        or interval < self.opt.resolution then

        log_notice("fallback to ngx.timer.every [interval = "
            .. interval .. "]")
        local ok, err = ngx_timer_every(interval, callback, ...)
        return ok ~= nil, err
    end

    local name_or_false, err =
        create(self, name, callback, interval, false, { ... })

    return name_or_false, err
end


function _M:run(name)
    assert(type(name) == "string", "expected `name` to be a string")

    local jobs = self.jobs
    local old_job = jobs[name]
    jobs[name] = nil

    if not old_job then
        return false, "timer not found"
    end

    if old_job:is_runnable() then
        return false, "running"
    end

    local name_or_false, err =
        create(self, old_job.name, old_job.callback,
               old_job.delay, old_job:is_oneshot(), old_job.args)

    local ok = name_or_false ~= false

    jobs[name].meta = old_job:get_metadata()

    return ok, err
end


function _M:pause(name)
    assert(type(name) == "string", "expected `name` to be a string")

    local jobs = self.jobs
    local job = jobs[name]

    if not job then
        return false, "timer not found"
    end

    job:pause()

    return true, nil
end


function _M:cancel(name)
    assert(type(name) == "string", "expected `name` to be a string")

    local jobs = self.jobs
    local job = jobs[name]

    if not job then
        return false, "timer not found"
    end

    job:cancel()
    jobs[name] = nil

    return true, nil
end


function _M.stats(timer_sys)
    local pending_jobs = timer_sys.wheels.pending_jobs
    local ready_jobs = timer_sys.wheels.ready_jobs

    local sys = {
        running = 0,
        pending = 0,
        waiting = 0,
    }

    -- TODO: use `utils.table_new`
    local jobs = {}

    for name, job in pairs(timer_sys.jobs) do
        if job.running then
            sys.running = sys.running + 1

        elseif pending_jobs[name] or ready_jobs[name] then
            sys.pending = sys.pending + 1

        else
            sys.waiting = sys.waiting + 1
        end

        local stats = job.stats
        jobs[name] = {
            name = name,
            meta = job:get_metadata(),
            elapsed_time = utils.table_deepcopy(job.stats.elapsed_time),
            runs = stats.runs,
            faults = stats.runs - stats.finish,
            last_err_msg = stats.last_err_msg,
        }
    end


    return {
        sys = sys,
        timers = jobs,
    }
end


return _M