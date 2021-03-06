package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.sys.PrimeInstance.CheckPrimeParams;
import ted.driver.sys.Registry.Channel;
import ted.driver.sys.TedDriverImpl.TedContext;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author Augustus
 *         created on 2018.07.27
 *
 * for TED internal usage only!!!
 *
 * quick check in db for tasks. Will be called every ~1s
 * (config "ted.driver.intervalDriverMs").
 * tries to combine few selects into single call
 *
 */
class QuickCheck {
    private final static Logger logger = LoggerFactory.getLogger(QuickCheck.class);
    private final static int SKIP_CHANNEL_THRESHOLD_MS = 70;
    private final static int SKIP_CHANNEL_THRESHOLD_TASK_COUNT = 500;

    private final TedContext context;

    private final AtomicLong tickCount = new AtomicLong();
    private long nextPrimeCheckTimeMs = 0L;
    private boolean skipNextChannelLookup = false;

    QuickCheck(TedContext context) {
        this.context = context;
    }

    static class CheckResult {
        String type; // CHAN - waiting channels, PRIM - prime check results
        String name;
        Date tillTs;
        Integer taskCnt;

        public CheckResult() {
        }

        public CheckResult(String type, String name, Integer taskCnt) {
            this.type = type;
            this.name = name;
            this.taskCnt = taskCnt;
        }
    }

    static class GetWaitChannelsResult {
        String channel;
        Integer taskCnt;
    }

    static class Tick {
        final long number;
        final long startTs = System.currentTimeMillis();

        // limiting from nextTs, even if there are no such active record, but only deleted all,
        // still improves index usage - skips invisible (deleted) entries (PostgreSql)
        boolean limitNextTs = false;
        int limitNextTsSec = 300;

        // if there are many new tasks (thousands), getting channel stats may be expensive.
        // in that case all channels will be queried separately
        boolean skipChannelLookup = false;

        // check for prime instance every 7 ticks
        boolean checkPrime = false;

        Tick(long number) {
            this.number = number;
        }
    }


    private static class PrimeResult {
        private final List<CheckResult> primeResults = new ArrayList<>();

        public PrimeResult(List<CheckResult> checkResList) {
            for (CheckResult cres : checkResList) {
                if ("PRIM".equals(cres.type))
                    primeResults.add(cres);
            }
        }

        public boolean canPrime() {
            return existsPrime("CAN_PRIME");
        }

        public boolean lostPrime() {
            return existsPrime("LOST_PRIME");
        }

        public boolean isPrime() {
            return existsPrime("PRIME");
        }

        public Date nextPrimeCheck() {
            for (CheckResult cres : primeResults) {
                if ("NEXT_CHECK".equals(cres.name)) {
                    return cres.tillTs;
                }
            }
            return null;
        }

        private boolean existsPrime(String primeCheck){
            for (CheckResult cres : primeResults) {
                if (primeCheck.equals(cres.name))
                    return true;
            }
            return false;
        }

    }

    private static class ChannelLookupResult {
        private final Set<String> channelResults = new HashSet<>();
        private final List<CheckResult> quickCheckResult;
        private final Set<String> taskChannels;

        public ChannelLookupResult(List<CheckResult> checkResList) {
            quickCheckResult = checkResList.stream().filter(cr -> "CHAN".equals(cr.type)).collect(Collectors.toList());
            Set<String> channels = quickCheckResult.stream().map(cr -> cr.name).collect(Collectors.toSet());
            channelResults.addAll(channels);
            taskChannels = new HashSet<>(channelResults);
            taskChannels.removeAll(Model.nonTaskChannels);
        }

        public boolean needProcessTedQueue() {
            return channelResults.contains(Model.CHANNEL_QUEUE);
        }

        public boolean needProcessTedBatch() {
            return channelResults.contains(Model.CHANNEL_BATCH);
        }

        public boolean needProcessTedNotify() {
            return channelResults.contains(Model.CHANNEL_NOTIFY);
        }

        public Set<String> getTaskChannels() {
            return taskChannels;
        }

        public List<CheckResult> getTaskCheckResults() {
            Set<String> taskChannels = getTaskChannels();
            return quickCheckResult.stream().filter(cr -> taskChannels.contains(cr.name)).collect(Collectors.toList());
        }
    }

    public void quickCheck() {

        Tick tick = new Tick(tickCount.getAndIncrement());
        tick.skipChannelLookup = skipNextChannelLookup;
        if (tick.number % 30 == 0) {
            tick.limitNextTs = false; // every 30 tick use full period check
        } else if (tick.number % 6 == 0) {
            tick.limitNextTs = true;
            tick.limitNextTsSec = 3 * 3600; // every 6 tick - longer period
        } else {
            tick.limitNextTs = true;
            tick.limitNextTsSec = 5 * 60;
        }


        // do need to check is instance prime
        CheckPrimeParams checkPrimeParams = getCheckPrimeParams(tick);
        tick.checkPrime = checkPrimeParams != null;

        long startTs = System.currentTimeMillis();

        List<CheckResult> checkResList = callDao(checkPrimeParams, tick);

        long checkDurationMs = System.currentTimeMillis() - startTs;

        PrimeResult primeResult = new PrimeResult(checkResList);
        ChannelLookupResult channelLookupResult = new ChannelLookupResult(checkResList);

        handleResultSystemChannels(channelLookupResult);

        handleResultTaskChannels(channelLookupResult, checkDurationMs, tick);

        handleResultPrime(checkPrimeParams, primeResult);

    }


    private CheckPrimeParams getCheckPrimeParams(Tick tick) {
        CheckPrimeParams checkPrimeParams = null;
        if (context.prime.isEnabled()) {
            boolean needCheckPrime = false;
            if (context.prime.isPrime()) {
                // update every 7 ticks
                needCheckPrime = (tick.number % PrimeInstance.TICK_SKIP_COUNT == 0);
            } else {
                needCheckPrime = (nextPrimeCheckTimeMs <= System.currentTimeMillis());
            }
            if (needCheckPrime) {
                checkPrimeParams = context.prime.checkPrimeParams;
            }
        }
        return checkPrimeParams;
    }


    private List<CheckResult> callDao(CheckPrimeParams checkPrimeParams, Tick tick) {
        // channels
        List<CheckResult> checkResList;
        try {
            if (tick.skipChannelLookup)
                logger.debug("skip channels quick check");

            checkResList = context.tedDao.quickCheck(checkPrimeParams, tick);

            if (tick.skipChannelLookup) { // add all channels
                checkResList = new ArrayList<>(checkResList);
                for (Channel chan : context.registry.getChannels()) {
                    checkResList.add(new CheckResult("CHAN", chan.name, null));
                }
            }
        } catch (RuntimeException e) {
            if (context.prime.isEnabled()) {
                context.prime.lostPrime();
            }
            throw e;
        }

        return checkResList;
    }

    private void handleResultTaskChannels(ChannelLookupResult channelLookupResult, long checkDurationMs, Tick tick) {
        Set<String> allTaskChannels = channelLookupResult.getTaskChannels();

        List<String> taskChannels = new ArrayList<>();

        // if we are not in prime instance, then remove channel allowed to run only in for prime instance
        if (context.prime.isEnabled() && context.prime.isPrime() == false) {
            for (String chanName : allTaskChannels) {
                Channel chan = context.registry.getChannel(chanName);
                if (chan == null || chan.primeOnly)
                    continue;
                taskChannels.add(chanName);
            }
        } else {
            taskChannels.addAll(allTaskChannels);
        }

        // process tasks
        //
        if (! taskChannels.isEmpty()) {
            boolean wasAnyChannelFull = context.taskManager.processChannelTasks(taskChannels, tick);

            // get total waiting task count in db
            int waitingTaskCount = channelLookupResult.getTaskCheckResults().stream()
                .map(cr -> cr.taskCnt)
                .filter(cnt -> cnt != null)
                .reduce(0, Integer::sum);

            // in some cases there may be a lot of new tasks. Then we will skip channel lookup in next quickCheck
            this.skipNextChannelLookup = (skipNextChannelLookup && waitingTaskCount == 0 && wasAnyChannelFull) // 0 can be when skipping channelLookup (if skipNextChannelLookup was true)
                || (checkDurationMs > SKIP_CHANNEL_THRESHOLD_MS) // if last check was long
                || (waitingTaskCount > SKIP_CHANNEL_THRESHOLD_TASK_COUNT); // if there are a lot of waiting tasks
        }

    }

    private void handleResultSystemChannels(ChannelLookupResult channelLookupResult) {
        if (channelLookupResult.needProcessTedQueue()) {
            context.eventQueueManager.processTedQueue();
        }
        if (channelLookupResult.needProcessTedBatch()) {
            context.batchWaitManager.processBatchWaitTasks();
        }
        if (channelLookupResult.needProcessTedNotify()) {
            context.notificationManager.processNotifications();
        }

    }

    private void handleResultPrime(CheckPrimeParams primeParams, PrimeResult primeResult) {
        if (! context.prime.isEnabled())
            return;

        Date tillTs = primeResult.nextPrimeCheck();
        if (tillTs != null && primeParams != null) {
            nextPrimeCheckTimeMs = Math.min(tillTs.getTime(), System.currentTimeMillis() + primeParams.postponeSec() * 1000L);
        }

        if (context.prime.isPrime() && primeResult.lostPrime()) {
            context.prime.lostPrime();
        } else if (! context.prime.isPrime() && primeResult.canPrime()) {
            context.prime.becomePrime();
        }
    }

}
