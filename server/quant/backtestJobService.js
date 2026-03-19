export class BacktestJobService {
  constructor({
    backtestRunner,
    historicalDataService,
    resolveStrategy,
    createJob,
    updateJob,
    completeJob,
    failJob,
    saveResult,
    listJobProgress,
    getJobById
  }) {
    this.backtestRunner = backtestRunner;
    this.historicalDataService = historicalDataService;
    this.resolveStrategy = resolveStrategy;
    this.createJob = createJob;
    this.updateJob = updateJob;
    this.completeJob = completeJob;
    this.failJob = failJob;
    this.saveResult = saveResult;
    this.listJobProgress = listJobProgress;
    this.getJobById = getJobById;
  }

  start({ strategyRef, runConfig }) {
    const normalizedRunConfig = {
      includeCurrentDay: Boolean(runConfig?.includeCurrentDay),
      ...runConfig
    };

    const job = this.createJob({
      strategy_id: strategyRef?.id && Number.isFinite(Number(strategyRef.id)) ? Number(strategyRef.id) : null,
      run_config_json: JSON.stringify({ ...normalizedRunConfig, strategyRef })
    });

    setTimeout(() => {
      this.#run(job.id, strategyRef, normalizedRunConfig).catch((error) => {
        console.error('[backtest] uncaught runner failure', { jobId: job.id, error: error?.message || error });
      });
    }, 0);

    return job;
  }

  async #run(jobId, strategyRef, runConfig) {
    const startedAt = Date.now();
    let latestCoverage = null;

    const assertNotCancelled = () => {
      const dbJob = this.getJobById(jobId);
      if (!dbJob || dbJob.status === 'cancelled') throw new Error('cancelled');
    };

    const persistProgress = ({
      processed = 0,
      total = 1,
      progressPct = null,
      marker,
      currentDate,
      currentDay,
      totalDays,
      totalTrades = 0,
      elapsedMs,
      phase,
      coverage = latestCoverage,
      hydration = null,
      replay = null
    }) => {
      assertNotCancelled();
      this.updateJob(jobId, {
        status: 'running',
        progress_pct: Number.isFinite(Number(progressPct))
          ? Math.max(0, Math.min(Math.floor(Number(progressPct)), 99))
          : Math.min(Math.floor((processed / Math.max(total, 1)) * 100), 99),
        processed_items: processed,
        current_marker: marker,
        progress_json: JSON.stringify({
          phase: phase || null,
          coverage: coverage || null,
          hydration: hydration || null,
          replay: replay || null
        }),
        current_date: currentDate,
        current_day: currentDay,
        total_days: totalDays,
        closed_trade_count: totalTrades,
        elapsed_ms: elapsedMs
      });
    };

    try {
      const resolved = this.resolveStrategy(strategyRef);
      if (!resolved) throw new Error('Strategy not found');

      persistProgress({
        progressPct: 1,
        marker: 'Preparing historical coverage',
        phase: 'planning',
        currentDate: runConfig?.startDate || null,
        currentDay: 1,
        totalDays: computeDayCount(runConfig?.startDate, runConfig?.endDate),
        elapsedMs: 0,
        coverage: {
          totalDays: computeDayCount(runConfig?.startDate, runConfig?.endDate),
          readyDays: 0,
          hydratableDays: 0,
          waitingOnCoverage: true,
          hydratingDay: null,
          includeCurrentDay: Boolean(runConfig?.includeCurrentDay),
          requestedCurrentUtcDay: false,
          currentUtcDaySlowPath: false,
          classifications: []
        }
      });

      const coveragePlan = await this.historicalDataService.prepareCoverage({
        symbol: resolved.strategy.market?.symbol,
        startDate: runConfig.startDate,
        endDate: runConfig.endDate,
        includeCurrentDay: Boolean(runConfig.includeCurrentDay),
        shouldStop: assertNotCancelled,
        progressCallback: ({ phase, stage, progressPct, coverage, hydration, currentDate, currentDay, totalDays }) => {
          latestCoverage = coverage || latestCoverage;
          persistProgress({
            progressPct,
            marker: stage,
            phase,
            coverage: latestCoverage,
            hydration,
            replay: null,
            currentDate,
            currentDay,
            totalDays,
            elapsedMs: Date.now() - startedAt,
            totalTrades: 0
          });
        }
      });

      latestCoverage = {
        totalDays: coveragePlan.totalDays,
        readyDays: coveragePlan.readyDays,
        hydratableDays: coveragePlan.hydratableDays,
        waitingOnCoverage: false,
        hydratingDay: null,
        includeCurrentDay: coveragePlan.includeCurrentDay,
        requestedCurrentUtcDay: coveragePlan.requestedCurrentUtcDay,
        currentUtcDaySlowPath: coveragePlan.requestedCurrentUtcDay && coveragePlan.includeCurrentDay,
        classifications: summarizeCoveragePlan(coveragePlan.days)
      };

      const resultPayload = await this.backtestRunner.run({
        strategy: resolved.strategy,
        coveragePlan,
        runConfig: {
          ...runConfig,
          startedAtIso: new Date(startedAt).toISOString()
        },
        shouldStop: assertNotCancelled,
        progressCallback: ({ processed, total, progressPct, marker, currentDate, currentDay, totalDays, totalTrades, elapsedMs, phase, hydration, replay }) => {
          persistProgress({
            processed,
            total,
            progressPct,
            marker,
            currentDate,
            currentDay,
            totalDays,
            totalTrades,
            elapsedMs,
            phase,
            coverage: latestCoverage,
            hydration,
            replay
          });
        }
      });

      const result = this.saveResult({
        job_id: jobId,
        summary_json: JSON.stringify({
          ...resultPayload.metrics,
          analyses: resultPayload.analyses,
          sessionResults: resultPayload.sessionResults,
          strategy: resolved.summary,
          runConfig
        }),
        equity_series_json: JSON.stringify({
          cumulativePnlSeries: resultPayload.cumulativePnlSeries,
          equitySeries: resultPayload.equitySeries,
          drawdownSeries: resultPayload.drawdownSeries
        }),
        trade_log_json: JSON.stringify(resultPayload.trades)
      });

      this.completeJob(jobId, {
        progress_pct: 100,
        current_marker: 'Completed',
        progress_json: JSON.stringify({
          phase: 'completed',
          coverage: latestCoverage,
          hydration: null,
          replay: null
        }),
        result_id: result.id,
        elapsed_ms: Date.now() - startedAt,
        closed_trade_count: resultPayload.trades.length
      });
    } catch (error) {
      if (String(error?.message).includes('cancelled')) return;

      try {
        this.failJob(jobId, error, {
          elapsed_ms: Date.now() - startedAt,
          current_marker: 'Failed',
          progress_json: JSON.stringify({
            phase: 'failed',
            coverage: latestCoverage,
            hydration: null,
            replay: null
          })
        });
      } catch (failError) {
        console.error('[backtest] unable to persist failed job state', {
          jobId,
          error: failError?.message || failError,
          cause: error?.message || error
        });
      }
    }
  }

  cancel(jobId) {
    this.updateJob(jobId, {
      status: 'cancelled',
      current_marker: 'Cancelled by operator',
      progress_json: JSON.stringify({
        phase: 'cancelled',
        coverage: null,
        hydration: null,
        replay: null
      })
    });
  }

  getProgress(jobId) {
    return this.listJobProgress(jobId);
  }
}

function computeDayCount(startDate, endDate) {
  if (!startDate) return null;
  const start = new Date(startDate);
  const end = new Date(endDate || startDate);
  const startMs = Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate());
  const endMs = Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate());
  return Math.floor((endMs - startMs) / 86400000) + 1;
}

function summarizeCoveragePlan(days = []) {
  const counts = new Map();
  for (const day of days) {
    counts.set(day.classification, (counts.get(day.classification) || 0) + 1);
  }
  return [...counts.entries()].map(([classification, count]) => ({ classification, count }));
}
