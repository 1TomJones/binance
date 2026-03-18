export class BacktestJobService {
  constructor({
    backtestRunner,
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
    const job = this.createJob({
      strategy_id: strategyRef?.id && Number.isFinite(Number(strategyRef.id)) ? Number(strategyRef.id) : null,
      run_config_json: JSON.stringify({ ...runConfig, strategyRef })
    });
    setTimeout(() => this.#run(job.id, strategyRef, runConfig), 0);
    return job;
  }

  async #run(jobId, strategyRef, runConfig) {
    const startedAt = Date.now();

    try {
      const resolved = this.resolveStrategy(strategyRef);
      if (!resolved) throw new Error('Strategy not found');

      this.updateJob(jobId, { status: 'running', progress_pct: 1, current_marker: 'Preparing historical replay', elapsed_ms: 0 });

      const resultPayload = this.backtestRunner.run({
        strategy: resolved.strategy,
        runConfig: {
          ...runConfig,
          startedAtIso: new Date(startedAt).toISOString()
        },
        shouldStop: () => {
          const dbJob = this.getJobById(jobId);
          if (!dbJob || dbJob.status === 'cancelled') throw new Error('cancelled');
        },
        progressCallback: ({ processed, total, marker, currentDate, totalTrades, elapsedMs, dayIndex, totalDays }) => {
          const dbJob = this.getJobById(jobId);
          if (!dbJob || dbJob.status === 'cancelled') throw new Error('cancelled');
          this.updateJob(jobId, {
            status: 'running',
            progress_pct: Math.min(Math.floor((processed / Math.max(total, 1)) * 100), 99),
            processed_items: processed,
            current_marker: `${marker} · ${totalTrades} trades · day ${dayIndex}/${totalDays}`,
            elapsed_ms: elapsedMs,
            current_date: currentDate
          });
        }
      });

      const result = this.saveResult({
        job_id: jobId,
        summary_json: JSON.stringify({
          ...resultPayload.metrics,
          analyses: resultPayload.analyses,
          dayResults: resultPayload.dayResults,
          strategy: resolved.summary
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
        result_id: result.id,
        elapsed_ms: Date.now() - startedAt
      });
    } catch (error) {
      if (String(error?.message).includes('cancelled')) return;

      try {
        this.failJob(jobId, error, {
          elapsed_ms: Date.now() - startedAt,
          current_marker: 'Failed'
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
    this.updateJob(jobId, { status: 'cancelled', current_marker: 'Cancelled by operator' });
  }

  getProgress(jobId) {
    return this.listJobProgress(jobId);
  }
}
