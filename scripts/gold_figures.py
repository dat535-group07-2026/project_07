"""Plot published Gold and hourly enrichment; never publish pipeline tables.

Run from the checkout: python -m scripts.gold_figures --env dev --dev-user alkval
    --flights-user alkval --weather-user ckolle --out /tmp/gold-figures
"""

import argparse
from pathlib import Path

from project07 import config, spark as spark_mod
from project07.gold.run import MONTHS
from project07.silver import common
from project07.silver.enrichment import enrich_flights

VISIBILITY_BAND_SQL = """CASE
    WHEN observation_utc IS NULL THEN 'No weather'
    WHEN visibility_km IS NULL THEN 'Unknown visibility'
    WHEN visibility_km < 1.6 THEN '<1.6 km'
    WHEN visibility_km < 5 THEN '1.6–5 km'
    WHEN visibility_km < 10 THEN '5–10 km'
    ELSE '>=10 km' END"""
BANDS = ('<1.6 km', '1.6–5 km', '5–10 km', '>=10 km', 'Unknown visibility', 'No weather')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    config.add_config_args(parser)
    parser.add_argument('--flights-user', help='Dev only: whose published flights to read.')
    parser.add_argument('--weather-user', help='Dev only: whose published weather to read.')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args(argv)
    cfg = config.resolve_config(args.env, args.data_root, args.dev_user)
    if args.env == 'prod' and (args.flights_user or args.weather_user):
        parser.error('--flights-user and --weather-user are only for --env dev')
    flights_cfg = config.resolve_config(args.env, args.data_root, args.flights_user or args.dev_user)
    weather_cfg = config.resolve_config(args.env, args.data_root, args.weather_user or args.dev_user)
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from pyspark import StorageLevel

    args.out.mkdir(parents=True, exist_ok=True)
    spark = spark_mod.get_spark(app_name='project07-gold-figures', env=cfg.env)
    enriched = None
    try:
        gold = common.read_ready_table(spark, cfg.gold_root, 'daily_airport')
        gold.createOrReplaceTempView('gold')
        points = spark.sql('''SELECT origin, reported_precipitation_mm AS rain, avg_dep_delay_min AS delay
            FROM gold WHERE reported_precipitation_mm IS NOT NULL AND avg_dep_delay_min IS NOT NULL
            ORDER BY origin, flight_date''').collect()
        known = spark.sql('''SELECT COUNT(*) n, CORR(reported_precipitation_mm, avg_dep_delay_min) r
            FROM gold WHERE reported_precipitation_mm IS NOT NULL AND avg_dep_delay_min IS NOT NULL''').first()
        complete = spark.sql('''SELECT COUNT(*) n, CORR(COALESCE(reported_precipitation_mm, 0), avg_dep_delay_min) r
            FROM gold WHERE precipitation_hours + trace_hours = 24 AND avg_dep_delay_min IS NOT NULL''').first()
        caption = (f'Known pairs: n={known.n}, r={known.r:.4f}. '
                   f'24 numeric/trace hours: n={complete.n}, r={complete.r:.4f}.\n'
                   'Reported precipitation can be incomplete. Association does not show causation.')
        fig, ax = plt.subplots(figsize=(8, 5))
        for airport, colour in [('JFK', '#2864a0'), ('LGA', '#d47720')]:
            rows = [r for r in points if r.origin == airport]
            ax.scatter([r.rain for r in rows], [r.delay for r in rows], label=airport, color=colour, alpha=0.65)
        ax.set(xlabel='Reported daily precipitation (mm)', ylabel='Average departure delay (min)',
               title='Reported precipitation and departure delay')
        ax.legend()
        fig.text(0.02, 0.02, caption, fontsize=8)
        fig.tight_layout(rect=(0, 0.10, 1, 1))
        fig.savefig(args.out / 'rain_vs_delay.png', dpi=180)
        plt.close(fig)
        print(caption)

        flights = None
        for month in MONTHS:
            frame = common.read_ready_table(spark, flights_cfg.silver_root, 'flights', pointer_key=month)
            flights = frame if flights is None else flights.unionByName(frame)
        weather = common.read_ready_table(spark, weather_cfg.silver_root, 'weather_hourly')
        enriched = enrich_flights(flights, weather).persist(StorageLevel.DISK_ONLY)
        enriched.createOrReplaceTempView('enriched')
        # Use the scheduled-hour enrichment here, not the daily minimum visibility in Gold.
        bands = spark.sql(f'''SELECT {VISIBILITY_BAND_SQL} AS band, COUNT(*) AS flights,
            AVG(CASE WHEN dep_delay_min >= 15 THEN 1 ELSE 0 END) AS delayed_rate
            FROM enriched WHERE NOT cancelled AND dep_delay_min IS NOT NULL GROUP BY 1''').collect()
        bands = sorted(bands, key=lambda r: BANDS.index(r.band))
        fig, ax = plt.subplots(figsize=(8, 5))
        bars = ax.bar([r.band for r in bands], [float(r.delayed_rate) for r in bands], color='#2864a0')
        ax.bar_label(bars, labels=[f'n={r.flights}' for r in bands], padding=4, fontsize=8)
        ax.set(xlabel='Visibility at the scheduled departure hour', ylabel='Share delayed 15+ minutes',
               title='Hourly visibility and departure delays', ylim=(0, 1))
        fig.tight_layout()
        fig.savefig(args.out / 'visibility_vs_delay.png', dpi=180)
        plt.close(fig)

        # AVG of daily averages would weight quiet days too much. Use eligible flights.
        monthly = spark.sql('''WITH totals AS (
            SELECT date_format(flight_date, 'yyyy-MM') AS month, origin,
                   SUM(flights) flights, SUM(cancelled_flights) cancelled,
                   SUM(flights_with_weather) matched FROM gold GROUP BY 1, 2
        ), delays AS (
            SELECT date_format(flight_date, 'yyyy-MM') AS month, origin,
                   AVG(dep_delay_min) avg_delay,
                   AVG(CASE WHEN dep_delay_min >= 15 THEN 1 ELSE 0 END) delayed_rate
            FROM enriched WHERE NOT cancelled AND dep_delay_min IS NOT NULL GROUP BY 1, 2
        ) SELECT month, origin, flights, cancelled, cancelled / flights AS cancellation_rate,
                 avg_delay, delayed_rate, matched, matched / flights AS coverage
          FROM totals LEFT JOIN delays USING (month, origin) ORDER BY month, origin''').collect()
        lines = ['| Month | Airport | Flights | Cancelled | Cancellation rate | Avg delay (min) | 15+ rate | With weather | Coverage |',
                 '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
        for row in monthly:
            lines.append('| ' + ' | '.join(f'{v:.4f}' if isinstance(v, float) else str(v) for v in row) + ' |')
        totals = f'Totals: {sum(r.flights for r in monthly)} flights, {sum(r.matched for r in monthly)} with weather.'
        lines += ['', totals, '', 'Delay metrics use non-cancelled flights with a known delay, including diverted flights.']
        (args.out / 'monthly_summary.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
        print('\n'.join(lines))
        print(f'Figures and summary: {args.out.resolve()}')
    finally:
        if enriched is not None:
            enriched.unpersist()
        spark.stop()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
