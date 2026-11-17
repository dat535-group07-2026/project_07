"""Draw the report figures and the monthly summary from the published Gold tables.

Reads only Gold (daily_airport, airport_hour, departure_chain); never writes
pipeline tables. Every number in the figures is also printed, so it can be
checked and quoted.

Figures (drawn at report column width):
  rain_vs_delay.png        daily rain and average departure delay (daily_airport)
  visibility_vs_delay.png  share delayed 15+ min by visibility in the hour (airport_hour)
  delay_propagation.png    share delayed 15+ min by how late the inbound plane was (departure_chain)
  weather_both_ends.png    share delayed 15+ min by weather at JFK/LGA and at the inbound origin
  monthly_summary.md       flights, cancellations, delays and weather coverage per month and airport

Run from the checkout:
    python -m scripts.gold_figures --env dev --dev-user alkval --out /tmp/gold-figures
"""

import argparse
import json
from pathlib import Path

from project07 import config, spark as spark_mod
from project07.silver import common

VISIBILITY_BAND_SQL = """CASE
    WHEN NOT has_weather THEN 'No weather'
    WHEN visibility_km IS NULL THEN 'Unknown visibility'
    WHEN visibility_km < 1.6 THEN '<1.6 km'
    WHEN visibility_km < 5 THEN '1.6–5 km'
    WHEN visibility_km < 10 THEN '5–10 km'
    ELSE '>=10 km' END"""
BANDS = ('<1.6 km', '1.6–5 km', '5–10 km', '>=10 km', 'Unknown visibility', 'No weather')
INBOUND_BANDS = ('under 15', '15-44', '45-89', '90+')
# The report is two-column, so figures are drawn at column width with readable text.
FIGURE_SIZE = (3.5, 2.5)
BLUE, ORANGE, GREY = '#2864a0', '#d47720', '#777777'


def _pct(part, whole):
    return 100.0 * part / whole if whole else float('nan')


def _bar_chart(plt, path, labels, values, counts, xlabel, ylim):
    fig, ax = plt.subplots(figsize=FIGURE_SIZE)
    bars = ax.bar(labels, values, color=BLUE, width=0.65)
    ax.bar_label(bars, labels=[f'{v:.1f}%\nn = {n:,}' for v, n in zip(values, counts)], padding=2, fontsize=7)
    ax.set(xlabel=xlabel, ylabel='Delayed 15+ minutes (%)', ylim=ylim)
    fig.tight_layout(pad=0.3)
    fig.savefig(path, dpi=300)
    plt.close(fig)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    config.add_config_args(parser)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args(argv)
    cfg = config.resolve_config(args.env, args.data_root, args.dev_user)
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    plt.rcParams.update({'font.size': 8, 'axes.spines.top': False, 'axes.spines.right': False})
    args.out.mkdir(parents=True, exist_ok=True)
    spark = spark_mod.get_spark(app_name='project07-gold-figures', env=cfg.env)
    try:
        for table in ('daily_airport', 'airport_hour', 'departure_chain'):
            common.read_ready_table(spark, cfg.gold_root, table).createOrReplaceTempView(table)
        pointer = json.loads(common.current_pointer(cfg.gold_root, 'daily_airport').read_text())
        manifest = json.loads((cfg.gold_root / 'daily_airport' / pointer['ready_manifest']).read_text())
        print(f"Gold run {manifest['run_id']}, months {manifest['months'][0]} to {manifest['months'][-1]}")

        # 1. Rain. Only days with a weather report in all 24 hours. Filled points: every hour
        #    had an amount or a trace. Open points: NOAA left dry hours blank (from 27 August
        #    2025), so blank hours are counted as dry (decision D14).
        points = spark.sql('''SELECT origin, COALESCE(reported_precipitation_mm, 0) AS rain,
                   avg_dep_delay_min AS delay, precipitation_hours + trace_hours = 24 AS complete
            FROM daily_airport WHERE weather_hours = 24 AND avg_dep_delay_min IS NOT NULL''').collect()
        complete = spark.sql('''SELECT COUNT(*) n, CORR(COALESCE(reported_precipitation_mm, 0), avg_dep_delay_min) r
            FROM daily_airport WHERE precipitation_hours + trace_hours = 24 AND avg_dep_delay_min IS NOT NULL''').first()
        all_hours = spark.sql('''SELECT COUNT(*) n, CORR(COALESCE(reported_precipitation_mm, 0), avg_dep_delay_min) r
            FROM daily_airport WHERE weather_hours = 24 AND avg_dep_delay_min IS NOT NULL''').first()
        print(f'Rain: fully reported days n={complete.n}, r={complete.r:.4f}; '
              f'all days with 24 weather hours (blank = dry) n={all_hours.n}, r={all_hours.r:.4f}')
        fig, ax = plt.subplots(figsize=FIGURE_SIZE)
        for airport, colour in (('JFK', BLUE), ('LGA', ORANGE)):
            rows = [r for r in points if r.origin == airport and r.complete]
            ax.scatter([r.rain for r in rows], [r.delay for r in rows], label=airport, color=colour,
                       alpha=0.7, s=10, linewidths=0)
        rows = [r for r in points if not r.complete]
        ax.scatter([r.rain for r in rows], [r.delay for r in rows], label='Blank hours = dry',
                   facecolors='none', edgecolors=GREY, s=10, linewidths=0.5)
        ax.set(xlabel='Daily rain (mm)', ylabel='Average departure delay (min)')
        ax.legend(frameon=False, ncol=3, loc='lower center', bbox_to_anchor=(0.5, 1.0),
                  columnspacing=1.0, handletextpad=0.2)
        fig.tight_layout(pad=0.3)
        fig.savefig(args.out / 'rain_vs_delay.png', dpi=300)
        plt.close(fig)

        # 2. Visibility in the departure hour, weighted by flights (sums, not averages of hours).
        bands = {r.band: r for r in spark.sql(f'''SELECT {VISIBILITY_BAND_SQL} AS band,
                   SUM(flights_with_delay) AS flights, SUM(delayed_15) AS delayed
            FROM airport_hour GROUP BY 1''').collect()}
        for band in BANDS:
            if band in bands:
                r = bands[band]
                print(f'Visibility {band}: n={r.flights}, delayed 15+ = {_pct(r.delayed, r.flights):.1f}%')
        shown = [bands[b] for b in BANDS[:4] if b in bands]
        _bar_chart(plt, args.out / 'visibility_vs_delay.png', [r.band.replace('>=', '≥') for r in shown],
                   [_pct(r.delayed, r.flights) for r in shown], [r.flights for r in shown],
                   'Visibility in the scheduled departure hour', (0, 40))

        # 3. Delay propagation: how late the inbound plane was.
        # BTS splits a flight's arrival delay into causes; "late aircraft" is the part the
        # airline blames on the plane arriving late. Shown per departure as a check on the links.
        chain = {r.band: r for r in spark.sql('''SELECT inbound_delay_band AS band,
                   SUM(departures) AS departures, SUM(departures_with_delay) AS flights,
                   SUM(delayed_15) AS delayed, SUM(total_dep_delay_min) AS dep_minutes,
                   SUM(bts_late_aircraft_delay_min) AS late_aircraft_minutes
            FROM departure_chain GROUP BY 1''').collect()}
        for band in INBOUND_BANDS:
            r = chain[band]
            print(f'Inbound {band} min late: n={r.flights}, delayed 15+ = {_pct(r.delayed, r.flights):.1f}%, '
                  f'avg delay {r.dep_minutes / r.flights:.1f} min, '
                  f'BTS late-aircraft minutes per departure {(r.late_aircraft_minutes or 0) / r.departures:.1f}')
        shown = [chain[b] for b in INBOUND_BANDS]
        _bar_chart(plt, args.out / 'delay_propagation.png', [f'{b}' for b in INBOUND_BANDS],
                   [_pct(r.delayed, r.flights) for r in shown], [r.flights for r in shown],
                   'How late the inbound plane arrived (min)', (0, 80))

        # 4. Weather at both ends. Also printed with the inbound delay held under 15 minutes,
        #    to see how much of the effect goes through a late inbound plane.
        def both_ends(extra=''):
            return {(r.local_weather, r.origin_weather): r for r in spark.sql(f'''SELECT local_weather,
                       origin_weather, SUM(departures_with_delay) AS flights, SUM(delayed_15) AS delayed
                FROM departure_chain WHERE local_weather != 'unknown' AND origin_weather != 'unknown' {extra}
                GROUP BY 1, 2''').collect()}
        cells, on_time = both_ends(), both_ends("AND inbound_delay_band = 'under 15'")
        for key in sorted(cells):
            print(f'Weather here={key[0]}, at origin={key[1]}: n={cells[key].flights}, '
                  f'delayed 15+ = {_pct(cells[key].delayed, cells[key].flights):.1f}% '
                  f'(inbound under 15 min late: {_pct(on_time[key].delayed, on_time[key].flights):.1f}%, '
                  f'n={on_time[key].flights})')
        fig, ax = plt.subplots(figsize=FIGURE_SIZE)
        width = 0.36
        for i, (origin, colour) in enumerate((('good', BLUE), ('bad', ORANGE))):
            xs = [j + (i - 0.5) * width for j in range(2)]
            values = [_pct(cells[(local, origin)].delayed, cells[(local, origin)].flights) for local in ('good', 'bad')]
            bars = ax.bar(xs, values, width=width, color=colour, label=f'{origin.capitalize()} at origin')
            ax.bar_label(bars, labels=[f'{v:.1f}%' for v in values], padding=2, fontsize=7)
        ax.set_xticks(range(2), ['Good at JFK/LGA', 'Bad at JFK/LGA'])
        ax.set(ylabel='Delayed 15+ minutes (%)', ylim=(0, 42))
        ax.legend(frameon=False, ncol=2, loc='lower center', bbox_to_anchor=(0.5, 1.0))
        fig.tight_layout(pad=0.3)
        fig.savefig(args.out / 'weather_both_ends.png', dpi=300)
        plt.close(fig)

        # 5. Monthly summary from Gold sums.
        monthly = spark.sql('''WITH days AS (
            SELECT date_format(flight_date, 'yyyy-MM') AS month, origin, SUM(flights) AS flights,
                   SUM(cancelled_flights) AS cancelled, SUM(flights_with_weather) AS matched
            FROM daily_airport GROUP BY 1, 2
        ), hours AS (
            SELECT date_format(local_date, 'yyyy-MM') AS month, origin, SUM(flights_with_delay) AS with_delay,
                   SUM(delayed_15) AS delayed, SUM(total_dep_delay_min) AS minutes
            FROM airport_hour GROUP BY 1, 2
        ) SELECT d.month, d.origin, flights, cancelled, cancelled / flights AS cancellation_rate,
                 minutes / with_delay AS avg_delay, delayed / with_delay AS delayed_rate,
                 matched / flights AS coverage
          FROM days d JOIN hours h USING (month, origin) ORDER BY month, origin''').collect()
        lines = ['| Month | Airport | Flights | Cancelled | Cancellation rate | Avg delay (min) | 15+ rate | Coverage |',
                 '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |']
        for row in monthly:
            lines.append('| ' + ' | '.join(f'{v:.4f}' if isinstance(v, float) else str(v) for v in row) + ' |')
        lines += ['', f'Totals: {sum(r.flights for r in monthly)} flights.', '',
                  'Delay metrics use non-cancelled flights with a known delay, including diverted flights.',
                  'Months of the hourly table use the New York date of the scheduled hour.']
        (args.out / 'monthly_summary.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
        print(f'Figures and summary: {args.out.resolve()}')
    finally:
        spark.stop()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
