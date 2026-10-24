#!/usr/bin/env python3
"""Render shareable PNG/SVG figures from verified cloud analysis JSON.

Requires matplotlib; does not contact the servers or recompute percentiles.
"""
import argparse
import json
import os
from pathlib import Path
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    os.environ.setdefault('MPLCONFIGDIR', str(Path(tempfile.gettempdir()) / f'yewukv-matplotlib-{os.getuid()}'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    plt.rcParams.update({'font.family': 'Noto Sans CJK SC', 'font.size': 10,
                         'axes.spines.top': False, 'axes.spines.right': False,
                         'axes.unicode_minus': False, 'svg.fonttype': 'path'})
    analysis = json.loads((args.root / 'analysis.json').read_text())
    groups = analysis['groups']
    profiles = [('read-only', '纯读', '#2563eb'), ('balanced', '50% 写入', '#059669'),
                ('write-heavy', '90% 写入', '#d97706')]
    output = args.root / 'figures'
    output.mkdir(exist_ok=True)

    def series(profile, field):
        return [g[field] for g in groups if g['profile'] == profile]

    def finish(fig, name):
        fig.savefig(output / f'{name}.png', dpi=180, facecolor='white')
        svg = output / f'{name}.svg'
        fig.savefig(svg, facecolor='white', metadata={'Date': None})
        svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines()) + '\n')
        plt.close(fig)

    x = [1, 4, 8, 16, 32]
    fig, axes = plt.subplots(1, 3, figsize=(15.5, 5.0))
    fig.subplots_adjust(left=.06, right=.98, bottom=.23, top=.75, wspace=.28)
    for ax, field, title, unit in zip(axes, ('ops_per_second', 'p95_ms', 'p99_ms'),
                                    ('吞吐量', 'P95 延迟', 'P99 延迟'),
                                    ('成功操作 / 秒', '毫秒', '毫秒')):
        for profile, label, color in profiles:
            ax.plot(x, series(profile, f'{field}_median_run'), marker='o', ms=4.5,
                    lw=2, color=color, label=label)
            ax.fill_between(x, series(profile, f'{field}_min_run'),
                            series(profile, f'{field}_max_run'), color=color, alpha=.12)
        ax.set(title=title, xlabel='并发连接数', ylabel=unit, xticks=x, ylim=(0, None))
        ax.grid(alpha=.17)
    fig.suptitle('三台 ECS 跨机压测：吞吐量与 P95 / P99', fontsize=18, y=.97, fontweight='bold')
    fig.text(.5, .86, '每台 2 vCPU / 4 GiB · 128 B value · 每轮 30 秒 · 三次重复', ha='center', fontsize=11)
    fig.legend(*axes[0].get_legend_handles_labels(), loc='lower center', bbox_to_anchor=(.5, .07),
               ncol=3, frameon=False, fontsize=11)
    fig.text(.5, .025, '线：三次统计量中位数；阴影：最小—最大范围。客户端与 Follower 共置；闭环负载。',
             ha='center', color='#555555', fontsize=9)
    finish(fig, 'concurrency')

    fig, axes = plt.subplots(1, 2, figsize=(13.2, 5.2))
    fig.subplots_adjust(left=.07, right=.98, bottom=.25, top=.73, wspace=.25)
    fields = [('p95_ms_median_run', 'P95 中位数', '#059669', '-'),
              ('p99_ms_median_run', 'P99 中位数', '#2563eb', '-'),
              ('p999_ms_median_run', 'P99.9 中位数', '#d97706', '-'),
              ('max_ms_max_run', '观测最大值', '#6b7280', '--')]
    for field, label, color, style in fields:
        axes[0].plot(x, series('read-only', field), marker='o', ms=4, lw=2,
                     label=label, color=color, linestyle=style)
    axes[0].set(title='纯读：P95 与尾部延迟的分离', xlabel='并发连接数', ylabel='毫秒',
                xticks=x, ylim=(0, None))
    axes[0].legend(fontsize=8, frameon=False, loc='upper left')
    positions = np.arange(3)
    for i, (field, label, color, _) in enumerate(fields):
        values = [next(g[field] for g in groups if g['profile'] == p and g['clients'] == 32)
                  for p, _, _ in profiles]
        bars = axes[1].bar(positions + (i - 1.5) * .19, values, width=.18,
                           color=color, label=label, alpha=.9)
        if field == 'max_ms_max_run':
            axes[1].bar_label(bars, fmt='%.1f', padding=3, fontsize=9)
    axes[1].set(title='32 并发：分位数与最坏请求', ylabel='毫秒', xticks=positions,
                xticklabels=[p[1] for p in profiles], ylim=(0, 170))
    for ax in axes:
        ax.grid(axis='y', alpha=.17)
        ax.set_axisbelow(True)
    fig.suptitle('尾延迟记录：高吞吐量下仍需检查慢请求', fontsize=17, y=.97, fontweight='bold')
    fig.text(.5, .85, 'P95 / P99 / P99.9 为三次分位数的中位数；最大值为三次中的最坏观测',
             ha='center', fontsize=10)
    fig.legend(*axes[0].get_legend_handles_labels(), loc='lower center', bbox_to_anchor=(.5, .07),
               ncol=4, frameon=False, fontsize=9)
    fig.text(.5, .025, '共 45 轮、1,954,767 次计时请求；未记录失败。未采集远端 CPU / 网络追踪，暂不归因。',
             ha='center', color='#555555', fontsize=9)
    finish(fig, 'tails')
    print(json.dumps({'figures': ['concurrency.png', 'concurrency.svg', 'tails.png', 'tails.svg'],
                      'matplotlib': matplotlib.__version__}))


if __name__ == '__main__':
    main()
