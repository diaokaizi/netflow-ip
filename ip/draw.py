from collections import Counter
import matplotlib.pyplot as plt
import math
import numpy as np

def count_pic(data: list[int], xlabel: str = '', ylabel: str = '', title: str = '', filename: str = ''):
    grouped = [math.floor(math.log10(x+1)) for x in data]  # 加1是为了处理x=0的情况
    group_counts = Counter(grouped)
    labels, values = zip(*sorted(group_counts.items()))

    # 绘制条形图
    plt.figure(figsize=(10, 6))
    plt.bar(labels, values)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks(range(min(labels), max(labels) + 1))
    plt.savefig(filename)

def draw_aggregation_compare():
    values = [1, 0.74, 0.53, 0.24]
    names = ["raw", "5-tuple", "src_dec_ip", "src_ip"]
    # 绘图
    plt.figure(figsize=(10, 5))
    plt.plot(names, values)
    plt.ylim([0, 1.1])
    # 添加标题和标签
    plt.title("aggregation_compare")
    plt.savefig('aggregation_compare.png')

def draw_rank_ratio(data: list[int]):
    sorted_array = np.sort(data)[::-1]  # 从大到小排序

    # 计算累积和及其占总和的百分比
    cumulative_sums = np.cumsum(sorted_array)
    total_sum = cumulative_sums[-1]
    cumulative_percentage = cumulative_sums / total_sum * 100

    # 绘制图表
    plt.figure(figsize=(10, 6))
    # plt.plot(np.log10(np.arange(1, len(data) + 1)), cumulative_percentage)
    plt.plot(cumulative_percentage)
    plt.xlabel('bytes_rank')
    plt.ylabel('ratio')
    plt.title('rank_ratio')
    plt.grid(True)
    plt.savefig('rank_ratio.png')