from collections import Counter
import matplotlib.pyplot as plt
import math

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