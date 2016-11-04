import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cmx
import matplotlib.colors as colors
import numpy as np


def get_cmap(N):
    '''Returns a function that maps each index in 0, 1, ... N-1 to a distinct
    RGB color.'''
    color_norm  = colors.Normalize(vmin=0, vmax=N - 1)
    scalar_map = cmx.ScalarMappable(norm=color_norm, cmap='hsv')
    def map_index_to_rgb_color(index):
        return scalar_map.to_rgba(index)
    return map_index_to_rgb_color

grouping_colors={
    'bpg': 'r',
    'pkg': 'g',
    'sg': 'b',
    'kg': 'k'
}

grouping_sources_colors={
    0: 'r',
    1: 'g',
    2: 'b',
    3: 'k'
}

grouping_style={
    'bpg': grouping_colors['bpg'] + '^-',
    'pkg': grouping_colors['pkg'] + 'o--',
    'sg': grouping_colors['sg'] + 'x--',
    'kg': grouping_colors['kg'] + 's--'
}

grouping_sources_indices={
    'bpg-5': 0,
    'pkg-5': 1,
    'bpg-10': 2,
    'pkg-10': 3
}

grouping_sources_labels={
    'bpg-5': '$bpg_5$',
    'bpg-10': '$bpg_{10}$',
    'pkg-5': '$pkg_5$',
    'pkg-10': '$pkg_{10}$'
}

def plotImbalance(imbalanceMap, ylabel, xlabel, legendLabels):
    fig, ax = plt.subplots(1, imbalanceMap.__len__(), sharey=True)
    # legend_colors_generator = get_cmap(legendLabels.__len__())
    handles = []
    for ind, key in enumerate(imbalanceMap):
        sub_handles=[]
        df = imbalanceMap[key]
        x = list(df.index)
        for ind1, ele in enumerate(x):
            x[ind1] = int(x[ind1])
        df.index = x
        df = df.sort_index()
        x = list(df.index)
        x_axis = list(range(1, x.__len__()+1))
        w = 0.3
        cols = list(df.columns)
        for ind2, col in enumerate(cols):
            # line = ax[ind].bar(np.asarray(x_axis)-(cols.__len__()/2 - ind2)*w, df.loc[:,col].values, width=w, \
            #             color=legend_colors_generator(ind2), align='center', log=True)
            line = ax[ind].bar(np.asarray(x_axis)-(cols.__len__()/2 - ind2)*w, df.loc[:,col].values, width=w, \
                        color=grouping_colors[col], align='center', log=True)
            sub_handles.append(line)
        if sub_handles.__len__() > handles.__len__():
            handles = sub_handles
        ax[ind].set(xlabel=xlabel)
        ax[ind].set_yscale('log')
        ax[ind].set_xlim([0, x.__len__() + 1])
        ax[ind].set_ylim([1.0E-4, 1.0E-1])
        ax[ind].set_xticks(x_axis)
        ax[ind].set_xticklabels(x)
        ax[ind].text(0.04,0.04, key, size='large')
    fig.legend(handles, legendLabels, 'upper center', ncol=legendLabels.__len__())
    fig.text(0.04, 0.5, ylabel, va='center', rotation='vertical', size='large')
    # plt.show()
    if not os.path.exists('expViz'):
        os.mkdir('expViz')
    plt.savefig('expViz/exp1.png')
    # pass

def exp1_viz():
    imbalanceMap = {}
    groupingList = []
    metrics_dir = 'exp1'
    for metrics_file in os.listdir(metrics_dir):
        print('processing file: ' + metrics_file)
        metrics = json.loads(open(metrics_dir + '/' + metrics_file, 'r').read())
        dataset = str(metrics['dataset'])
        sources = int(metrics['sources'])
        workers = int(metrics['workers'])
        grouping = str(metrics['grouping'])
        if not groupingList.__contains__(grouping):
            groupingList.append(grouping)
        delay = int(metrics['delay'])
        imbalance = float(metrics['imbalance'])
        throughput = int(metrics['throughput'])
        if not imbalanceMap.__contains__(dataset):
            df = pd.DataFrame({grouping: imbalance}, index=[str(workers)])
            imbalanceMap[dataset] = df
        else:
            df = imbalanceMap[dataset]
            df.loc[str(workers), grouping] = imbalance
    plotImbalance(imbalanceMap, ylabel='Imbalance Rate', xlabel='workers', legendLabels=groupingList)


def plotThroughput(throughputContainer, ylabel, xlabel, legendLabels):
    fig, ax = plt.subplots(1, 1, sharey=True)
    x = list(throughputContainer.index)
    for ind1, ele in enumerate(x):
        x[ind1] = int(x[ind1])
    throughputContainer.index = x
    throughputContainer = throughputContainer.sort_index()
    x = list(throughputContainer.index)
    x_axis = list(range(1, x.__len__() + 1))
    w = 0.3
    cols = list(throughputContainer.columns)
    handles=[]
    for ind1, col in enumerate(cols):
        line, = ax.plot(x_axis, throughputContainer.loc[:,col].values, grouping_style[col])
        handles.append(line)
    ax.set(xlabel=xlabel)
    ax.set_xlim([0, x.__len__() + 1])
    ax.set_ylim([500, 6500])
    ax.set_xticks(x_axis)
    ax.set_xticklabels(x)
    ax.legend(handles, legendLabels, loc='lower left')
    fig.text(0.04, 0.5, ylabel, va='center', rotation='vertical', size='large')
    # plt.show()
    if not os.path.exists('expViz'):
        os.mkdir('expViz')
    plt.savefig('expViz/exp3.png')

def exp3_viz():
    groupingList = []
    metrics_dir = 'exp3'
    throughputContainer = pd.DataFrame()
    for metrics_file in os.listdir(metrics_dir):
        print('processing file: ' + metrics_file)
        metrics = json.loads(open(metrics_dir + '/' + metrics_file, 'r').read())
        dataset = str(metrics['dataset'])
        sources = int(metrics['sources'])
        workers = int(metrics['workers'])
        grouping = str(metrics['grouping'])
        if not groupingList.__contains__(grouping):
            groupingList.append(grouping)
        delay = int(metrics['delay'])
        imbalance = float(metrics['imbalance'])
        throughput = int(metrics['throughput'])
        throughputContainer.loc[str(delay), grouping] = throughput
    plotThroughput(throughputContainer, ylabel='Throughput (keys/s)', xlabel='Worker task delay(ms)', legendLabels=groupingList)


def plotImbalanceRobustness(imbalanceContainer, ylabel, xlabel, legendLabels):
    fig, ax = plt.subplots(1, 1, sharey=True)
    x = list(imbalanceContainer.index)
    for ind1, ele in enumerate(x):
        x[ind1] = int(x[ind1])
    imbalanceContainer.index = x
    imbalanceContainer = imbalanceContainer.sort_index()
    imbalanceContainer = imbalanceContainer.sort_index(axis=1)
    x = list(imbalanceContainer.index)
    w = 0.3
    expand_coe = np.ceil(w * legendLabels.__len__())
    x_axis = list(expand_coe * np.asarray(range(1, x.__len__() + 1)))
    cols = list(imbalanceContainer.columns)
    handles=[]
    for ind, col in enumerate(cols):
        handle = ax.bar(np.asarray(x_axis) - (cols.__len__() / 2 - ind) * w, imbalanceContainer.loc[:, col].values, \
                        width=w, color=grouping_sources_colors[col], align='center', log=True)
        handles.append(handle)
    ax.set(xlabel=xlabel)
    ax.set_yscale('log')
    ax.set_xlim([0, expand_coe*(x.__len__() + 1)])
    ax.set_ylim([1.0E-4, 1.0E-1])
    ax.set_xticks(x_axis)
    ax.set_xticklabels(x)
    ax.legend(handles, legendLabels, loc='upper left')
    fig.text(0.04, 0.5, ylabel, va='center', rotation='vertical', size='large')
    # plt.show()
    if not os.path.exists('expViz'):
        os.mkdir('expViz')
    plt.savefig('expViz/exp2.png')
    pass

def exp2_viz():
    groupingSourcesList = []
    metrics_dir = 'exp2'
    imbalanceContainer = pd.DataFrame()
    for metrics_file in os.listdir(metrics_dir):
        print('processing file: ' + metrics_file)
        metrics = json.loads(open(metrics_dir + '/' + metrics_file, 'r').read())
        dataset = str(metrics['dataset'])
        sources = int(metrics['sources'])
        workers = int(metrics['workers'])
        grouping = str(metrics['grouping'])
        if not groupingSourcesList.__contains__(grouping + '-' + str(sources)):
            groupingSourcesList.append(grouping + '-' + str(sources))
        delay = int(metrics['delay'])
        imbalance = float(metrics['imbalance'])
        throughput = int(metrics['throughput'])
        imbalanceContainer.loc[str(workers), grouping_sources_indices[grouping + '-' + str(sources)]] = imbalance
    groupingSourcesLabels = [None] * groupingSourcesList.__len__()
    for ele in groupingSourcesList:
        groupingSourcesLabels[grouping_sources_indices[ele]] = grouping_sources_labels[ele]
    plotImbalanceRobustness(imbalanceContainer, ylabel='Imbalance Rate', xlabel='workers',
                           legendLabels=groupingSourcesLabels)

def main():
    # experiment 4: chronological stable
    pass

if __name__=='__main__':
    main()