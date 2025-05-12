
import os
import time
import numpy as np
import matplotlib.pyplot as plt
plt.style.use('ggplot')


def measure_mongo_query_time(collection, query_filter, projection=None):
    """
    Measures the execution time of a MongoDB query.

    Args:
        collection (pymongo.collection.Collection): MongoDB collection to query.
        query_filter (dict): the query filter to specify the search criteria.
        projection (dict, optional): MongoDB projection to specify the fields to return. Defaults to None 
                                     (returns all fields).
    
    Returns:
        float: the execution time of the query in seconds.
    """

    time_i = time.perf_counter()
    collection.find(query_filter, projection)
    time_f = time.perf_counter()
    return time_f - time_i


def measure_sqlite_query_time(c, query):
    """
    Measures the execution time of an SQLite query.
    
    Args:
        c (sqlite3.Cursor): SQLite cursor object for executing SQL statements.
        query (str): SQL query to execute and measure.
    
    Returns:
        float: The execution time of the query in seconds
    """

    time_i = time.perf_counter()
    c.execute(query)
    time_f = time.perf_counter()

    return time_f - time_i


def create_performance_plot(times, index_times, labels, title=None, save_path=None, file_name=None):
    """
    Creates a bar chart comparing query execution times with and without indexes.
    
    Args:
        times (list): list of execution times without indexes.
        index_times (list): list of execution times with indexes.
        labels (list): list of labels for the x-axis, where each element corresponding to the query identifier.
        title (str, optional): the title for the plot. Defaults to None.
        save_path (str, optional): path to save the plot. Defaults to None.
        filename (str, optional): name of the file for the saved plot. Defaults to None.
    """

    x = np.arange(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(11, 6))
    rects1 = ax.bar(x - width/2, times, width, label = 'No index')
    rects2 = ax.bar(x + width/2, index_times, width, label = 'Index')

    ax.set_ylabel('Time (seconds)')
    if title:
        ax.set_title(title)
    ax.set_xticks(x, labels)
    ax.legend()

    ax.bar_label(rects1, padding = 3)
    ax.bar_label(rects2, padding = 3)

    if save_path:
        os.makedirs(save_path, exist_ok=True)

    plt.savefig(os.path.join(save_path, file_name), bbox_inches='tight', dpi=300)
    plt.close(fig)