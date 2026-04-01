# ===============================================================================================================
# Question 11: Compare performance: list vs set for membership checking with 1M location IDs (benchmark and plot)
# ===============================================================================================================

import time
import random
import matplotlib.pyplot as plt


def benchmark_membership():

    # Step 1: Create 1M data
    data = list(range(1_000_000))

    # Convert to set
    data_set = set(data)

    # Random values to check
    test_values = [random.randint(0, 1_000_000) for _ in range(1000)]

    # Step 2: List membership timing
    start = time.time()

    for val in test_values:
        val in data

    list_time = time.time() - start

    # Step 3: Set membership timing
    start = time.time()

    for val in test_values:
        val in data_set

    set_time = time.time() - start

    print(f"List Time: {list_time:.6f} sec")
    print(f"Set Time: {set_time:.6f} sec")

    return list_time, set_time


def plot_results(list_time, set_time):

    labels = ['List', 'Set']
    times = [list_time, set_time]

    plt.figure()
    plt.bar(labels, times)
    plt.title("List vs Set Membership Performance")
    plt.xlabel("Data Structure")
    plt.ylabel("Time (seconds)")
    plt.show()


'''
# example usage
if __name__ == "__main__":

    list_time, set_time = benchmark_membership()
    plot_results(list_time, set_time)
'''

