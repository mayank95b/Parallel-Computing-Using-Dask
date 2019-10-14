# Parallel-Computing-Using-Dask


## Introduction

Dask is a parallel computing library that scales the existing Python ecosystem.it is popularly known as a ‘parallel computing’ python library that has been designed to run across multiple systems. Your next question would understandably be – what is parallel computing?

parallel computation is performing multiple tasks (or computations) simultaneously, using more than one resource.

- Dask can efficiently perform parallel computations on a single machine using multi-core CPUs. For example, if you have a quad core processor, Dask can effectively use all 4 cores of your system simultaneously for processing.
- In order to use lesser memory during computations, Dask stores the complete data on the disk, and uses chunks of data (smaller parts, rather than the whole data) from the disk for processing. During the processing, the intermediate values generated (if any) are discarded as soon as possible, to save the memory consumption.

## Features :

- Acts as an Drop and replacement.
- It can efficiently perform parallel computations on a single machine using multi-core CPUs.
- it stores the complete data on the disk, and uses chunks of data (smaller parts, rather than the whole data) from the disk for processing.


Dask supports the Pandas dataframe and Numpy array data structures to analyze large datasets. Basically, Dask lets you scale pandas and numpy with minimum changes in your code format. How great is that?

### Dask emphasizes the following virtues:
- **Familiar:** Provides parallelized NumPy array and Pandas DataFrame objects
- **Flexible:** Provides a task scheduling interface for more custom workloads and integration with other projects.
- **Native:** Enables distributed computing in pure Python with access to the PyData stack.
- **Fast:** Operates with low overhead, low latency, and minimal serialization necessary for fast numerical algorithms
- **Scales up:** Runs resiliently on clusters with 1000s of cores
- **Scales down:** Trivial to set up and run on a laptop in a single process
- **Responsive:** Designed with interactive computing in mind, it provides rapid feedback and diagnostics to aid humans


### User Interface

Dask supports several user interfaces:

High-Level
- Arrays: Parallel NumPy
- Bags: Parallel lists
- DataFrames: Parallel Pandas
- Machine Learning : Parallel Scikit-Learn

Low-Level
- Delayed: Parallel function evaluation
- Futures: Real-time parallel function evaluation

## Install Dask

You can install dask with conda, with pip, or by installing from source.

**Using conda**

Dask is installed in Anaconda by default. You can update it using the following command:

`conda install dask`

**Using Pip**

To install Dask using pip, simply use the below code in your command prompt/terminal window:

`pip install “dask[complete]"`

### Contents

Now, let's officially start.
