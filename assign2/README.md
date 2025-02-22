# Buffer Manager Interface
## Overview

The **Buffer Manager Interface** is designed to manage a buffer pool for an existing page file. It facilitates efficient management of pages in memory using various page replacement strategies. This interface includes data structures and functions for initializing, managing, and retrieving statistics about the buffer pool.

Video Link: https://www.loom.com/share/b600152716934fba8364cdd60a378141

## Compile and Run
### Compilation
```sh
make clean
make test1  # Compiles the first test case
make test2  # Compiles the second test case
```

### Execution
```sh
./test1       # Runs the first test case
make run_test1  # Alternative way to run test1
./test2       # Runs the second test case
make run_test2  # Alternative way to run test2
```

## Contribution Table
| CWID      | Name          | Contribution                 |
|-----------|--------------|------------------------------|
| A20577685  | Nijgururaj Ashtagi | 33.33 % |
| A20576257  | Purnendu Kale     | 33.33 % |
| A20594926 | Rayyan Maindargi | 33.33 % |

---

## Data Structures

### **DLNode (Doubly Linked List Node)**

The buffer pool is implemented using a doubly linked list where each node represents a page frame. Each node contains the following fields:

- `data`: The actual page content (`SM_PageHandle`)
- `pageNum`: The page number in the file
- `isDirty`: Boolean flag indicating if the page has been modified
- `pinCount`: The number of clients using this page
- `accessCount`: Counter for page accesses (used in **LFU**)
- `lastAccessed`: Timestamp of the last access (used in **LRU**)
- `accessHistory`: Array storing access history (used in **LRU-K**)
- `historySize`: Size of the access history array
- `next`, `prev`: Pointers to adjacent nodes in the list

---

### **BufferPoolMetadata**

This structure maintains the buffer pool's state and statistics:

- `head`, `tail`: Pointers to the first and last nodes in the buffer pool
- `fifoHead`: Tracks the oldest page for **FIFO** replacement
- `numFramesUsed`: Current number of frames in use
- `totalFrames`: Maximum number of frames allowed
- `readCount`: Number of disk reads performed
- `writeCount`: Number of disk writes performed
- `clockHand`: Current position for **CLOCK** algorithm
- `globalTimer`: Timestamp counter for access tracking

---

## Page Replacement Strategies

### **1. FIFO (First-In-First-Out)**

- Maintains a FIFO queue of pages.
- Replaces the oldest page in the buffer pool.
- Uses `fifoHead` to track the oldest page.
- Only replaces unpinned pages.

### **2. LRU (Least Recently Used)**

- Tracks page access times using `lastAccessed`.
- Replaces the page with the oldest access timestamp.
- Updates the timestamp on each page access.
- Only considers unpinned pages.

### **3. CLOCK (Second-Chance Algorithm)**

- Implements the second-chance algorithm.
- Uses `clockHand` to track the current position.
- Pages get a second chance if recently accessed.
- Performs circular traversal through the buffer pool.

### **4. LFU (Least Frequently Used)**

- Uses `accessCount` to track page usage frequency.
- Replaces the page with the lowest access count.
- Breaks ties using access timestamps.
- Only considers unpinned pages.

---

## Core Functions

### **Buffer Pool Management**

- `initBufferPool`: Initializes the buffer pool with the specified strategy.
- `shutdownBufferPool`: Cleans up and frees resources.
- `forceFlushPool`: Writes all dirty pages to disk.

### **Page Operations**

- `pinPage`: Brings a page into the buffer pool or increases the pin count.
- `unpinPage`: Decreases the pin count of a page.
- `markDirty`: Marks a page as modified.
- `forcePage`: Forces a page write to disk.

### **Statistics Functions**

- `getFrameContents`: Returns an array of page numbers in frames.
- `getDirtyFlags`: Returns an array of dirty flags for frames.
- `getFixCounts`: Returns an array of pin counts for frames.
- `getNumReadIO`: Returns the total number of disk reads.
- `getNumWriteIO`: Returns the total number of disk writes.

### **Error Handling**

- Checks for pinned pages during shutdown.
- Validates page numbers and file operations.
- Handles memory allocation failures.
- Returns appropriate error codes for various conditions.

### **Memory Management**

- Dynamically allocates page frames.
- Properly frees resources during shutdown.
- Handles page content initialization.
- Manages dirty page writes to prevent data loss.

---

## Conclusion

The buffer manager efficiently manages pages using various page replacement strategies. It includes functionality for initializing, shutting down, and flushing the buffer pool, while maintaining detailed statistics for performance monitoring and error handling.

## Output
### Test 1
![Alt text](Output/image1.png)

### Test 2
![Alt text](Output/image2.png)