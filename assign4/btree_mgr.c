#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "btree_mgr.h"
#include "tables.h"
#include "expr.h"

// Global variable to track the last accessed page
int lastPage = 0;
// Initial RID value with invalid page and slot numbers
const RID INIT_RID = {-1, -1};
// Counter for scan operations
int scanCount = 0;

// Node structure for B-tree implementation
typedef struct Node
{
    int mother;         // Parent node identifier
    bool leaf;          // Flag indicating if this is a leaf node
    RID left;           // Left child RID
    int value1;         // First key value
    RID mid;            // Middle child RID
    int value2;         // Second key value
    RID right;          // Right child RID
} Node;

// Structure to hold B-tree metadata
typedef struct TreeInfo
{
    BM_BufferPool *bm;   // Buffer pool for managing pages
    BM_PageHandle *page; // Page handle for current operations
    int root;            // Root node identifier
    int globalCount;     // Total number of entries in the tree
    int maxCount;        // Maximum number of entries per node
} TreeInfo;

// Helper functions

/**
 * Checks if the provided data type is an integer type
 * @param keyType The data type to check
 * @return RC_OK if the type is integer, otherwise error code
 */
RC checkDataType(DataType keyType)
{
    if (keyType != DT_INT)
    {
        printf("The values are not integers!!!\n");
        return RC_RM_UNKOWN_DATATYPE;
    }
    return RC_OK;
}

/**
 * Initializes a new B-tree node with the provided values
 * @param node Pointer to the node to initialize
 * @param mother Parent node identifier
 * @param isLeaf Whether this node is a leaf
 * @param leftRID Left child RID
 * @param value1 First key value
 * @param midRID Middle child RID
 * @param value2 Second key value
 * @param rightRID Right child RID
 * @return RC_OK on success, otherwise error code
 */
RC initializeNewNode(Node *node, int mother, bool isLeaf, RID leftRID, int value1, RID midRID, int value2, RID rightRID)
{
    if (node == NULL)
    {
        return RC_NULL_POINTER;
    }

    (*node).mother = mother;
    (*node).leaf = isLeaf;
    (*node).left = leftRID;
    (*node).value1 = value1;
    (*node).mid = midRID;
    (*node).value2 = value2;
    (*node).right = rightRID;

    return RC_OK;
}

/**
 * Handles page pinning operations with optional dirty marking
 * @param bm Buffer manager pool
 * @param page Page handle
 * @param pageNum Page number to pin
 * @param shouldMarkDirty Whether to mark the page as dirty
 * @return RC_OK on success, otherwise error code
 */
RC handlePagePinning(BM_BufferPool *bm, BM_PageHandle *page, int pageNum, bool shouldMarkDirty)
{
    RC pinResult = pinPage(bm, page, pageNum);
    if (pinResult != RC_OK)
    {
        return pinResult;
    }

    if (shouldMarkDirty)
    {
        RC markResult = markDirty(bm, page);
        if (markResult != RC_OK)
        {
            unpinPage(bm, page);
            return markResult;
        }
    }

    return RC_OK;
}

// ************************************** init and shutdown index manager ************************************
/**
 * Initializes the index manager
 * @param mgmtData Management data (not used in this implementation)
 * @return RC_OK always
 */
extern RC initIndexManager(void *mgmtData)
{
    return RC_OK;
}

/**
 * Shuts down the index manager
 * @return RC_OK always
 */
extern RC shutdownIndexManager()
{
    return RC_OK;
}

// ******************************** create, destroy, open, and close an btree index *******************************
/**
 * Creates a new B-tree index
 * @param idxId Index identifier (filename)
 * @param keyType Type of keys in the index
 * @param n Order of the B-tree
 * @return RC_OK on success, otherwise error code
 */
extern RC createBtree(char *idxId, DataType keyType, int n)
{
    // Verify that the key type is integer
    RC result = checkDataType(keyType);
    if (result != RC_OK)
    {
        return result;
    }

    // Create a new page file for the B-tree
    result = createPageFile(idxId);
    if (result != RC_OK)
    {
        return result;
    }

    // Open the created page file
    SM_FileHandle fh;
    result = openPageFile(idxId, &fh);
    if (result != RC_OK)
    {
        return result;
    }

    // Ensure that the file has at least one page
    result = ensureCapacity(1, &fh);
    if (result != RC_OK)
    {
        closePageFile(&fh);
        return result;
    }

    // Allocate memory for the page
    SM_PageHandle ph = malloc(PAGE_SIZE * sizeof(char));
    if (ph == NULL)
    {
        closePageFile(&fh);
        return RC_MALLOC_FAILED;
    }

    // Store the B-tree order (n) in the first page
    *((int *)ph) = n;

    // Write the page back to the file
    result = writeCurrentBlock(&fh, ph);
    free(ph);

    if (result != RC_OK)
    {
        closePageFile(&fh);
        return result;
    }

    // Close the page file
    result = closePageFile(&fh);
    return result;
}

/**
 * Opens an existing B-tree index
 * @param tree Double pointer to store the created B-tree handle
 * @param idxId Index identifier (filename)
 * @return RC_OK on success, otherwise error code
 */
extern RC openBtree(BTreeHandle **tree, char *idxId)
{
    if (tree == NULL || idxId == NULL)
    {
        return RC_NULL_POINTER;
    }

    // Create and initialize tree info structure
    TreeInfo *trInfo = malloc(sizeof(TreeInfo));
    if (trInfo == NULL)
    {
        return RC_MALLOC_FAILED;
    }

    (*trInfo).bm = MAKE_POOL();
    (*trInfo).page = MAKE_PAGE_HANDLE();
    (*trInfo).globalCount = 0;
    (*trInfo).root = 0;

    // Initialize buffer pool with 10 frames
    RC result = initBufferPool((*trInfo).bm, idxId, 10, RS_FIFO, NULL); // we assume that data would not require more than 10 pages
    if (result != RC_OK)
    {
        free((*trInfo).page);
        free((*trInfo).bm);
        free(trInfo);
        return result;
    }

    // Pin the first page to read B-tree metadata
    result = pinPage((*trInfo).bm, (*trInfo).page, 1);
    if (result != RC_OK)
    {
        shutdownBufferPool((*trInfo).bm);
        free((*trInfo).page);
        free((*trInfo).bm);
        free(trInfo);
        return result;
    }

    // Create a B-tree handle
    BTreeHandle *treeTemp = (BTreeHandle *)malloc(sizeof(BTreeHandle));
    if (treeTemp == NULL)
    {
        unpinPage((*trInfo).bm, (*trInfo).page);
        shutdownBufferPool((*trInfo).bm);
        free((*trInfo).page);
        free((*trInfo).bm);
        free(trInfo);
        return RC_MALLOC_FAILED;
    }

    // Initialize the B-tree handle
    (*treeTemp).keyType = DT_INT;
    (*trInfo).maxCount = *((int *)(*trInfo).page->data);  // Read max count from first page
    (*treeTemp).idxId = idxId;
    (*treeTemp).mgmtData = trInfo;

    *tree = treeTemp;

    // Unpin the first page
    result = unpinPage((*trInfo).bm, (*trInfo).page);
    if (result != RC_OK)
    {
        free(treeTemp);
        shutdownBufferPool((*trInfo).bm);
        free((*trInfo).page);
        free((*trInfo).bm);
        free(trInfo);
        return result;
    }

    return RC_OK;
}

/**
 * Closes a B-tree index
 * @param tree The B-tree handle to close
 * @return RC_OK on success
 */
extern RC closeBtree(BTreeHandle *tree)
{
    // Reset global variables
    lastPage = 0;
    scanCount = 0;
    
    // Free allocated memory
    free(tree);
    free(((TreeInfo *)(tree->mgmtData))->page);
    shutdownBufferPool(((TreeInfo *)(tree->mgmtData))->bm);

    return RC_OK;
}

/**
 * Deletes a B-tree index file
 * @param idxId Index identifier (filename)
 * @return RC_OK on success, otherwise error code
 */
extern RC deleteBtree(char *idxId)
{
    if (idxId == NULL)
    {
        return RC_NULL_POINTER;
    }

    // Remove the file
    int removeResult = remove(idxId);

    switch (removeResult)
    {
    case 0:
        return RC_OK;
    default:
        return RC_FILE_NOT_FOUND;
    }
}

// ************************************* access information about a b-tree *************************************
/**
 * Gets the number of nodes in the B-tree
 * @param tree The B-tree handle
 * @param result Pointer to store the result
 * @return RC_OK on success, otherwise error code
 */
extern RC getNumNodes(BTreeHandle *tree, int *result)
{
    if (tree == NULL || result == NULL)
    {
        return RC_NULL_POINTER;
    }

    // Using lastPage + 1 as the number of nodes
    *result = lastPage + 1;
    return RC_OK;
}

/**
 * Gets the number of entries in the B-tree
 * @param tree The B-tree handle
 * @param result Pointer to store the result
 * @return RC_OK on success, otherwise error code
 */
extern RC getNumEntries(BTreeHandle *tree, int *result)
{
    if (tree == NULL || result == NULL)
    {
        return RC_NULL_POINTER;
    }

    TreeInfo *trInfo = (TreeInfo *)((*tree).mgmtData);
    *result = (*trInfo).globalCount;
    return RC_OK;
}

/**
 * Gets the key type of the B-tree
 * @param tree The B-tree handle
 * @param result Pointer to store the result
 * @return RC_OK on success, otherwise error code
 */
extern RC getKeyType(BTreeHandle *tree, DataType *result)
{
    if (tree == NULL || result == NULL)
    {
        return RC_NULL_POINTER;
    }

    *result = (*tree).keyType;
    return RC_OK;
}

// ********************************************** index access *********************************************
/**
 * Finds a key in the B-tree and returns its associated RID
 * @param tree The B-tree handle
 * @param key Pointer to the key value to find
 * @param result Pointer to store the RID associated with the key
 * @return RC_OK if key is found, RC_IM_KEY_NOT_FOUND if key doesn't exist, otherwise error code
 */
extern RC findKey(BTreeHandle *tree, Value *key, RID *result)
{
    if (tree == NULL || key == NULL || result == NULL)
    {
        return RC_NULL_POINTER;
    }

    TreeInfo *trInfo = (TreeInfo *)((*tree).mgmtData);
    int index, findKey = (*key).v.intV;  // Extract integer value from key
    Node *node;
    bool find = false;
    RC rc;

    // Search through all pages in the B-tree
    int i = 1;
    for (; i <= lastPage; i += 1)
    {
        // Pin the current page for reading
        rc = handlePagePinning((*trInfo).bm, (*trInfo).page, i, false);
        if (rc != RC_OK)
        {
            return rc;
        }

        // Get node from the page data (after the boolean flag)
        node = (Node *)(*trInfo).page->data + sizeof(bool);
        int v1 = (*node).value1;
        int v2 = (*node).value2;

        // Check if the key matches the first value
        if (findKey == v1)
        {
            find = true;
            *result = (*node).left;  // Return the RID associated with first value
            break;
        }

        // Check if the key matches the second value
        if (findKey == v2)
        {
            find = true;
            *result = (*node).mid;  // Return the RID associated with second value
            break;
        }

        // Unpin the page before moving to the next
        rc = unpinPage((*trInfo).bm, (*trInfo).page);
        if (rc != RC_OK)
        {
            return rc;
        }
    }

    // If we found the key (loop terminated before reaching the end), unpin the last page
    if (i <= lastPage)
    {
        rc = unpinPage((*trInfo).bm, (*trInfo).page);
        if (rc != RC_OK)
        {
            return rc;
        }
    }

    // Return error if the key was not found
    if (find == false)
    {
        return RC_IM_KEY_NOT_FOUND;
    }

    return RC_OK;
}

/**
 * Inserts a key-RID pair into the B-tree
 * @param tree The B-tree handle
 * @param key Pointer to the key value to insert
 * @param rid RID value to associate with the key
 * @return RC_OK on success, otherwise error code
 */
extern RC insertKey(BTreeHandle *tree, Value *key, RID rid)
{
    if (tree == NULL || key == NULL)
    {
        return RC_NULL_POINTER;
    }

    TreeInfo *trInfo = (TreeInfo *)((*tree).mgmtData);
    Node *node;
    RC rc;

    switch (lastPage)
    {
    case 0:
        // Case: This is the first key being inserted into an empty tree
        lastPage = 1;
        (*trInfo).root = 1;

        // Pin the first page and mark it as dirty (will be modified)
        rc = handlePagePinning((*trInfo).bm, (*trInfo).page, lastPage, true);
        if (rc != RC_OK)
        {
            return rc;
        }

        // Set the page to be not full (false)
        *(bool *)(*trInfo).page->data = false;
        // Get pointer to node location in the page (after the boolean flag)
        node = (Node *)(*trInfo).page->data + sizeof(bool);

        // Initialize the node as a leaf with the key-RID pair in the first position
        rc = initializeNewNode(node, -1, true, rid, (*key).v.intV, INIT_RID, -1, INIT_RID);
        if (rc != RC_OK)
        {
            unpinPage((*trInfo).bm, (*trInfo).page);
            return rc;
        }

        // Unpin the page after modifications
        rc = unpinPage((*trInfo).bm, (*trInfo).page);
        if (rc != RC_OK)
        {
            return rc;
        }
        break;

    default:
        // Case: Tree already contains at least one node
        rc = handlePagePinning((*trInfo).bm, (*trInfo).page, lastPage, true);
        if (rc != RC_OK)
        {
            return rc;
        }

        if ((*(bool *)(*trInfo).page->data) == true)
        {
            // Case: The current page is full, need to create a new page
            lastPage += 1;

            // Unpin the current page
            rc = unpinPage((*trInfo).bm, (*trInfo).page);
            if (rc != RC_OK)
            {
                return rc;
            }

            // Pin the new page
            rc = handlePagePinning((*trInfo).bm, (*trInfo).page, lastPage, true);
            if (rc != RC_OK)
            {
                return rc;
            }

            // Set the new page to be not full
            *(bool *)(*trInfo).page->data = false;
            // Create a new node in the page
            node = (Node *)(*trInfo).page->data + sizeof(bool);

            // Initialize the new node with the key-RID pair in the first position
            rc = initializeNewNode(node, -1, true, rid, (*key).v.intV, INIT_RID, -1, INIT_RID);
            if (rc != RC_OK)
            {
                unpinPage((*trInfo).bm, (*trInfo).page);
                return rc;
            }

            // Unpin the page after modifications
            rc = unpinPage((*trInfo).bm, (*trInfo).page);
            if (rc != RC_OK)
            {
                return rc;
            }
        }
        else
        {
            // Case: The current page is not full, can add the key to it
            node = (Node *)(*trInfo).page->data + sizeof(bool);
            
            // Store key-RID pair in the second position
            (*node).mid = rid;
            (*node).value2 = (*key).v.intV;
            
            // Mark the page as full now
            *(bool *)(*trInfo).page->data = true;

            // Unpin the page after modifications
            rc = unpinPage((*trInfo).bm, (*trInfo).page);
            if (rc != RC_OK)
            {
                return rc;
            }
        }
        break;
    }

    // Increment the global count of entries
    (*trInfo).globalCount += 1;
    return RC_OK;
}

/**
 * Deletes a key from the B-tree
 * @param tree The B-tree handle
 * @param key Pointer to the key value to delete
 * @return RC_OK on success, RC_IM_KEY_NOT_FOUND if key doesn't exist, otherwise error code
 */
extern RC deleteKey(BTreeHandle *tree, Value *key)
{
    if (tree == NULL || key == NULL)
    {
        return RC_NULL_POINTER;
    }

    TreeInfo *trInfo = (TreeInfo *)((*tree).mgmtData);
    int index, findKey = (*key).v.intV;  // Extract integer value from key
    Node *node;
    bool find = false;
    int valueNum = 0;      // Position of the key in the node (1 or 2)
    RID moveRID;           // RID to move during reorganization
    int moveValue;         // Key value to move during reorganization
    RC rc;

    // Search through all pages to find the key
    int i = 1;
    for (; i <= lastPage; i += 1)
    {
        // Pin the current page for reading and modification
        rc = handlePagePinning((*trInfo).bm, (*trInfo).page, i, true);
        if (rc != RC_OK)
        {
            return rc;
        }

        // Get node from the page data
        node = (Node *)(*trInfo).page->data + sizeof(bool);
        int v1 = (*node).value1;
        int v2 = (*node).value2;

        // Check if key matches first or second value in node
        if (findKey == v1)
        {
            find = true;
            valueNum = 1;  // First position
            break;
        }

        if (findKey == v2)
        {
            find = true;
            valueNum = 2;  // Second position
            break;
        }

        // Unpin the page before moving to the next
        rc = unpinPage((*trInfo).bm, (*trInfo).page);
        if (rc != RC_OK)
        {
            return rc;
        }
    }

    // Return error if key not found
    if (find == false)
    {
        return RC_IM_KEY_NOT_FOUND;
    }
    else
    {
        // Key exists - pin the last page for deletion and reorganization
        rc = handlePagePinning((*trInfo).bm, (*trInfo).page, lastPage, true);
        if (rc != RC_OK)
        {
            return rc;
        }

        switch (i)
        {
        case 0:
            // This should never happen as i starts at 1
            rc = unpinPage((*trInfo).bm, (*trInfo).page);
            return RC_ERROR;
        default:
            if (i == lastPage)
            {
                // Case: Deleting a key from the last page
                node = (Node *)(*trInfo).page->data + sizeof(bool);

                switch (valueNum)
                {
                case 2:
                    // Case: Deleting the second value in the node
                    (*node).mid = INIT_RID;         // Clear the middle RID
                    (*node).value2 = -1;            // Clear the second value
                    *(bool *)(*trInfo).page->data = false;  // Mark page as not full
                    break;
                default:
                    if ((*(bool *)(*trInfo).page->data) == true)
                    {
                        // Case: Deleting the first value when the page has two values
                        // Move second value to first position
                        moveRID = (*node).mid;
                        (*node).left = moveRID;
                        moveValue = (*node).value2;
                        (*node).value1 = moveValue;
                        // Clear second position
                        (*node).mid = INIT_RID;
                        (*node).value2 = -1;
                        // Mark page as not full
                        *(bool *)(*trInfo).page->data = false;
                    }
                    else
                    {
                        // Case: Deleting the only value in the last page
                        (*node).left = INIT_RID;    // Clear the left RID
                        (*node).value1 = -1;        // Clear the first value
                        lastPage -= 1;              // Reduce page count
                    }
                    break;
                }

                // Unpin the last page after modifications
                rc = unpinPage((*trInfo).bm, (*trInfo).page);
                if (rc != RC_OK)
                {
                    return rc;
                }
            }
            else
            {
                // Case: Deleting a key not from the last page
                if ((*(bool *)(*trInfo).page->data) == true)
                {
                    // Case: Last page is full
                    // Mark last page as not full
                    *(bool *)(*trInfo).page->data = false;
                    // Get node from the page data
                    node = (Node *)(*trInfo).page->data + sizeof(bool);
                    
                    // Save the second value from last page for relocation
                    moveRID = (*node).mid;
                    moveValue = (*node).value2;

                    // Clear second position in last page
                    (*node).mid = INIT_RID;
                    (*node).value2 = -1;

                    // Unpin the last page
                    rc = unpinPage((*trInfo).bm, (*trInfo).page);
                    if (rc != RC_OK)
                    {
                        return rc;
                    }

                    // Pin the page where deletion occurs
                    rc = handlePagePinning((*trInfo).bm, (*trInfo).page, i, true);
                    if (rc != RC_OK)
                    {
                        return rc;
                    }

                    // Replace the deleted value with the saved value from last page
                    switch (valueNum)
                    {
                    case 1:
                        // Replace first value
                        node = (Node *)(*trInfo).page->data + sizeof(bool);
                        (*node).left = moveRID;
                        (*node).value1 = moveValue;
                        break;
                    default:
                        // Replace second value
                        node = (Node *)(*trInfo).page->data + sizeof(bool);
                        (*node).mid = moveRID;
                        (*node).value2 = moveValue;
                        break;
                    }

                    // Unpin the modified page
                    rc = unpinPage((*trInfo).bm, (*trInfo).page);
                    if (rc != RC_OK)
                    {
                        return rc;
                    }
                }
                else
                {
                    // Case: Last page is not full (has only one value)
                    node = (Node *)(*trInfo).page->data + sizeof(bool);
                    
                    // Save the only value from last page for relocation
                    moveRID = (*node).left;
                    moveValue = (*node).value1;
                    
                    // Clear the last page
                    (*node).left = INIT_RID;
                    (*node).value1 = -1;
                    lastPage -= 1;  // Reduce page count

                    // Unpin the last page
                    rc = unpinPage((*trInfo).bm, (*trInfo).page);
                    if (rc != RC_OK)
                    {
                        return rc;
                    }

                    // Pin the page where deletion occurs
                    rc = handlePagePinning((*trInfo).bm, (*trInfo).page, i, true);
                    if (rc != RC_OK)
                    {
                        return rc;
                    }

                    // Replace the deleted value with the saved value from last page
                    switch (valueNum)
                    {
                    case 1:
                        // Replace first value
                        node = (Node *)(*trInfo).page->data + sizeof(bool);
                        (*node).left = moveRID;
                        (*node).value1 = moveValue;
                        break;
                    default:
                        // Replace second value
                        node = (Node *)(*trInfo).page->data + sizeof(bool);
                        (*node).mid = moveRID;
                        (*node).value2 = moveValue;
                        break;
                    }

                    // Unpin the modified page
                    rc = unpinPage((*trInfo).bm, (*trInfo).page);
                    if (rc != RC_OK)
                    {
                        return rc;
                    }
                }
            }
            break;
        }

        // Decrement the global count of entries
        (*trInfo).globalCount -= 1;
    }

    return RC_OK;
}

/**
 * Opens a scan handle for traversing the B-tree in sorted order
 * @param tree The B-tree handle
 * @param handle Double pointer to store the created scan handle
 * @return RC_OK on success, otherwise error code
 */
extern RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle)
{
    if (tree == NULL || handle == NULL)
    {
        return RC_NULL_POINTER;
    }

    TreeInfo *trInfo = (TreeInfo *)((*tree).mgmtData);
    Node *node;
    int *values;          // Array to store sorted key values
    int index, i = 0, j = 0, temp1, temp2, min;
    RC rc;

    // Allocate memory for storing all key values in the tree
    values = (int *)malloc(sizeof(int) * ((*trInfo).globalCount));
    if (values == NULL)
    {
        return RC_MALLOC_FAILED;
    }

    // Collect all values from all pages in the B-tree
    int pageIndex = 1;
    for (; pageIndex <= lastPage; pageIndex += 1)
    {
        rc = pinPage((*trInfo).bm, (*trInfo).page, pageIndex);
        if (rc != RC_OK)
        {
            free(values);
            return rc;
        }

        // Get node from page data
        node = (Node *)(*trInfo).page->data + sizeof(bool);
        int v1 = (*node).value1;
        int v2 = (*node).value2;

        // Add first value if it exists
        if (v1 != -1)
        {
            values[i] = (*node).value1;
            i += 1;
        }

        // Add second value if it exists
        if (v2 != -1)
        {
            values[i] = (*node).value2;
            i += 1;
        }

        rc = unpinPage((*trInfo).bm, (*trInfo).page);
        if (rc != RC_OK)
        {
            free(values);
            return rc;
        }
    }

    // Sort the collected values using selection sort
    i = 0;
    while (i < ((*trInfo).globalCount))
    {
        min = i;  // Initialize minimum value index
        j = i + 1;

        // Find minimum element in unsorted portion
        while (j < ((*trInfo).globalCount))
        {
            if (values[min] > values[j])
            {
                min = j;
            }
            j += 1;
        }

        // Swap elements
        temp1 = values[min];
        temp2 = values[i];
        values[min] = temp2;
        values[i] = temp1;
        i += 1;
    }

    // Create and initialize the scan handle
    BT_ScanHandle *handleTemp = (BT_ScanHandle *)malloc(sizeof(BT_ScanHandle));
    if (handleTemp == NULL)
    {
        free(values);
        return RC_MALLOC_FAILED;
    }

    // Store the tree reference and sorted values in the handle
    (*handleTemp).tree = tree;
    (*handleTemp).mgmtData = values;
    *handle = handleTemp;

    // Reset the scan counter
    scanCount = 0;

    return RC_OK;
}

/**
 * Gets the next entry (key-RID pair) from the B-tree scan
 * @param handle The scan handle
 * @param result Pointer to store the RID associated with the next key
 * @return RC_OK on success, RC_IM_NO_MORE_ENTRIES when scan complete, otherwise error code
 */
extern RC nextEntry(BT_ScanHandle *handle, RID *result)
{
    if (handle == NULL || result == NULL)
    {
        return RC_NULL_POINTER;
    }

    TreeInfo *trInfo = (TreeInfo *)((*(*handle).tree).mgmtData);
    // Get the sorted array of key values
    int *values = (int *)((*handle).mgmtData);

    // Check if we've reached the end of entries
    if (scanCount >= ((*trInfo).globalCount))
    {
        return RC_IM_NO_MORE_ENTRIES;
    }

    // Create a value object for the current key
    Value *vl = (Value *)malloc(sizeof(Value));
    if (vl == NULL)
    {
        return RC_MALLOC_FAILED;
    }

    (*vl).dt = DT_INT;
    (*vl).v.intV = values[scanCount];  // Get the next value from sorted array

    // Allocate space for the RID result
    RID *rslt = (RID *)malloc(sizeof(RID));
    if (rslt == NULL)
    {
        free(vl);
        return RC_MALLOC_FAILED;
    }

    // Find the RID associated with this key value
    RC rc = findKey((*handle).tree, vl, rslt);
    free(vl);

    if (rc != RC_OK)
    {
        free(rslt);
        return rc;
    }

    // Copy the RID to the result
    *result = *rslt;
    free(rslt);

    // Increment scan counter for next call
    scanCount += 1;

    return RC_OK;
}

/**
 * Closes a B-tree scan handle and frees associated resources
 * @param handle The scan handle to close
 * @return RC_OK on success, otherwise error code
 */
extern RC closeTreeScan(BT_ScanHandle *handle)
{
    printf("We are closing tree scan!!!\n");

    if (handle == NULL)
    {
        return RC_NULL_POINTER;
    }

    // Reset the scan counter
    scanCount = 0;
    
    // Free the allocated memory
    free((*handle).mgmtData);  // Free the array of sorted values
    free(handle);              // Free the scan handle itself
    
    return RC_OK;
}

// ******************************************** debug and test functions *************************************
/**
 * Debug function to print the tree's identifier
 * @param tree The B-tree handle
 * @return The index ID of the B-tree
 */
extern char *printTree(BTreeHandle *tree)
{
    if (tree == NULL)
    {
        return NULL;
    }
    return (*tree).idxId;
}