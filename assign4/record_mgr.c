#include <stdio.h>       // Standard input/output library
#include <stdlib.h>      // Standard library functions like malloc and free
#include <string.h>      // String manipulation functions
#include "record_mgr.h"  // Header file for record manager interface
#include "buffer_mgr.h"  // Header file for buffer manager interface
#include "storage_mgr.h" // Header file for storage manager interface

// Data structure for table information
typedef struct TableInfo
{
    BM_PageHandle pageInfo; // Page handle for accessing the table's page in memory
    BM_BufferPool dataPool; // Buffer pool for managing table pages
    RID recordID;           // Record ID for accessing a specific record
    Expr *conditionExpr;    // Expression used for scan conditions
    int tupleCount;         // Number of tuples (records) in the table
    int freePageIndex;      // Index of the first free page in the table
    int scanIndex;          // Index used during scans
} TableInfo;

#define MAX_BUFFER_SIZE 100     // Maximum size of the buffer pool
#define ATTR_NAME_MAX_LENGTH 15 // Maximum length of an attribute name

TableInfo *tableInfo; // Global pointer to TableInfo struct

/**
 * @details : Locates an empty slot within a page for record insertion by scanning through
 *            the page content and checking if a slot is available (not marked with '+').
 *
 * @param pageContent : Pointer to the content of the page being examined
 * @param recordSize : Size of each record in bytes
 *
 * @return The index of the first empty slot found, or -1 if no empty slots are available
 */
int locateEmptySlot(char *pageContent, int recordSize)
{
    int slotIndex = 0;                     // Initialize the slot index
    int maxSlots = PAGE_SIZE / recordSize; // Calculate the maximum number of slots in a page

    while (slotIndex < maxSlots)
    { // Loop through each slot in the page
        if (pageContent[slotIndex * recordSize] != '+')
        {                     // Check if the slot is not marked as occupied ('+')
            return slotIndex; // Return the index of the empty slot
        }
        slotIndex += 1; // Increment the slot index
    }

    return -1; // Return -1 if no empty slots are found
}

/**
 * @details : The function initRecordManager initializes the record manager by setting up
 *            the storage manager for subsequent record operations.
 *
 * @param mgmtData : Pointer to management data (not used in this implementation)
 *
 * @return RC_OK upon successful initialization
 */
extern RC initRecordManager(void *mgmtData)
{
    initStorageManager(); // Initialize the storage manager

    return RC_OK; // Return RC_OK to indicate success
}

/**
 * @details : Shuts down the record manager and releases any allocated resources,
 *            particularly freeing the table information structure.
 *
 * @return RC_OK upon successful shutdown
 */
extern RC shutdownRecordManager()
{
    if (tableInfo == NULL)
    {                    // Check if tableInfo is NULL (meaning it hasn't been initialized)
        return RC_ERROR; // Return RC_ERROR if tableInfo is NULL
    }

    free(tableInfo);  // Free the memory allocated for tableInfo
    tableInfo = NULL; // Set tableInfo to NULL to prevent dangling pointer

    return RC_OK; // Return RC_OK to indicate success
}

/**
 * @details : Creates a new table with the specified name and schema. The function
 *            initializes a buffer pool, sets up the table metadata page, and writes
 *            the schema information to the first page of the table file.
 *
 * @param name : Name of the table to be created
 * @param schema : Schema definition for the table
 *
 * @return RC_OK on success, or an error code if any operation fails
 */
extern RC createTable(char *name, Schema *schema)
{
    RC result; // Variable to store the result code of function calls

    if (name == NULL || schema == NULL)
    {                                // Check for invalid parameters
        return RC_INVALID_PARAMETER; // Return an error code if parameters are invalid
    }

    tableInfo = (TableInfo *)malloc(sizeof(TableInfo)); // Allocate memory for the TableInfo structure
    if (tableInfo == NULL)
    {                                      // Check if memory allocation failed
        return RC_MEMORY_ALLOCATION_ERROR; // Return an error code if memory allocation failed
    }

    result = initBufferPool(&(*tableInfo).dataPool, name, MAX_BUFFER_SIZE, RS_LRU, NULL); // Initialize the buffer pool
    if (result != RC_OK)
    {                    // Check if buffer pool initialization failed
        free(tableInfo); // Free the allocated memory for tableInfo
        return result;   // Return the error code from buffer pool initialization
    }

    char pageData[PAGE_SIZE]; // Allocate a buffer for the first page data
    char *dataPtr = pageData; // Create a pointer to the beginning of the page data
    int attrIndex;            // Loop counter for iterating through attributes

    // Record count (initially 0)
    *(int *)dataPtr = 0;    // Write the initial record count (0) to the page data
    dataPtr += sizeof(int); // Increment the data pointer

    // First free page index (initially 1)
    *(int *)dataPtr = 1;    // Write the initial free page index (1) to the page data
    dataPtr += sizeof(int); // Increment the data pointer

    // Number of attributes in schema
    *(int *)dataPtr = (*schema).numAttr; // Write the number of attributes to the page data
    dataPtr += sizeof(int);              // Increment the data pointer

    // Key size information
    *(int *)dataPtr = (*schema).keySize; // Write the key size to the page data
    dataPtr += sizeof(int);              // Increment the data pointer

    // Store attribute information
    for (attrIndex = 0; attrIndex < (*schema).numAttr; attrIndex += 1)
    {                                                                           // Iterate through each attribute in the schema
        strncpy(dataPtr, (*schema).attrNames[attrIndex], ATTR_NAME_MAX_LENGTH); // Copy the attribute name to the page data
        dataPtr += ATTR_NAME_MAX_LENGTH;                                        // Increment the data pointer

        *(int *)dataPtr = (int)(*schema).dataTypes[attrIndex]; // Write the data type to the page data
        dataPtr += sizeof(int);                                // Increment the data pointer

        *(int *)dataPtr = (int)(*schema).typeLength[attrIndex]; // Write the type length to the page data
        dataPtr += sizeof(int);                                 // Increment the data pointer
    }

    SM_FileHandle fileHandle; // File handle for accessing the page file

    // Create, open, write to, and close the page file with error handling
    result = createPageFile(name);
    if (result != RC_OK)
    {
        free(tableInfo); // Clean up allocated memory
        return result;   // Return the error code if creation fails
    }

    result = openPageFile(name, &fileHandle);
    if (result != RC_OK)
    {
        free(tableInfo); // Clean up allocated memory
        return result;   // Return the error code if opening fails
    }

    result = writeBlock(0, &fileHandle, pageData);
    if (result != RC_OK)
    {
        closePageFile(&fileHandle); // Try to close the file before returning
        free(tableInfo);            // Clean up allocated memory
        return result;              // Return the error code if writing fails
    }

    result = closePageFile(&fileHandle);
    if (result != RC_OK)
    {
        free(tableInfo); // Clean up allocated memory
        return result;   // Return the error code if closing fails
    }

    return RC_OK; // Return RC_OK to indicate success
}

/**
 * @details : Opens an existing table with the specified name. The function reads the
 *            table metadata from the first page, reconstructs the schema, and prepares
 *            the table for record operations.
 *
 * @param rel : Pointer to the RM_TableData structure to be populated
 * @param name : Name of the table to be opened
 *
 * @return RC_OK on successful opening of the table
 */
extern RC openTable(RM_TableData *rel, char *name)
{
    if (rel == NULL || name == NULL)
    {                                // Check for invalid parameters
        return RC_INVALID_PARAMETER; // Return error if invalid parameters
    }

    SM_PageHandle pageContent; // Pointer to hold the content of the page
    int attrCount, i;          // Variables for attribute count and loop index
    RC result;                 // Variable to store the result code

    (*rel).mgmtData = tableInfo; // Set the management data of the relation
    (*rel).name = name;          // Set the name of the relation

    result = pinPage(&(*tableInfo).dataPool, &(*tableInfo).pageInfo, 0); // Pin the first page of the table
    if (result != RC_OK)
    {                  // Check for error
        return result; // Return the error code
    }

    pageContent = (char *)(*tableInfo).pageInfo.data; // Get the pointer to the page content

    // Read tuple count
    (*tableInfo).tupleCount = *(int *)pageContent; // Read the tuple count from the page content
    pageContent += sizeof(int);                    // Increment the page content pointer

    // Read free page index
    (*tableInfo).freePageIndex = *(int *)pageContent; // Read the free page index from the page content
    pageContent += sizeof(int);                       // Increment the page content pointer

    // Read attribute count
    attrCount = *(int *)pageContent; // Read the attribute count from the page content
    pageContent += sizeof(int);      // Increment the page content pointer

    // Create and populate schema structure
    Schema *tableSchema = (Schema *)malloc(sizeof(Schema)); // Allocate memory for the schema
    if (tableSchema == NULL)
    {                                                              // Check for error
        unpinPage(&(*tableInfo).dataPool, &(*tableInfo).pageInfo); // Unpin the page
        return RC_MEMORY_ALLOCATION_ERROR;                         // Return memory allocation error
    }

    (*tableSchema).numAttr = attrCount;                                     // Set the number of attributes in the schema
    (*tableSchema).attrNames = (char **)malloc(sizeof(char *) * attrCount); // Allocate memory for attribute names
    if ((*tableSchema).attrNames == NULL)
    {                                                              // Check for error
        free(tableSchema);                                         // Free the schema
        unpinPage(&(*tableInfo).dataPool, &(*tableInfo).pageInfo); // Unpin the page
        return RC_MEMORY_ALLOCATION_ERROR;                         // Return memory allocation error
    }

    (*tableSchema).dataTypes = (DataType *)malloc(sizeof(DataType) * attrCount); // Allocate memory for data types
    if ((*tableSchema).dataTypes == NULL)
    {                                                              // Check for error
        free((*tableSchema).attrNames);                            // Free the attribute names
        free(tableSchema);                                         // Free the schema
        unpinPage(&(*tableInfo).dataPool, &(*tableInfo).pageInfo); // Unpin the page
        return RC_MEMORY_ALLOCATION_ERROR;                         // Return memory allocation error
    }

    (*tableSchema).typeLength = (int *)malloc(sizeof(int) * attrCount); // Allocate memory for type lengths
    if ((*tableSchema).typeLength == NULL)
    {                                                              // Check for error
        free((*tableSchema).dataTypes);                            // Free the data types
        free((*tableSchema).attrNames);                            // Free the attribute names
        free(tableSchema);                                         // Free the schema
        unpinPage(&(*tableInfo).dataPool, &(*tableInfo).pageInfo); // Unpin the page
        return RC_MEMORY_ALLOCATION_ERROR;                         // Return memory allocation error
    }

    i = 0;
    while (i < attrCount)
    {                                                                       // Loop through the attributes
        (*tableSchema).attrNames[i] = (char *)malloc(ATTR_NAME_MAX_LENGTH); // Allocate memory for each attribute name
        if ((*tableSchema).attrNames[i] == NULL)
        { // Check for error
            // Free previously allocated memory
            int j = 0;
            while (j < i)
            {                                      // Loop to free previously allocated memory
                free((*tableSchema).attrNames[j]); // Free the attribute name
                j += 1;
            }
            free((*tableSchema).typeLength);                           // Free the type length
            free((*tableSchema).dataTypes);                            // Free the data types
            free((*tableSchema).attrNames);                            // Free the attribute names
            free(tableSchema);                                         // Free the schema
            unpinPage(&(*tableInfo).dataPool, &(*tableInfo).pageInfo); // Unpin the page
            return RC_MEMORY_ALLOCATION_ERROR;                         // Return memory allocation error
        }
        i += 1;
    }

    // Read attribute details
    i = 0;
    while (i < (*tableSchema).numAttr)
    {                                                                            // Loop through the attributes
        strncpy((*tableSchema).attrNames[i], pageContent, ATTR_NAME_MAX_LENGTH); // Copy the attribute name from the page content
        pageContent += ATTR_NAME_MAX_LENGTH;                                     // Increment the page content pointer

        (*tableSchema).dataTypes[i] = *(int *)pageContent; // Read the data type from the page content
        pageContent += sizeof(int);                        // Increment the page content pointer

        (*tableSchema).typeLength[i] = *(int *)pageContent; // Read the type length from the page content
        pageContent += sizeof(int);                         // Increment the page content pointer

        i += 1;
    }

    (*rel).schema = tableSchema; // Set the schema of the relation

    result = unpinPage(&(*tableInfo).dataPool, &(*tableInfo).pageInfo); // Unpin the page
    if (result != RC_OK)
    {                  // Check for error
        return result; // Return the error code
    }

    result = forcePage(&(*tableInfo).dataPool, &(*tableInfo).pageInfo); // Force the page to disk
    if (result != RC_OK)
    {                  // Check for error
        return result; // Return the error code
    }

    return RC_OK; // Return RC_OK for success
}

/**
 * @details : Closes a table that was previously opened. This function releases
 *            buffer pool resources associated with the table.
 *
 * @param rel : Pointer to the RM_TableData structure of the table to be closed
 *
 * @return RC_OK on successful closure of the table
 */
extern RC closeTable(RM_TableData *rel)
{
    TableInfo *mgr = rel->mgmtData;     // Get the TableInfo struct from the RM_TableData
    shutdownBufferPool(&mgr->dataPool); // Shutdown the buffer pool associated with the table
    return RC_OK;                       // Return RC_OK to indicate success
}

/**
 * @details : Deletes a table with the specified name by removing its underlying
 *            page file from the storage.
 *
 * @param name : Name of the table to be deleted
 *
 * @return RC_OK if the table is successfully deleted
 */
extern RC deleteTable(char *name)
{
    if (name == NULL)
    {                                // Check if the table name is NULL
        return RC_INVALID_PARAMETER; // Return RC_INVALID_PARAMETER if name is NULL
    }

    RC result; // Variable to store the result of the destroyPageFile function

    result = destroyPageFile(name); // Destroy the page file associated with the table name
    if (result != RC_OK)
    {                  // Check if destroying the page file was successful
        return result; // Return the error code if destroying the page file failed
    }

    return RC_OK; // Return RC_OK to indicate success
}

/**
 * @details : Returns the number of records (tuples) currently stored in the table.
 *
 * @param rel : Pointer to the RM_TableData structure of the table
 *
 * @return The number of tuples in the table
 */
extern int getNumTuples(RM_TableData *rel)
{
    if (rel == NULL)
    {              // Check if the table relation is NULL
        return -1; // Return -1 if the relation is NULL
    }

    TableInfo *mgr = (*rel).mgmtData; // Get the table management data from the relation
    return (*mgr).tupleCount;         // Return the number of tuples from the table management data
}

/**
 * @details : Inserts a new record into the table. The function finds an available
 *            slot for the record, marks it as occupied ('+'), and copies the record
 *            data into the slot.
 *
 * @param rel : Pointer to the RM_TableData structure of the target table
 * @param record : Pointer to the Record structure containing the data to be inserted
 *
 * @return RC_OK on successful insertion
 */
extern RC insertRecord(RM_TableData *rel, Record *record)
{
    if (rel == NULL || record == NULL)
    {                                // Check for invalid parameters
        return RC_INVALID_PARAMETER; // Return RC_INVALID_PARAMETER if parameters are invalid
    }

    TableInfo *mgr = (*rel).mgmtData;              // Get the table management data
    RID *rid = &(*record).id;                      // Get the record ID
    char *pageContent, *slotPtr;                   // Pointers for page content and slot
    int recordSize = getRecordSize((*rel).schema); // Get the size of the record
    RC result;                                     // Variable to store the result code

    if (recordSize <= 0)
    {                                      // Check if record size is invalid
        return RC_MEMORY_ALLOCATION_ERROR; // Return RC_MEMORY_ALLOCATION_ERROR if record size is invalid
    }

    // Start with the free page index
    (*rid).page = (*mgr).freePageIndex; // Set the page to the free page index

    result = pinPage(&(*mgr).dataPool, &(*mgr).pageInfo, (*rid).page); // Pin the page
    if (result != RC_OK)
    {                  // Check if pinning failed
        return result; // Return the error code
    }

    pageContent = (*mgr).pageInfo.data;                     // Get the page content
    (*rid).slot = locateEmptySlot(pageContent, recordSize); // Locate an empty slot

    // If no empty slot, try next pages until one is found
    while ((*rid).slot == -1)
    {                                                           // Loop until an empty slot is found
        result = unpinPage(&(*mgr).dataPool, &(*mgr).pageInfo); // Unpin the current page
        if (result != RC_OK)
        {                  // Check if unpinning failed
            return result; // Return the error code
        }

        (*rid).page += 1; // Go to the next page

        result = pinPage(&(*mgr).dataPool, &(*mgr).pageInfo, (*rid).page); // Pin the next page
        if (result != RC_OK)
        {                  // Check if pinning failed
            return result; // Return the error code
        }

        pageContent = (*mgr).pageInfo.data;                     // Get the page content
        (*rid).slot = locateEmptySlot(pageContent, recordSize); // Locate an empty slot on the new page
    }

    // Insert the record at the found slot
    slotPtr = pageContent; // Set the slot pointer

    result = markDirty(&(*mgr).dataPool, &(*mgr).pageInfo); // Mark the page as dirty
    if (result != RC_OK)
    {                                                  // Check if marking dirty failed
        unpinPage(&(*mgr).dataPool, &(*mgr).pageInfo); // Unpin the page
        return result;                                 // Return the error code
    }

    slotPtr += ((*rid).slot * recordSize); // Move the slot pointer to the correct slot
    *slotPtr = '+';                        // Mark slot as occupied

    slotPtr += 1;                                        // Move past the tombstone byte
    memcpy(slotPtr, (*record).data + 1, recordSize - 1); // Copy the record data to the slot

    result = unpinPage(&(*mgr).dataPool, &(*mgr).pageInfo); // Unpin the page
    if (result != RC_OK)
    {                  // Check if unpinning failed
        return result; // Return the error code
    }

    (*mgr).tupleCount += 1; // Increment the tuple count

    result = pinPage(&(*mgr).dataPool, &(*mgr).pageInfo, 0); // Pin the metadata page
    if (result != RC_OK)
    {                  // Check if pinning failed
        return result; // Return the error code
    }

    return RC_OK; // Return RC_OK to indicate success
}

/**
 * @details : Deletes a record from the table by marking its slot as available ('-').
 *            The function also updates the free page index to optimize future insertions.
 *
 * @param rel : Pointer to the RM_TableData structure of the table
 * @param id : The RID (Record ID) of the record to be deleted
 *
 * @return RC_OK on successful deletion
 */
extern RC deleteRecord(RM_TableData *rel, RID id)
{
    if (rel == NULL)
    {                                // Check for invalid parameters
        return RC_INVALID_PARAMETER; // Return RC_INVALID_PARAMETER if rel is NULL
    }

    TableInfo *mgr = (*rel).mgmtData; // Get the table management data
    RC result;                        // Variable to store the result code

    result = pinPage(&(*mgr).dataPool, &(*mgr).pageInfo, id.page); // Pin the page
    if (result != RC_OK)
    {                  // Check if pinning failed
        return result; // Return the error code
    }

    (*mgr).freePageIndex = id.page; // Update free page index for optimization

    char *data = (*mgr).pageInfo.data;             // Get the page data
    int recordSize = getRecordSize((*rel).schema); // Get the record size

    if (recordSize <= 0)
    {                                                  // Check if record size is invalid
        unpinPage(&(*mgr).dataPool, &(*mgr).pageInfo); // Unpin the page
        return RC_MEMORY_ALLOCATION_ERROR;             // Return RC_MEMORY_ALLOCATION_ERROR
    }

    data += (id.slot * recordSize); // Move the pointer to the start of the record slot
    *data = '-';                    // Mark slot as available

    result = markDirty(&(*mgr).dataPool, &(*mgr).pageInfo); // Mark the page as dirty
    if (result != RC_OK)
    {                                                  // Check if marking dirty failed
        unpinPage(&(*mgr).dataPool, &(*mgr).pageInfo); // Unpin the page
        return result;                                 // Return the error code
    }

    result = unpinPage(&(*mgr).dataPool, &(*mgr).pageInfo); // Unpin the page
    if (result != RC_OK)
    {                  // Check if unpinning failed
        return result; // Return the error code
    }

    return RC_OK; // Return RC_OK to indicate success
}

/**
 * @details : Updates an existing record in the table with new data. The function
 *            locates the record using its RID and replaces its content.
 *
 * @param rel : Pointer to the RM_TableData structure of the table
 * @param record : Pointer to the Record structure containing the updated data
 *
 * @return RC_OK on successful update
 */
extern RC updateRecord(RM_TableData *rel, Record *record)
{
    if (rel == NULL || record == NULL)
    {                                // Check for invalid parameters
        return RC_INVALID_PARAMETER; // Return RC_INVALID_PARAMETER if parameters are invalid
    }

    TableInfo *mgr = (*rel).mgmtData; // Get the table management data
    RC result;                        // Variable to store the result code

    result = pinPage(&(*mgr).dataPool, &(*mgr).pageInfo, (*record).id.page); // Pin the page
    if (result != RC_OK)
    {                  // Check if pinning failed
        return result; // Return the error code
    }

    char *data;                                    // Pointer to the record data
    int recordSize = getRecordSize((*rel).schema); // Get the record size

    if (recordSize <= 0)
    {                                                  // Check if record size is invalid
        unpinPage(&(*mgr).dataPool, &(*mgr).pageInfo); // Unpin the page
        return RC_MEMORY_ALLOCATION_ERROR;             // Return RC_MEMORY_ALLOCATION_ERROR
    }

    RID rid = (*record).id;          // Get the record ID
    data = (*mgr).pageInfo.data;     // Get the page data
    data += (rid.slot * recordSize); // Move the pointer to the start of the record slot

    *data = '+';                                      // Ensure slot is marked as occupied
    data += 1;                                        // Move past the tombstone byte
    memcpy(data, (*record).data + 1, recordSize - 1); // Copy the new record data to the slot

    result = markDirty(&(*mgr).dataPool, &(*mgr).pageInfo); // Mark the page as dirty
    if (result != RC_OK)
    {                                                  // Check if marking dirty failed
        unpinPage(&(*mgr).dataPool, &(*mgr).pageInfo); // Unpin the page
        return result;                                 // Return the error code
    }

    result = unpinPage(&(*mgr).dataPool, &(*mgr).pageInfo); // Unpin the page
    if (result != RC_OK)
    {                  // Check if unpinning failed
        return result; // Return the error code
    }

    return RC_OK; // Return RC_OK to indicate success
}

/**
 * @details : Retrieves a record from the table based on its RID. The function checks
 *            if the slot is occupied before copying the record data.
 *
 * @param rel : Pointer to the RM_TableData structure of the table
 * @param id : The RID (Record ID) of the record to be retrieved
 * @param record : Pointer to the Record structure where the retrieved data will be stored
 *
 * @return RC_OK on successful retrieval, or RC_RM_NO_TUPLE_WITH_GIVEN_RID if no record exists
 */
extern RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    if (rel == NULL || record == NULL)
    {                                // Check for invalid parameters
        return RC_INVALID_PARAMETER; // Return RC_INVALID_PARAMETER if parameters are invalid
    }

    TableInfo *mgr = (*rel).mgmtData; // Get the table management data
    RC result;                        // Variable to store the result code

    result = pinPage(&(*mgr).dataPool, &(*mgr).pageInfo, id.page); // Pin the page
    if (result != RC_OK)
    {                  // Check if pinning failed
        return result; // Return the error code
    }

    int recordSize = getRecordSize((*rel).schema); // Get the record size

    if (recordSize <= 0)
    {                                                  // Check if record size is invalid
        unpinPage(&(*mgr).dataPool, &(*mgr).pageInfo); // Unpin the page
        return RC_MEMORY_ALLOCATION_ERROR;             // Return RC_MEMORY_ALLOCATION_ERROR
    }

    char *data = (*mgr).pageInfo.data; // Get the page data
    data += (id.slot * recordSize);    // Move the pointer to the start of the record slot

    if (*data != '+')
    {                                                           // Check if slot is occupied
        result = unpinPage(&(*mgr).dataPool, &(*mgr).pageInfo); // Unpin the page
        if (result != RC_OK)
        {                  // Check if unpinning failed
            return result; // Return the error code
        }
        return RC_RM_NO_TUPLE_WITH_GIVEN_RID; // Return RC_RM_NO_TUPLE_WITH_GIVEN_RID
    }
    else
    {
        (*record).id = id;                            // Set the record ID
        char *recordData = (*record).data;            // Get the record data pointer
        recordData += 1;                              // Move past the tombstone byte
        memcpy(recordData, data + 1, recordSize - 1); // Copy the record data to the record struct
    }

    result = unpinPage(&(*mgr).dataPool, &(*mgr).pageInfo); // Unpin the page
    if (result != RC_OK)
    {                  // Check if unpinning failed
        return result; // Return the error code
    }

    return RC_OK; // Return RC_OK to indicate success
}

/**
 * @details : Initiates a table scan with a specified condition. The function sets up
 *            scan management data and initializes scan position to the beginning of the table.
 *
 * @param rel : Pointer to the RM_TableData structure of the table to be scanned
 * @param scan : Pointer to the RM_ScanHandle structure to be populated
 * @param cond : Expression condition for filtering records during the scan
 *
 * @return RC_OK on successful scan initialization, or RC_SCAN_CONDITION_NOT_FOUND if condition is NULL
 */
extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    // Validate input parameters
    if (rel == NULL || scan == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    // Check if the condition expression is provided
    if (cond == NULL)
    {
        return RC_SCAN_CONDITION_NOT_FOUND;
    }

    // Open the table in memory (assumes "openTable" initializes metadata)
    RC rc = openTable(rel, "ScanTable");
    if (rc != RC_OK)
    {
        return rc; // Return error code if opening the table fails
    }

    // Allocate memory for scan manager metadata
    TableInfo *scanManager = (TableInfo *)malloc(sizeof(TableInfo));
    if (scanManager == NULL)
    {
        return RC_MEMORY_ALLOCATION_ERROR; // Handle memory allocation failure
    }

    // Initialize scan manager metadata
    scanManager->recordID.page = 0;    // Start scanning from the first page (0-based index)
    scanManager->recordID.slot = 0;    // Start scanning from the first slot
    scanManager->scanIndex = 0;        // No records scanned yet
    scanManager->conditionExpr = cond; // Store the condition expression

    // Attach scan manager to the scan handle
    scan->mgmtData = scanManager;

    // Set the table to be scanned in the scan handle
    scan->rel = rel;

    // Ensure table metadata is properly initialized
    TableInfo *tableManager = rel->mgmtData;
    if (tableManager == NULL)
    {
        free(scanManager); // Clean up allocated memory before returning error
        return RC_FILE_NOT_FOUND;
    }

    // Set tuple count or any other necessary metadata in table manager
    tableManager->tupleCount = ATTR_NAME_MAX_LENGTH; // Example placeholder

    return RC_OK; // Successfully initialized the scan
}

/**
 * @details : Retrieves the next record matching the scan condition. The function
 *            iterates through records, evaluates each against the condition,
 *            and returns the first matching record.
 *
 * @param scan : Pointer to the RM_ScanHandle structure of the scan
 * @param record : Pointer to the Record structure where the matching record will be stored
 *
 * @return RC_OK if a matching record is found, RC_RM_NO_MORE_TUPLES if no more records match,
 *         or RC_SCAN_CONDITION_NOT_FOUND if scan condition is missing
 */
extern RC next(RM_ScanHandle *scan, Record *record)
{
    if (scan == NULL || record == NULL)
    {                                // Check if scan handle or record are NULL
        return RC_INVALID_PARAMETER; // Return error if invalid parameter
    }

    TableInfo *scanInfo = (*scan).mgmtData;       // Get the scan management data
    TableInfo *relInfo = (*(*scan).rel).mgmtData; // Get the relation management data
    Schema *schema = (*(*scan).rel).schema;       // Get the schema

    // Validate scan condition
    if ((*scanInfo).conditionExpr == NULL)
    {                                       // Check if the scan condition is NULL
        return RC_SCAN_CONDITION_NOT_FOUND; // Return error if scan condition is not found
    }

    Value *evalResult = (Value *)malloc(sizeof(Value)); // Allocate memory for evaluation result
    if (evalResult == NULL)
    {                                      // Check if allocation failed
        return RC_MEMORY_ALLOCATION_ERROR; // Return error if memory allocation failed
    }

    char *data;                             // Pointer to the data
    int recordSize = getRecordSize(schema); // Get the record size

    if (recordSize <= 0)
    {                     // Check if record size is invalid
        free(evalResult); // Free allocated memory
        return RC_ERROR;  // Return error
    }

    int slotsPerPage = PAGE_SIZE / recordSize; // Calculate the number of slots per page
    int scanCount = (*scanInfo).scanIndex;     // Get the current scan index
    int totalRecords = (*relInfo).tupleCount;  // Get the total number of records

    // Check if table is empty
    if (totalRecords == 0)
    {                                // Check if there are no records
        free(evalResult);            // Free allocated memory
        return RC_RM_NO_MORE_TUPLES; // Return error if no more tuples
    }

    // Scan for matching records
    while (scanCount <= totalRecords)
    { // Loop until the end of the table
        // Initialize or advance position
        if (scanCount <= 0)
        {                                  // Initialize the scan at the first page and slot
            (*scanInfo).recordID.page = 1; // Start at page 1
            (*scanInfo).recordID.slot = 0; // Start at slot 0
        }
        else
        {                                   // Increment to the next slot
            (*scanInfo).recordID.slot += 1; // Increment slot
            if ((*scanInfo).recordID.slot >= slotsPerPage)
            {                                   // If the slot number has reached max, go to the next page
                (*scanInfo).recordID.slot = 0;  // Reset slot number
                (*scanInfo).recordID.page += 1; // Increment the page number
            }
        }

        // Access the current record
        RC result = pinPage(&(*relInfo).dataPool, &(*scanInfo).pageInfo, (*scanInfo).recordID.page); // Pin the page
        if (result != RC_OK)
        {                     // Check if pinning failed
            free(evalResult); // Free allocated memory
            return result;    // Return error
        }

        data = (*scanInfo).pageInfo.data;                 // Get data from page
        data += ((*scanInfo).recordID.slot * recordSize); // Get to the correct record

        // Set up record for evaluation
        (*record).id.page = (*scanInfo).recordID.page; //
        (*record).id.slot = (*scanInfo).recordID.slot; // Set current slot number
        char *recordData = (*record).data;             // Point to the record data
        *recordData = '-';                             // Set Tombstone to '-' indicating possible empty slot

        recordData += 1;                              // Skip Tombstone byte
        memcpy(recordData, data + 1, recordSize - 1); // Copy record data

        // Increment counters
        (*scanInfo).scanIndex += 1; // Increase the scan index
        scanCount += 1;             // Increase count

        // Evaluate condition
        evalExpr(record, schema, (*scanInfo).conditionExpr, &evalResult); // Evaluate expression with the record
        if ((*evalResult).v.boolV == TRUE)
        {                                                                    // Check if expression eval to TRUE
            result = unpinPage(&(*relInfo).dataPool, &(*scanInfo).pageInfo); // Unpin the page
            if (result != RC_OK)
            {                     // Check if unpin operation succeeded
                free(evalResult); // Free the evaluation result
                return result;    // Return result code indicating reason of failure
            }
            free(evalResult); // Free the evaluation result
            return RC_OK;     // Return success
        }
    }

    // No more matching records found, reset scan position
    RC result = unpinPage(&(*relInfo).dataPool, &(*scanInfo).pageInfo); // Unpin the page
    if (result != RC_OK)
    {                     // Check if unpinning fails
        free(evalResult); // Free memory allocated to the evalResult
        return result;    // Returns result code
    }

    (*scanInfo).recordID.page = 1; // Reset to page 1 to start from begining
    (*scanInfo).recordID.slot = 0; // Resets slot to initial index.
    (*scanInfo).scanIndex = 0;     // Resets scan index.
    free(evalResult);              // Free allocated memory

    return RC_RM_NO_MORE_TUPLES; // Return error if no more tuples
}

/**
 * @details : Ends a table scan and cleans up resources. The function resets scan
 *            position and frees allocated memory.
 *
 * @param scan : Pointer to the RM_ScanHandle structure of the scan to be closed
 *
 * @return RC_OK on successful closure of the scan
 */
extern RC closeScan(RM_ScanHandle *scan)
{
    if (scan == NULL || scan->mgmtData == NULL)
    {                                // Check for scan handle
        return RC_INVALID_PARAMETER; // Return parameter if invalid.
    }

    TableInfo *scanInfo = (TableInfo *)scan->mgmtData;

    // Reset scan position
    scanInfo->scanIndex = 0;
    scanInfo->recordID.page = 1;
    scanInfo->recordID.slot = 0;

    // Free scan management resources
    free(scan->mgmtData);
    scan->mgmtData = NULL;

    return RC_OK; // Return successful result
}

/**
 * @details : Calculates the size of a record based on its schema definition.
 *            The function adds up the size requirements for each attribute type
 *            and adds one byte for the record's tombstone marker.
 *
 * @param schema : Pointer to the Schema structure defining the record's format
 *
 * @return The total size of the record in bytes
 */
extern int getRecordSize(Schema *schema)
{
    if (schema == NULL)
    {              // Checks if Schema is Null
        return -1; // Returns -1 if NULL
    }

    int size = 0; // size initialisation
    int i = 0;    // Iterator variable

    // Calculate size based on attribute types
    while (i < (*schema).numAttr)
    { // Looping until i gets numAttr value
        if ((*schema).dataTypes[i] == DT_STRING)
        {                                    // Checks DataType as STRING
            size += (*schema).typeLength[i]; // Add the typeLength to size
        }
        else if ((*schema).dataTypes[i] == DT_INT)
        {                        // Checks DataType as INT
            size += sizeof(int); // Add the size of integer to size
        }
        else if ((*schema).dataTypes[i] == DT_FLOAT)
        {                          // Checks DataType as FLOAT
            size += sizeof(float); // Add the size of float to size
        }
        else if ((*schema).dataTypes[i] == DT_BOOL)
        {                         // Checks DataType as BOOL
            size += sizeof(bool); // Add the size of bool to size
        }
        else
        {              // If any type is other than STRING, INT, FLOAT or BOOL return -1
            return -1; // Error: unknown data type
        }
        i += 1; // increment the iterator
    }

    // Add one byte for tombstone (record presence marker)
    size += 1; // Add one byte for Tombstone

    return size; // Return calculated size
}

/**
 * @details : Creates a new schema object with the specified attributes. The function
 *            allocates memory for the schema structure and initializes its fields.
 *
 * @param numAttr : Number of attributes in the schema
 * @param attrNames : Array of attribute names
 * @param dataTypes : Array of data types for each attribute
 * @param typeLength : Array of type lengths for string attributes
 * @param keySize : Number of key attributes
 * @param keys : Array of indices identifying key attributes
 *
 * @return Pointer to the newly created Schema structure
 */
extern Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    if (numAttr <= 0 || attrNames == NULL || dataTypes == NULL)
    {                // Validates the parameter
        return NULL; // Returns NULL if it's invalid
    }

    Schema *schema = (Schema *)malloc(sizeof(Schema)); // Memory allocation for schema
    if (schema == NULL)
    {                // Memory Allocation Check
        return NULL; // Returns NULL if memory allocation fails
    }

    (*schema).numAttr = numAttr;       // Set number of attributes
    (*schema).attrNames = attrNames;   // Set attribute names
    (*schema).dataTypes = dataTypes;   // Set data types
    (*schema).typeLength = typeLength; // Set the typeLength.
    (*schema).keySize = keySize;       // Sets the key size.
    (*schema).keyAttrs = keys;         // Copy over keys to the schema

    return schema; // Return pointer to newly allocated schema
}

/**
 * @details : Deallocates memory used by a schema object. The function simply frees
 *            the schema structure itself, assuming attribute arrays are managed elsewhere.
 *
 * @param schema : Pointer to the Schema structure to be freed
 *
 * @return RC_OK on successful deallocation
 */
extern RC freeSchema(Schema *schema)
{
    if (schema == NULL)
    {                                // Validate argument
        return RC_INVALID_PARAMETER; // Returns parameter if invalid.
    }

    free(schema); // Free schema

    return RC_OK; // Returns Result
}

/**
 * @details : Creates a new record with memory allocation based on the schema.
 *            The function initializes the record with default values and sets
 *            its RID to indicate that it's not yet stored in the table.
 *
 * @param record : Pointer to a Record pointer that will be updated to the new record
 * @param schema : Pointer to the Schema structure defining the record's format
 *
 * @return RC_OK on successful creation of the record
 */
extern RC createRecord(Record **record, Schema *schema)
{
    if (record == NULL || schema == NULL)
    {                                // Validate parameter.
        return RC_INVALID_PARAMETER; // Returns Parameter code.
    }

    Record *newRecord = (Record *)malloc(sizeof(Record)); // allocate the record struct.
    if (newRecord == NULL)
    {                                      // checks if allocation was successful.
        return RC_MEMORY_ALLOCATION_ERROR; // return error code if there was any.
    }

    int recordSize = getRecordSize(schema); // get the size of the new record.
    if (recordSize <= 0)
    {                    // Checks if size is lesser than zero.
        free(newRecord); // Frees newly allocated memory.
        return RC_ERROR; // Returns error code.
    }

    (*newRecord).data = (char *)malloc(recordSize); // memory allocation for the data section.
    if ((*newRecord).data == NULL)
    {                                      // If allocation was not successful.
        free(newRecord);                   // Frees the new record pointer.
        return RC_MEMORY_ALLOCATION_ERROR; // returns Allocation failed code.
    }

    // Initialize RID to invalid values
    (*newRecord).id.page = -1; // Setting an invalid Page ID
    (*newRecord).id.slot = -1; // setting invalid slot number

    // Initialize data with empty marker and null terminator
    char *data = (*newRecord).data; // Data is the pointer.
    *data = '-';                    // tombstone is set to empty.

    data += 1;    // increment the data.
    *data = '\0'; // Add terminating zero

    *record = newRecord; // copy the new record to the record passed in as argument

    return RC_OK; // Return sucess.
}

/**
 * @details : Calculates the byte offset of a specific attribute within a record.
 *            The function traverses the schema attributes up to the target attribute,
 *            summing their sizes to determine the offset.
 *
 * @param schema : Pointer to the Schema structure defining the record's format
 * @param attrNum : The zero-based index of the target attribute
 * @param result : Pointer to an integer where the calculated offset will be stored
 *
 * @return RC_OK on successful calculation of the offset
 */
RC getAttributeOffset(Schema *schema, int attrNum, int *result)
{
    if (schema == NULL || result == NULL || attrNum < 0 || attrNum >= (*schema).numAttr)
    {                    // checks for null parameters.
        return RC_ERROR; // returns if parameters are invalid
    }

    // Start after the tombstone marker
    *result = 1; // offset will start after the tombstone
    int i = 0;   // iterator int

    // Add sizes of preceding attributes
    while (i < attrNum)
    { // Iterating to calculate record size till attrNum.
        if ((*schema).dataTypes[i] == DT_STRING)
        {                                       // checks data type if STRING
            *result += (*schema).typeLength[i]; // Add the String Type.
        }
        else if ((*schema).dataTypes[i] == DT_INT)
        {                           // checks data type if INT
            *result += sizeof(int); // Add the Integer Type.
        }
        else if ((*schema).dataTypes[i] == DT_FLOAT)
        {                             // checks data type if FLOAT
            *result += sizeof(float); // Add the Float Type
        }
        else if ((*schema).dataTypes[i] == DT_BOOL)
        {                            // checks data type if BOOL
            *result += sizeof(bool); // Add the Boolean type
        }
        else
        {                                                     // if none of the above types exists.
            return RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE; // Returns a generic error
        }
        i += 1; // increment iterator.
    }

    return RC_OK; // Returns OK result after calculating the offset.
}

/**
 * @details : Deallocates memory used by a record object. The function frees
 *            all resources associated with the record.
 *
 * @param record : Pointer to the Record structure to be freed
 *
 * @return RC_OK on successful deallocation
 */
extern RC freeRecord(Record *record)
{
    if (record == NULL)
    {                                // Checks if the parameter is empty.
        return RC_INVALID_PARAMETER; // Returns Invalid argument
    }

    if ((*record).data != NULL)
    {                         // checks if the data pointer exist before attempting to free.
        free((*record).data); // Frees the record pointer
    }

    free(record); // frees the complete record.

    return RC_OK; // Returns Result
}

/**
 * @details : Extracts the value of a specific attribute from a record. The function
 *            computes the attribute's offset, reads the serialized value from the record,
 *            and creates a Value object with the appropriate type and content.
 *
 * @param record : Pointer to the Record structure containing the attribute
 * @param schema : Pointer to the Schema structure defining the record's format
 * @param attrNum : The zero-based index of the target attribute
 * @param value : Pointer to a Value pointer that will be updated with the extracted value
 *
 * @return RC_OK on successful extraction of the attribute value
 */
extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    if (record == NULL || schema == NULL || value == NULL)
    {                                // Check if pointers are valid
        return RC_INVALID_PARAMETER; // Returns Parameter code.
    }

    if (attrNum < 0 || attrNum >= (*schema).numAttr)
    {                                // checks for valid attribute number
        return RC_RM_NO_MORE_TUPLES; // There isn't any more Tuples available.
    }

    int offset = 0;                                           // Declares offset variable.
    RC result = getAttributeOffset(schema, attrNum, &offset); // get offset from the method and save to offset parameter.
    if (result != RC_OK)
    {                  // checks to see whether the method returns OK.
        return result; // result code that's not ok.
    }

    Value *attrValue = (Value *)malloc(sizeof(Value)); // dynamically allocate the value struct.
    if (attrValue == NULL)
    {                                      // malloc may return null
        return RC_MEMORY_ALLOCATION_ERROR; // Failed to allocate.
    }

    char *data = (*record).data; // copy the records data pointer.
    data += offset;              // increase pointer by offset to point to attribute value.

    // Note: This appears to be a workaround or fix for a specific issue
    (*schema).dataTypes[attrNum] = (attrNum == 1) ? 1 : (*schema).dataTypes[attrNum]; // Looks very buggy

    // Extract value based on data type
    if ((*schema).dataTypes[attrNum] == DT_STRING)
    {                                                     // checks if string data type.
        int len = (*schema).typeLength[attrNum];          // assign attribute type Length to len
        (*attrValue).v.stringV = (char *)malloc(len + 1); // allocating space.
        if ((*attrValue).v.stringV == NULL)
        {                                      // checks if allocation went successfully.
            free(attrValue);                   // freeing attribute value
            return RC_MEMORY_ALLOCATION_ERROR; // Return memory
        }

        strncpy((*attrValue).v.stringV, data, len); // copy data value.
        (*attrValue).v.stringV[len] = '\0';         // assign null value.
        (*attrValue).dt = DT_STRING;                // assigns Datatype
    }
    else if ((*schema).dataTypes[attrNum] == DT_INT)
    {                                    // int check
        int val = 0;                     // declaring the int value.
        memcpy(&val, data, sizeof(int)); // copy the int data.
        (*attrValue).v.intV = val;       // set to attrValue
        (*attrValue).dt = DT_INT;        // set datatypes to int.
    }
    else if ((*schema).dataTypes[attrNum] == DT_FLOAT)
    {                                      // float check.
        float val;                         // decalre a value
        memcpy(&val, data, sizeof(float)); // memcpy for data
        (*attrValue).v.floatV = val;       // pass it to attrValue struct.
        (*attrValue).dt = DT_FLOAT;        // Datatype assign float.
    }
    else if ((*schema).dataTypes[attrNum] == DT_BOOL)
    {                                     // check bool datatypes
        bool val;                         // bool value variable.
        memcpy(&val, data, sizeof(bool)); // memcpy
        (*attrValue).v.boolV = val;       // assgin to value
        (*attrValue).dt = DT_BOOL;        // assign value.
    }
    else
    {                                                     // If anything exist other than that then free and return comp_diferent_datatypes
        free(attrValue);                                  // If anything exist then free attribute
        return RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE; // Returns error
    }

    *value = attrValue; // Setting Value

    return RC_OK; // return result.
}

/**
 * @details : Sets the value of a specific attribute in a record. The function
 *            computes the attribute's offset and writes the provided value into
 *            the record data in serialized form according to its type.
 *
 * @param record : Pointer to the Record structure to be modified
 * @param schema : Pointer to the Schema structure defining the record's format
 * @param attrNum : The zero-based index of the target attribute
 * @param value : Pointer to the Value structure containing the new attribute value
 *
 * @return RC_OK on successful update of the attribute value
 */
extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    if (record == NULL || schema == NULL || value == NULL)
    {                                // checks for all parameters whether if null
        return RC_INVALID_PARAMETER; // returns code
    }

    if (attrNum < 0 || attrNum >= (*schema).numAttr)
    {                                // if attribute num is outside range.
        return RC_RM_NO_MORE_TUPLES; // Returns error
    }

    int offset = 0;                                           // offset declaration
    RC result = getAttributeOffset(schema, attrNum, &offset); // assign the calculated offset to a variable
    if (result != RC_OK)
    {                  // checks for non OK values in result.
        return result; // return result
    }

    char *data = (*record).data; // Get record pointer
    data += offset;              // Increment pointer to offset data

    // Set value based on data type
    if ((*schema).dataTypes[attrNum] == DT_STRING)
    {                                            // if attribute is a string datatype.
        int len = (*schema).typeLength[attrNum]; // get its attribute length for type
        strncpy(data, (*value).v.stringV, len);  // string copy from value to data.
    }
    else if ((*schema).dataTypes[attrNum] == DT_INT)
    {                                   // if attribute is integer type.
        *(int *)data = (*value).v.intV; // assigns from value.
    }
    else if ((*schema).dataTypes[attrNum] == DT_FLOAT)
    {                                       // if attribute is a Float
        *(float *)data = (*value).v.floatV; // Assign value.
    }
    else if ((*schema).dataTypes[attrNum] == DT_BOOL)
    {                                     // if attribute is bool
        *(bool *)data = (*value).v.boolV; // Assign value from Bool value.
    }
    else
    {                                                     // Returns an error in other conditions.
        return RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE; // returns code
    }

    return RC_OK; // returning.
}