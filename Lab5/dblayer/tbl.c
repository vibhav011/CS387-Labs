
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define SLOT_COUNT_OFFSET 2
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

// int  getLen(int slot, byte *pageBuf); UNIMPLEMENTED;
// int  getNumSlots(byte *pageBuf); UNIMPLEMENTED;
// void setNumSlots(byte *pageBuf, int nslots); UNIMPLEMENTED;
// int  getNthSlotOffset(int slot, char* pageBuf); UNIMPLEMENTED;

int File_Close(int fd) {
    int *pagenum;
    char **pagebuf;
    int errVal;

    if (PF_GetFirstPage(fd, pagenum, pagebuf) == PFE_OK) {
        PF_UnfixPage(fd, *pagenum, false);
        while (PF_GetNextPage(fd, pagenum, pagebuf) == PFE_OK) {
            PF_UnfixPage(fd, *pagenum, false);
        }
    }
    if ((errVal = PF_CloseFile(fd)) != PFE_OK) {
        return errVal;
    }
    return 0;
}

/**
   Opens a paged file, creating one if it doesn't exist, and optionally
   overwriting it.
   Returns 0 on success and a negative error code otherwise.
   If successful, it returns an initialized Table*.
 */
int
Table_Open(char *dbname, Schema *schema, bool overwrite, Table **ptable)
{
    int fd, errVal;

    if (overwrite) {
        // Delete the file if it exists
        if ((fd = PF_OpenFile(dbname)) >= 0) {
            if ((errVal = File_Close(fd)) != 0) {
                PF_PrintError("Error closing file");
                return errVal;
            }
            if ((errVal = PF_DestroyFile(dbname)) != PFE_OK) {
                PF_PrintError("Error destroying file");
                return errVal;
            }
        }
    }

    if ((fd = PF_OpenFile(dbname)) < 0) {
        if ((errVal = PF_CreateFile(dbname)) != PFE_OK) {
            PF_PrintError("Error creating file");
            return errVal;
        }
        if ((fd = PF_OpenFile(dbname)) < 0) {
            PF_PrintError("Error opening file");
            return fd;
        }
    }
    Table *table = (Table *) malloc(sizeof(Table));
    table->schema = schema;
    table->fd = fd;
    *table->lastPage = -1;
    table->numPages = 0;

    char **pagebuf;
    if (PF_GetFirstPage(fd, table->lastPage, pagebuf) == PFE_OK) {
        table->numPages++;
        while (PF_GetNextPage(fd, table->lastPage, pagebuf) == PFE_OK) {
            table->numPages++;
        }
    }

    *ptable = table;

    return 0;
    // allocate Table structure  and initialize and return via ptable
    // The Table structure only stores the schema. The current functionality
    // does not really need the schema, because we are only concentrating
    // on record storage. 
}

void
Table_Close(Table *tbl) {
    if (File_Close(tbl->fd) != 0) {
        PF_PrintError("Error closing file");
    }
    free(tbl);
    // Unfix any dirty pages, close file.
}


int
Table_Insert(Table *tbl, byte *record, int len, RecId *rid) {
    // Allocate a fresh page if len is not enough for remaining space
    // Get the next free slot on page, and copy record in the free
    // space
    // Update slot and free space index information on top of page.
    char *pagebuf;
    if (*tbl->lastPage == -1) {
        if (PF_AllocPage(tbl->fd, tbl->lastPage, &pagebuf) != PFE_OK) {
            PF_PrintError("Error allocating page");
            return -1;
        }
        Header *header = (Header *) pagebuf;
        header->numSlots = 0;
        header->freeSlotOffset = PF_PAGE_SIZE;
        PF_UnfixPage(tbl->fd, *tbl->lastPage, false);
    }
    if (PF_GetThisPage(tbl->fd, *tbl->lastPage, &pagebuf) != PFE_OK) {
        PF_PrintError("Error getting page");
        return -1;
    }
    Header *header = (Header *) pagebuf;
    int nslots = header->numSlots;
    int freeSpace = header->freeSlotOffset - sizeof(int) - sizeof(short)*(2+nslots);

    if (freeSpace < len+1) {
        PF_UnfixPage(tbl->fd, *tbl->lastPage, true);
        if (PF_AllocPage(tbl->fd, tbl->lastPage, &pagebuf) != PFE_OK) {
            PF_PrintError("Error allocating page");
            return -1;
        }
        header = (Header *) pagebuf;
        nslots = header->numSlots = 0;
        header->freeSlotOffset = PF_PAGE_SIZE;
        freeSpace = header->freeSlotOffset - sizeof(int) - sizeof(short)*2;
    }
    assert(freeSpace >= len+1);     // Not enough space on page
    header->freeSlotOffset -= len+1;
    memcpy(pagebuf+header->freeSlotOffset, record, len);
    pagebuf[header->freeSlotOffset+len] = '\0';
    header->numSlots = nslots+1;
    header->slotOffsets[nslots] = header->freeSlotOffset;
    *rid = (*tbl->lastPage << 16) + (int)header->freeSlotOffset;
    
    return 0;
}

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
 */
int
Table_Get(Table *tbl, RecId rid, byte *record, int maxlen) {
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;

    char *pagebuf;
    if (PF_GetThisPage(tbl->fd, pageNum, &pagebuf) != PFE_OK) {
        PF_PrintError("Error getting page");
        return -1;
    }
    Header *header = (Header *) pagebuf;
    int slotOffset = header->slotOffsets[slot];
    int len = strlen(pagebuf+slotOffset);
    if (len > maxlen) {
        len = maxlen;
    }
    memcpy(record, pagebuf+slotOffset, len);
    // PF_GetThisPage(pageNum)
    // In the page get the slot offset of the record, and
    // memcpy bytes into the record supplied.
    // Unfix the page
    return len; // return size of record
}

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn) {
    int *pagenum;
    char *pagebuf;

    if (PF_GetFirstPage(tbl->fd, pagenum, &pagebuf) != PFE_OK) {
        return;
    }

    do {
        Header *header = (Header *) pagebuf;
        int nslots = header->numSlots;
        for (int i = 0; i < nslots; i++) {
            int len = strlen(pagebuf+header->slotOffsets[i]);
            int rid = (*pagenum << 16) + (int)header->freeSlotOffset;
            callbackfn(callbackObj, rid, pagebuf+header->slotOffsets[i], len);
        }
    } while (PF_GetNextPage(tbl->fd, pagenum, &pagebuf) == PFE_OK);
    
    // UNIMPLEMENTED;

    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    //    for each record in that page,
    //          callbackfn(callbackObj, rid, record, recordLen)
}


