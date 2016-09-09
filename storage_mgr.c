#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include "storage_mgr.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>

#define FALSE 0
#define TRUE 1

static int open_fds;
int is_init_SM = FALSE;

typedef struct SM_Handle {
	SM_FileHandle *fHandle;
	struct SM_Handle *next;
}SM_Handle;
SM_Handle *head = NULL;
SM_Handle *end = NULL;

/* Initialize SM */
void initStorageManager (void) {
	if (!is_init_SM) {
		is_init_SM = TRUE;
	}
}

/* Create the file 
 * Input:
 * @ fileName : Name of the file to be created
 *
 * Return:
 * RC : Return code of the operation
 */
RC createPageFile (char *fileName) {
	FILE *fd;
	if( access( fileName, 0 ) == -1 ) {
		fd = fopen(fileName, "w");
		if (fd < 0) {
			printf("Couldnt create the file");
			return RC_FILE_HANDLE_NOT_INIT;
		}
		char * page_handle = (char*) calloc (PAGE_SIZE, sizeof(char));
		if (page_handle == NULL) {
			return RC_MEM_ALLOCATION_FAIL;
		}
		memset(page_handle, '\0', PAGE_SIZE);
		fwrite(page_handle, sizeof(char), PAGE_SIZE, fd);
		fclose(fd);
		free(page_handle); /* Free memory allocation */
		page_handle = NULL;
	} else {
		/* File already exists, return error */
                printf("The file already exsited.\n");
		return RC_FILE_EXIST;
	}
	return RC_OK;
}

/* Insert fd into list of open fd's
 * Input:
 * @ fHandle : SM_FileHandle data structure
 *
 * Return:
 * RC : Return code of the operation
 */
RC insert_into_list (SM_FileHandle *fHandle) {
	SM_Handle *handle = calloc(1, sizeof(SM_Handle));
	handle->fHandle = fHandle;
	handle->next = NULL; 
	if (head == NULL) {
		head = handle;
		end = head;
	} else {
		end->next = handle;
		end = handle;
	}	
	return RC_OK;
}

/* Delete fd from list of open fd's
 * Input:
 * @ fileName : Name of the file to be deleted
 *
 * Return:
 * RC : Return code of the operation
 */
RC delete_from_list (SM_FileHandle *fHandle) {
	SM_Handle *current, *prev;
	current = head;
	prev = head;
	while (current != NULL) {
		if (current->fHandle == fHandle) {
			if (current == head) {
				head = head->next;
			} else if (current == end) {
				end = prev;
				end->next = NULL;
			} else {
				prev->next = current->next;				
			}
			free(current); /* Free memory allocation */
		}
		prev = current;
		current = current->next;
	}
	return RC_OK;
}

/* Check if fd is open/valid
 * Input:
 * @ fHandle : SM_FileHandle data structure
 *
 * Return:
 * RC : Return code of the operation
 */
RC check_fd_in_list (SM_FileHandle *fHandle) {
	SM_Handle *handle;
	handle = head;
	while (handle != NULL && (FILE *)handle->fHandle->mgmtInfo != (FILE *)fHandle->mgmtInfo) {
		handle = handle->next;
	}
	if (handle == NULL) {
		return RC_FILE_HANDLE_NOT_INIT;
	}
	return RC_OK;
}

/* Open the file and return the file handle 
 * Input:
 * @ fileName: Name of the file to be opened
 *
 * Return:
 * RC : Return code of the operation
 * @ fHandle : Metadata about the file 
 */
RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
	FILE *fd;
	/* Calloc fHandle + we will insert it into our linked list */
	if( access( fileName, 0 ) != -1 ) {
		fd = fopen(fileName, "r+");
		if(fd) {
			fseek (fd, 0, SEEK_END);
			int size = ftell (fd);
			fHandle -> fileName = fileName;
			fHandle -> totalNumPages = size/PAGE_SIZE;
			fHandle -> curPagePos = 0;
			fHandle -> mgmtInfo = (void *)fd;
			open_fds = open_fds + 1;
			insert_into_list(fHandle);
			return RC_OK;
		} else {
			return RC_FILE_NOT_FOUND;
		}	
	} else {
		return RC_FILE_HANDLE_NOT_INIT;
	}
}

/* Close the file handle 
 * Input:
 * @ fHandle : Metadata about the file to be closed
 *
 * Return:
 * RC : Return code of the operation
 */
RC closePageFile (SM_FileHandle *fHandle) {
	if (fHandle->mgmtInfo) {
		fclose((FILE *)fHandle->mgmtInfo);
		delete_from_list(fHandle);
		open_fds = open_fds - 1;
	} else {
		return RC_FILE_NOT_FOUND;
	}
	return RC_OK;
}

/* Destroy/delete the PageFile 
 * Input:
 * @ fileName: Name of the file to be closed
 *
 * Return:
 * RC : Return code of the operation
 */ 
RC destroyPageFile (char *fileName) {
	printf("Removing %s", fileName);
	int res = remove(fileName);
	if (res ==0) {
		return RC_OK;
	}	
}

/* Read a block from fd
 * Input:
 * @ pageNum : page number for the block to be read
 * @ fHandle : Metadata about the file to be closed
 * @ memPage : output buffer to populate the data read from the block
 *
 * Return:
 * RC : Return code of the operation
 */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	FILE *fd = (FILE *)fHandle->mgmtInfo;
	if ( pageNum > fHandle->totalNumPages -1 || pageNum < 0) {
		return RC_READ_NON_EXISTING_PAGE;
	}
	if (check_fd_in_list(fHandle) == RC_OK) { 
		if (fseek(fd, pageNum*PAGE_SIZE*sizeof(char), SEEK_SET) ==0) {
			fread(memPage, sizeof(char), PAGE_SIZE, fd);
			fHandle->curPagePos = pageNum;
			return RC_OK;
		} else {
			return RC_READ_NON_EXISTING_PAGE;
		}
	} else {
		return RC_FILE_HANDLE_NOT_INIT;	
	}
}

int getBlockPos (SM_FileHandle *fHandle) {
	if (check_fd_in_list(fHandle) == RC_OK) { 
		return fHandle->curPagePos;
	} else {
		return RC_FILE_HANDLE_NOT_INIT;	
	}
}

/* Read first block from fd
 * Input:
 * @ fHandle : Metadata about the file to be closed
 * @ memPage : output buffer to populate the data read from the block
 *
 * Return:
 * RC : Return code of the operation
 */
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	return readBlock(0, fHandle, memPage);
}

/* Read previous block from fd
 * Input:
 * @ fHandle : Metadata about the file to be closed
 * @ memPage : output buffer to populate the data read from the block
 *
 * Return:
 * RC : Return code of the operation
 */
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

/* Read current block from fd
 * Input:
 * @ fHandle : Metadata about the file to be closed
 * @ memPage : output buffer to populate the data read from the block
 *
 * Return:
 * RC : Return code of the operation
 */
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	return readBlock(fHandle->curPagePos, fHandle, memPage);
}

/* Read next block from fd
 * Input:
 * @ fHandle : Metadata about the file to be closed
 * @ memPage : output buffer to populate the data read from the block
 *
 * Return:
 * RC : Return code of the operation
 */
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

/* Read last block from fd
 * Input:
 * @ fHandle : Metadata about the file to be closed
 * @ memPage : output buffer to populate the data read from the block
 *
 * Return:
 * RC : Return code of the operation
 */
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	return readBlock(fHandle->totalNumPages-1, fHandle, memPage);
}

/* Write a block to the FILE
 * Input:
 * @ pageNum : page number where block needs to be written
 * @ fHandle : Metadata about the file to be closed
 * @ memPage : output buffer to populate the data read from the block
 *
 * Return:
 * RC : Return code of the operation
 */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	FILE *fd = (FILE *)fHandle->mgmtInfo;
	if ( pageNum > fHandle->totalNumPages -1 || pageNum < 0) {
		return RC_READ_NON_EXISTING_PAGE;
	}	
	if (check_fd_in_list(fHandle) == RC_OK) {
		if (fseek(fd, pageNum*PAGE_SIZE*sizeof(char), SEEK_SET) == 0) {
			fwrite (memPage, sizeof(char), PAGE_SIZE, fd);
			fHandle->curPagePos = pageNum;
			return RC_OK;
		} else {
			return RC_WRITE_FAILED;
		}	
	} else {
		return RC_FILE_HANDLE_NOT_INIT;	
	}
}

/* Write to the current block of the file
 * Input:
 * @ fHandle : Metadata about the file to be closed
 * @ memPage : output buffer to populate the data read from the block
 *
 * Return:
 * RC : Return code of the operation
 */
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

/* Append empty bytes to the file
 * Input:
 * @ fHandle : Metadata about the file to be closed
 *
 * Return:
 * RC : Return code of the operation
 */
RC appendEmptyBlock (SM_FileHandle *fHandle) {
	FILE *fd = (FILE *)fHandle->mgmtInfo;
	if (check_fd_in_list(fHandle) == RC_OK) {
		char * memPage = (char*) calloc (PAGE_SIZE, sizeof(char));
		if (memPage == NULL) {
			return RC_MEM_ALLOCATION_FAIL;
		}
		memset(memPage, '\0', PAGE_SIZE);
		if (fseek(fd, fHandle->totalNumPages*PAGE_SIZE*sizeof(char), SEEK_SET) == 0) {
			fwrite (memPage, sizeof(char), PAGE_SIZE, fd);
			fHandle->totalNumPages ++;
			free(memPage); /* Free memory allocation */
			memPage = NULL;
			return RC_OK;
		} else {
			free(memPage); /* Free memory allocation */
			memPage = NULL;
			return RC_WRITE_FAILED;
		}	
		
	} else {
		return RC_FILE_HANDLE_NOT_INIT;
	}
}

/* Ensure that size of the file is enough to contain all the pages
 * Input:
 * @ numberOfPages : Number of pages to be written into the file
 * @ fHandle : Metadata about the file to be closed
 *
 * Return:
 * RC : Return code of the operation
 */
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
	if (numberOfPages > fHandle->totalNumPages) {
		FILE *fd = (FILE *)fHandle->mgmtInfo;
		int offset = numberOfPages - fHandle->totalNumPages;
		while (offset--) {
			appendEmptyBlock(fHandle);
		}
	}
	return RC_OK;
}	