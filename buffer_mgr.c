#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>



typedef struct Bookkeeping4Swap {
	int pool_index; /* the position of the page frame in the pool */
	int page_index; /* the position of the page in the page file */
	struct Bookkeeping4Swap * next;
	struct Bookkeeping4Swap * prev;
}Bookkeeping4Swap;


typedef struct BP_mgmt {
	int * FrameContents; /* the pointer that points to the array of PageNumbers */
	bool * DirtyFlags; /* the pointer that points to the array of DirtyFlags */
	int * FixCounts;  /* the pointer that points to the array of FixCounts */
	int  NumReadIO;  /* the number of ReadIO */
	int  NumWriteIO; /* the number of WriteIO */
	char * PagePool;  /* pointer to point to the acutal page_frame in the memeory pool*/
	int *Flag4Clock;  /* pointer to point to flag array for clock memory swapping algorithm*/
	int AvailablePool; /* the number of empty/availabe page frames left in the memory pool */
	Bookkeeping4Swap *HEAD;
	Bookkeeping4Swap *TAIL;
	Bookkeeping4Swap *CURRENT_HANDLE;
}BP_mgmt;









#define MAKE_BP_mgmt() \
	((BP_mgmt *)malloc (sizeof(BP_mgmt)))
#define MAKE_Bookkeeping4swap() \
	((Bookkeeping4Swap *)malloc (sizeof(Bookkeeping4Swap)))
#define head(bm) \
	(((BP_mgmt *)bm->mgmtData)->HEAD)
#define tail(bm) \
	(((BP_mgmt *)bm->mgmtData)->TAIL)
#define current(bm) \
	(((BP_mgmt *)bm->mgmtData)->CURRENT_HANDLE)

/************************************************************************
Function Name: init_BP_mgmt
Description:
	Initializes the variables of the BP_mgmt * mgmtData object with the 
	passed in pool_size.
Parameters:
	BP_mgmt * mgmtData, int pool_size
Return:
	None/Void
Author:
	Miao Song
HISTORY:
	Date		Name		Content
	2016-02-24	Miao Song	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/

void init_BP_mgmt (BP_mgmt *mgmtData, int pool_size){
	mgmtData->FrameContents = (int *)malloc(pool_size*sizeof(int));
	mgmtData->DirtyFlags = (bool *)malloc (pool_size*sizeof(bool));
	mgmtData->FixCounts = (int *)malloc(pool_size*sizeof(int));
	mgmtData->NumReadIO = 0;
	mgmtData->NumWriteIO = 0;
	mgmtData->PagePool = (char *)malloc(pool_size*PAGE_SIZE*sizeof(char));
	mgmtData->Flag4Clock = (int *)malloc(pool_size*PAGE_SIZE*sizeof(int));
	mgmtData->AvailablePool = pool_size;
	mgmtData->HEAD = NULL;
	mgmtData->TAIL = NULL;
	mgmtData->CURRENT_HANDLE = NULL;
	int i;
	for (i=0; i< pool_size; i++){
		*(mgmtData->FrameContents + i) = NO_PAGE;
		*(mgmtData->DirtyFlags + i) = false;
		*(mgmtData->FixCounts + i) = 0;
		*(mgmtData->Flag4Clock + i) = 0;
	}
}

/************************************************************************
Function Name: destroy_BP_mgmt
Description:
	Deallocates memory assigned to the BP_mgmt * mgmtData object and 
	resets all other variables to NULL.
Parameters:
	BP_mgmt * mgmtData
Return:
	None/Void
Author:
	Miao Song
HISTORY:
	Date		Name		Content
	2016-02-24	Miao Song	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
void destroy_BP_mgmt(BP_mgmt * mgmtData){
	free(mgmtData->FrameContents);
	free(mgmtData->DirtyFlags);
	free(mgmtData->FixCounts);
	free(mgmtData->PagePool);
	free(mgmtData->Flag4Clock);
	mgmtData->FrameContents = NULL;
	mgmtData->DirtyFlags = NULL;
	mgmtData->FixCounts = NULL;
	mgmtData->Flag4Clock = NULL;
	mgmtData->NumReadIO = 0;
	mgmtData->NumWriteIO = 0;
	mgmtData->PagePool = NULL;
	mgmtData->HEAD = NULL;
	mgmtData->TAIL = NULL;
	mgmtData->CURRENT_HANDLE = NULL;

}

/************************************************************************
Function Name: insert_into_bookkeepinglist
Description:
	This function inserts a frame's page and pool index into the 
	buffer pool's mgmtData linked list for internal tracking.
Parameters:
	int page_index, int pool_index, BM_BufferPool *const bm
Return:
	None/Void
Author:
	Miao Song
HISTORY:
	Date		Name		Content
	2016-02-24	Miao Song	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/

void insert_into_bookkeepinglist(int page_index, int pool_index, BM_BufferPool *const bm){
	Bookkeeping4Swap *handle = MAKE_Bookkeeping4swap();
	handle->page_index = page_index;
	handle->pool_index = pool_index;
	handle->next = NULL;
	if(((BP_mgmt *)bm->mgmtData)->HEAD == NULL){
		head(bm) = handle;
		tail(bm) = handle;
		handle->prev = NULL;
		handle->next = NULL;
	} else {
		tail(bm)->next = handle;
		handle->prev = tail(bm);
		tail(bm) = handle;
	}
	*(((BP_mgmt *)bm->mgmtData)->FrameContents+pool_index) = page_index;
}

/************************************************************************
Function Name: check_in_cache
Description:
	Checks the buffer pool's mgmtData linked list for the frame with the 
	passed in page index.
Parameters:
	int page_index, BM_BufferPool *const bm
Return:
	Returns RC_OK if frame is found
	Returns RC_PAGE_NOT_FOUND_IN_CACHE if frame is not found
Author:
	Miao Song
HISTORY:
	Date		Name		Content
	2016-02-24	Miao Song	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
RC check_in_cache(int page_index, BM_BufferPool *const bm){
	Bookkeeping4Swap *current = NULL;
	current = head(bm);
	while (current!=NULL){
		if (current->page_index == page_index){
			return RC_OK;
		}
		current = current->next;
	}
	return RC_PAGE_NOT_FOUND_IN_CACHE;
}

/************************************************************************
Function Name: pageindex_mapto_poolindex
Description:
	Iterates over the buffer pool's mgmtData linked list until a match 
	is found with the passed in page_index and that object's pool index 
	is returned.
Parameters:
	int page_index, BM_BufferPool *const bm
Return:
	current->pool_index
Author:
	Miao Song
HISTORY:
	Date		Name		Content
	2016-02-24	Miao Song	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/


int pageindex_mapto_poolindex(int page_index, BM_BufferPool *const bm){
	Bookkeeping4Swap *current = NULL;
	current = head(bm);
	while (current!=NULL){
		if (current->page_index == page_index){
			return current->pool_index;
		}
		current = current->next;
	}
	return RC_PAGE_NOT_FOUND_IN_CACHE;
}




/************************************************************************
Function Name: applyRSPolicy
Description:
	Applies the replacement policy designated in the function parameters.
Parameters:
	ReplacementStrategy policy, int pageNum, BM_BufferPool *const bm, SM_FileHandle fHandle
Return:
	Returns tail(bm)->pool_index
	Returns RC_ALL_PAGE_RESOURCE_OCCUPIED if candidate == null while iterating over list
Author:
	Miao Song
HISTORY:
	Date		Name		Content
	2016-02-24	Miao Song	Written code
	2016-02-25	Jon Yang	Added function header comment
	2016-03-01	Miao Song	Added support of Clock replacement policy
************************************************************************/
int applyRSPolicy(ReplacementStrategy policy, int pageNum, BM_BufferPool *const bm, SM_FileHandle fHandle){
	if (policy == RS_FIFO || policy == RS_LRU){
		Bookkeeping4Swap * candidate = head(bm);

		while (candidate != NULL && *(((BP_mgmt *)bm->mgmtData)->FixCounts+candidate->pool_index)>0){
			candidate = candidate->next;
		}
		if (candidate == NULL) {
			return RC_ALL_PAGE_RESOURCE_OCCUPIED;
		} else {
			if(candidate != tail(bm)){
				tail(bm)->next = candidate;

				candidate->next->prev = candidate->prev;
				if(candidate == head(bm)){
					head(bm) = head(bm)->next;
				} else {
					candidate->prev->next = candidate->next;
				}
				candidate->prev = tail(bm);				
				tail(bm) = tail(bm)->next;
				tail(bm)->next = NULL;
			}

			if(*(((BP_mgmt *)bm->mgmtData)->DirtyFlags+tail(bm)->pool_index)== true){
				char * memory = ((BP_mgmt *)bm->mgmtData)->PagePool + tail(bm)->pool_index*PAGE_SIZE*sizeof(char);
				int old_pageNum = tail(bm)->page_index;
				writeBlock(old_pageNum, &fHandle, memory);
				*(((BP_mgmt *)bm->mgmtData)->DirtyFlags+tail(bm)->pool_index)= false;
				((BP_mgmt *)bm->mgmtData)->NumWriteIO++;
			}
			tail(bm)->page_index = pageNum;
			*(((BP_mgmt *)bm->mgmtData)->FrameContents+tail(bm)->pool_index) = pageNum;
			return tail(bm)->pool_index;
		}
	} else if (policy == RS_CLOCK){
		Bookkeeping4Swap * candidate = NULL;
		if (current(bm) == NULL){
			current(bm) = head(bm);	
		}
		while (*(((BP_mgmt *)bm->mgmtData)->Flag4Clock+current(bm)->pool_index)== 1){
			*(((BP_mgmt *)bm->mgmtData)->Flag4Clock+current(bm)->pool_index) = 0; /* reset the bit flag back to 0 */
			if (current(bm) == tail(bm)){
				current(bm) = head(bm); /* To mock as the circular buffer for clock algorithm */
			} else {
				current(bm) = current(bm)->next;
			}	
		}
		current(bm)->page_index = pageNum; /* update the pageNum to the selected page frame */
		*(((BP_mgmt *)bm->mgmtData)->FrameContents+current(bm)->pool_index) = pageNum;	
		*(((BP_mgmt *)bm->mgmtData)->Flag4Clock+current(bm)->pool_index) = 1; /* set the bit to 1 since the new request page is referenced */
		candidate = current(bm);
		/* Increase the handle to point to the next candidate for the future round of swapping */
		if (current(bm) == tail(bm)){
				current(bm) = head(bm); /* To mock as the circular buffer for clock algorithm */
			} else {
				current(bm) = current(bm)->next;
			}	
		return candidate->pool_index;
	}
	return -1;
}
/************************************************************************
Function Name: adjustOrderInCacheByLRU
Description:
	Adjust the cache order by LRU 
Parameters:
	int page_index, BM_BufferPool *const bm
Return:
	RC_OK
Author:
	Miao Song
HISTORY:
	Date		Name		Content
	2016-02-24	Miao Song	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
RC adjustOrderInCacheByLRU(int page_index, BM_BufferPool *const bm){
	Bookkeeping4Swap *current = NULL;
	Bookkeeping4Swap *tail = NULL;
	current = head(bm);
	tail = tail(bm);
	while (current != NULL){
		if (current->page_index == page_index){
			break;
		}
		current = current->next;
	}
	if (current != tail){
		tail(bm)->next = current;
		current->next->prev = current->prev;
		if(current == head(bm)){
			head(bm) = head(bm)->next;
		} else {
			current->prev->next = current->next;
		}
		current->prev = tail(bm);				
		tail(bm) = tail(bm)->next;
		tail(bm)->next = NULL;	
	}
	
}

/************************************************************************
Function Name: initBufferPool
Description:
	Intializes the BM_BufferPool object's member variables to the 
	passed in values.
Parameters:
	BM_BufferPool *const bm, const char *const pageFileName, 
	const int numPages, ReplacementStrategy strategy, 
	void *stratData)
Return:
	RC_OK
Author:
	Miao Song
HISTORY:
	Date		Name		Content
	2016-02-24	Miao Song	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy, 
		void *stratData)
{
	/*assign values to BM_BufferPool structure.*/
	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;
	bm->mgmtData = MAKE_BP_mgmt();
	init_BP_mgmt(bm->mgmtData,bm->numPages);
	BP_mgmt *mgmtData = (BP_mgmt *)bm->mgmtData;
	return RC_OK;
}
/************************************************************************
Function Name: shutdownBufferPool
Description:
	Calls forceFlushPool and destroy functions on bm, deallocates 
	memory from bm->mgmtData, and sets it to NULL.
Parameters:
	BM_BufferPool *const bm
Return:
	Returns RC_OK
	Returns RC_FAIL_SHUTDOWN_POOL if bm->mgmtData->FixCounts+i> 0
Author:
	Miao Song
HISTORY:
	Date		Name		Content
	2016-02-24	Miao Song	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
RC shutdownBufferPool(BM_BufferPool *const bm){
	BP_mgmt *mgmtData = (BP_mgmt *)bm->mgmtData;
	int i;
	for (i=0; i< bm->numPages; i++){
	  if (*(((BP_mgmt *)bm->mgmtData)->FixCounts+i)> 0) {
	  	return RC_FAIL_SHUTDOWN_POOL;
	  }
	 }
	forceFlushPool(bm);
	destroy_BP_mgmt(bm->mgmtData);
	free(bm->mgmtData);
	bm->mgmtData = NULL;
	return RC_OK;
}

/************************************************************************
Function Name: markDirty
Description:
	Marks a frame as dirty by setting bm->mgmtData->DirtyFlags+pool_index to true
Parameters:
	BM_BufferPool *const bm, BM_PageHandle *const page
Return:
	Returns RC_OK
Author:
	Chris Dani
HISTORY:
	Date		Name		Content
	2016-02-24	Chris Dani	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
	int pool_index = pageindex_mapto_poolindex(page->pageNum, bm);
	*(((BP_mgmt *)bm->mgmtData)->DirtyFlags+pool_index) = true;
	return RC_OK;
}
/************************************************************************
Function Name: unpinPage
Description:
	Unpins a frame from the buffer pool.
Parameters:
	BM_BufferPool *const bm, BM_PageHandle *const page
Return:
	Returns RC_OK
	Returns RC_UNPIN_FAIL if bm->mgmtData->FixCounts+pool_index<0
Author:
	Chris Dani
HISTORY:
	Date		Name		Content
	2016-02-24	Chris Dani	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	int pool_index = pageindex_mapto_poolindex(page->pageNum, bm);
	(*(((BP_mgmt *)bm->mgmtData)->FixCounts+pool_index))--;
	return RC_OK;

}

/************************************************************************
Function Name: forcePage
Description:
	Writes the current content of the page back to the page file on disk.
Parameters:
	BM_BufferPool *const bm, BM_PageHandle *const page
Return:
	Returns RC_OK
	Returns RC_FAIL_FORCE_PAGE_DUETO_PIN_EXIT if bm->mgmtData->FixCounts+pool_index> 0)
Author:
	Nikhita Kataria
HISTORY:
	Date		Name		Content
	2016-02-24	Nikhita Kataria	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
	/* open the page file in the disk first */

	int pageNum = page->pageNum;
	int pool_index = pageindex_mapto_poolindex(pageNum,bm);
	if (*(((BP_mgmt *)bm->mgmtData)->FixCounts+pool_index)> 0){
		return RC_FAIL_FORCE_PAGE_DUETO_PIN_EXIT;
	} else {
		SM_FileHandle fh;
		openPageFile (bm->pageFile, &fh);
		char * memory = page->data;
		writeBlock(pageNum, &fh, memory);
		*(((BP_mgmt *)bm->mgmtData)->DirtyFlags+pool_index)= false; /* set dirty flag to false after flashing into disk */
		((BP_mgmt *)bm->mgmtData)->NumWriteIO++;
		closePageFile(&fh);	
		return RC_OK;
	}
}
/************************************************************************
Function Name: forceFlushPool
Description:
	Causes all dirty pages (with fix count 0) from the buffer pool to be written to disk.
Parameters:
	BM_BufferPool *const bm
Return:
	Returns RC_OK
Author:
	Nikhita Kataria
HISTORY:
	Date		Name		Content
	2016-02-24	Nikhita Kataria		Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
RC forceFlushPool(BM_BufferPool *const bm){
	/* open the page file in the disk first */
	SM_FileHandle fh;
	char * memory;
	int page_index;
	openPageFile (bm->pageFile, &fh);
	int i;
	for (i=0; i< bm->numPages; i++){
		if (*(((BP_mgmt *)bm->mgmtData)->DirtyFlags+i)== true) {
			page_index = *(((BP_mgmt *)bm->mgmtData)->FrameContents+i);
			memory = ((BP_mgmt *)bm->mgmtData)->PagePool + i*PAGE_SIZE*sizeof(char);
			writeBlock(page_index, &fh, memory);
			*(((BP_mgmt *)bm->mgmtData)->DirtyFlags+i)= false; /* set dirty flag to false after flashing into disk */
			((BP_mgmt *)bm->mgmtData)->NumWriteIO++;			
		}		
	}
	closePageFile(&fh);	
	return RC_OK;		
}

/************************************************************************
Function Name: pinPage
Description:
	Pins a page from disk to the buffer pool.
Parameters:
	BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum
Return:
	Returns RC_OK
Author:
	Nikhita Kataria
HISTORY:
	Date		Name		Content
	2016-02-24	Nikhita Kataria	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum){

	/* open the page file in the disk first */
	SM_FileHandle fh;
	openPageFile (bm->pageFile, &fh);
	/* The request page_index is larger than the actual size of page file, this could cause segment false */
	if (fh.totalNumPages < (pageNum+1)){
		ensureCapacity(pageNum+1,&fh);
	}
	page->pageNum = pageNum;
	if (check_in_cache(pageNum,bm)==0){	
		int pool_index = pageindex_mapto_poolindex(pageNum,bm);
		(*(((BP_mgmt *)bm->mgmtData)->FixCounts+pool_index))++;
		page->data = (((BP_mgmt *)bm->mgmtData)->PagePool)+pool_index*PAGE_SIZE*sizeof(char);
		if (bm->strategy == RS_LRU){
			adjustOrderInCacheByLRU(pageNum,bm);
		}
		(*(((BP_mgmt *)bm->mgmtData)->Flag4Clock+pool_index)) = 1; /* Set flag =1 when a page is referenced for clock algorithm*/
		return RC_OK;
	} else {
		if (((BP_mgmt *)bm->mgmtData)->AvailablePool>0){
			int pool_index = bm->numPages - ((BP_mgmt *)bm->mgmtData)->AvailablePool;
			insert_into_bookkeepinglist(pageNum, pool_index ,bm);
			((BP_mgmt *)bm->mgmtData)->AvailablePool--;
			(*(((BP_mgmt *)bm->mgmtData)->FixCounts+pool_index))++;
			page->data = (((BP_mgmt *)bm->mgmtData)->PagePool)+pool_index*PAGE_SIZE*sizeof(char);

		} else { 
			int pool_index = applyRSPolicy(bm->strategy,pageNum, bm, fh);
			(*(((BP_mgmt *)bm->mgmtData)->FixCounts+pool_index))++;
			page->data = (((BP_mgmt *)bm->mgmtData)->PagePool)+pool_index*PAGE_SIZE*sizeof(char);

		}
		((BP_mgmt *)bm->mgmtData)->NumReadIO++;
		readBlock(page->pageNum, &fh, page->data);
	}
	closePageFile(&fh);		
	return RC_OK;
}

// Statistics Interface
/************************************************************************
 Function Name: getFrameContents
 Description:
 	Retrieves the FrameContents of the passed in BM_BufferPool object pointer.
 Parameters:
 	BM_BufferPool *const bm
 Return:
 	bm->mgmtData->FrameContents
 Author:
 	Jon Yang
 HISTORY:
 	Date		Name		Content
 	2016-02-24	Jon Yang	Written code
 	2016-02-25	Jon Yang	Added function header comment
 ************************************************************************/
PageNumber *getFrameContents (BM_BufferPool *const bm){
	return ((BP_mgmt *)bm->mgmtData)->FrameContents;
}

/************************************************************************
Function Name: getDirtyFlags
Description:
	Retrieves the DirtyFlags of the passed in BM_BufferPool object pointer.
Parameters:
	BM_BufferPool *const bm
Return:
	bm->mgmtData->DirtyFlags
Author:
	Jon Yang
HISTORY:
	Date		Name		Content
	2016-02-24	Jon Yang	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
bool *getDirtyFlags (BM_BufferPool *const bm){
	return ((BP_mgmt *)bm->mgmtData)->DirtyFlags;
}
/************************************************************************
Function Name: getFixCounts
Description:
	Retrieves the FixCounts of the passed in BM_BufferPool object pointer.
Parameters:
	BM_BufferPool *const bm
Return:
	bm->mgmtData->FixCounts
Author:
	Jon Yang
HISTORY:
	Date		Name		Content
	2016-02-24	Jon Yang	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
int *getFixCounts (BM_BufferPool *const bm){
	return ((BP_mgmt *)bm->mgmtData)->FixCounts;
}
/************************************************************************
Function Name: getNumReadIO
Description:
	Retrieves the NumReadIO of the passed in BM_BufferPool object pointer.
Parameters:
	BM_BufferPool *const bm
Return:
	bm->mgmtData->NumReadIO
Author:
	Jon Yang
HISTORY:
	Date		Name		Content
	2016-02-24	Jon Yang	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
int getNumReadIO (BM_BufferPool *const bm){
	return ((BP_mgmt *)bm->mgmtData)->NumReadIO;
}
/************************************************************************
Function Name: getNumWriteIO
Description:
	Retrieves the NumWriteIO of the passed in BM_BufferPool object pointer.
Parameters:
	BM_BufferPool *const bm
Return:
	bm->mgmtData->NumWriteIO
Author:
	Jon Yang
HISTORY:
	Date		Name		Content
	2016-02-24	Jon Yang	Written code
	2016-02-25	Jon Yang	Added function header comment
************************************************************************/
int getNumWriteIO (BM_BufferPool *const bm){
	return ((BP_mgmt *)bm->mgmtData)->NumWriteIO;
}
