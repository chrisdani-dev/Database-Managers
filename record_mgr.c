#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"


typedef struct pageheader {
	int max_slot;  // the number of max slots on each page
	int used_slot; // the number of used slots on each page
	char * freespace; // the pointer to the next avaiable slot space.
}pageheader;

typedef struct tablemgmt {
	BM_BufferPool *bm; // the pointer of the BM_BufferPool which is needed by record magnement to load disk block into memory page frame
	int totalpages; // the total pages of the loaded blocks
	int record_start_page_index; // the page index where the records starts in table
	int record_end_page_index; // the page index where the records ends in table
	BM_PageHandle * pagelist; // the pointer to the listed of page frame managmed by buffer pool
}tablemgmt;

typedef struct scanmgmt {
	int cur_page_index; // the current page index of the running trace through tuple interation in table
	int cur_slot_index; // the current slot index of the running trance through tuple interation in table
	Expr *cond; // the search condition
}scanmgmt;


#define MAKE_Loadedpage() \
	((Loadedpage *)malloc (sizeof(Loadedpage)))
#define MAKE_tablemgmt() \
	((tablemgmt *)malloc (sizeof(tablemgmt)))	

/************************************************************************
Function Name: storeSchemaContent 
Description:
	Function to store the schema into page pool memory
Parameters:
	mem : the memory address where schema is about to instore
	schem : the schema pointer of the to-be-stored schema data structure
Return:
	void
Author:
	Miao Song
HISTORY:
	Date		Name		Content	
 2016-03-30	Miao Song	    Written code
 2016-04-02	Miao Song		Fixed improper schema datatype
************************************************************************/	
void storeSchemaContent(char *mem, Schema *schema){
	/**
	The format of the stored order is:
	name length of attribute1, name length of attribute 2, ...,
	the name of attr1, the name of attr2, ...,
	the datatype of attr1, the datatype of attr2, ...
	the typelenth of datatype in attr1, the typelenth of datatype in attr2,
	the keysize,
	the key position
	**/
	int i,j;
	char *stringV;
	memcpy(mem, &(schema->numAttr),sizeof(int));
	mem = mem + sizeof(int);
	
	for (i=0; i<schema->numAttr; i++){
		j = strlen(schema->attrNames[i]);
		memcpy(mem, &j, sizeof(int));
		mem = mem + sizeof(int);
		}
	for (i=0; i<schema->numAttr; i++){
		memcpy(mem, schema->attrNames[i], strlen(schema->attrNames[i]));
		stringV = (char *)malloc(strlen(schema->attrNames[i]));
		strncpy(stringV, mem, strlen(schema->attrNames[i]));
		mem = mem + strlen(schema->attrNames[i]);
		
		memcpy(mem, &(schema->dataTypes[i]),sizeof(DataType));
		mem = mem + sizeof(DataType);
		memcpy(mem, &(schema->typeLength[i]),sizeof(int));
		mem = mem + sizeof(int);
		}
	memcpy(mem, &(schema->keySize), sizeof(int));
	mem = mem+sizeof(int);
	for(i = 0; i < schema->keySize; i++){
		memcpy(mem, &(schema->keyAttrs[i]), sizeof(int)); 
		mem = mem + sizeof(int);
	}
	
}

/************************************************************************
Function Name: createSchema
Description:
	Function to create the schema from mentioned attributes
Parameters:
	numAttr : Number of attributes
	attrNames : Name of the attributes
	DataType : Data types for the attributes
	tyleLength : Array of Length/Size for the attributes
	keySize : Size of the key attribute
	keys : Array of attributes which will serve as keys.
Return:
	Returns the newly created schema or null if the operation was unsuccessful. 
Author:
	Jonathan Yang
HISTORY:
	Date		Name		Content	
 2016-04-01	Jonathan Yang	Written code
 2016-04-02	Miao Song		Fixed improper schema datatype
************************************************************************/
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {   
    Schema *schema = (Schema *) malloc(sizeof(Schema));
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keyAttrs = keys;
    schema->keySize = keySize;
    return schema;
}

/************************************************************************
Function Name: retriveSchemaContent 
Description:
	Function to get the schema data into page pool memory
Parameters:
	mem : the page pool memory address where schema is stored
	schem :the schema pointer points to the schema structure
Return:
	void
Author:
	Miao Song
HISTORY:
	Date		Name		Content	
 2016-03-30	Miao Song	    Written code
 2016-04-02	Miao Song		Fixed improper schema datatype
************************************************************************/	
void retriveSchemaContent(char *mem, Schema *schema){
	/**
	The format of the stored order is:
	name length of attribute1, name length of attribute 2, ...,
	the name of attr1, the name of attr2, ...,
	the datatype of attr1, the datatype of attr2, ...
	the typelenth of datatype in attr1, the typelenth of datatype in attr2,
	the keysize,
	the key position
	**/
	
	int i,j=0;
	int length;
	int keysize;
	char *stringV;
	memcpy(&(length), mem, sizeof(int));

	char **cpNames = (char **) malloc(sizeof(char*) * length);
	DataType *cpDt = (DataType *) malloc(sizeof(DataType) * length);
	int *cpSizes = (int *) malloc(sizeof(int) * length);
	int *nameSizes = (int *) malloc(sizeof(int) * length);
 	mem = mem+sizeof(int);
	for (i=0; i<length; i++){
		memcpy(&j, mem, sizeof(int));
		nameSizes[i] = j;
		mem = mem + sizeof(int);
	}
	for(i = 0; i < length; i++){
      cpNames[i] = (char *) malloc(nameSizes[i]+1);
      strncpy(cpNames[i], mem, nameSizes[i]);
	  mem = mem +nameSizes[i];
	  memcpy(&cpDt[i],mem, sizeof(DataType));
	  mem = mem+sizeof(DataType);
	  memcpy(&cpSizes[i],mem, sizeof(int));
	  mem = mem+sizeof(int);
    }
	memcpy(&(keysize), mem, sizeof(int));
	int *cpKeys = (int *) malloc(sizeof(int)*keysize);
	mem = mem+sizeof(int);
	for(i=0; i<keysize; i++){
		memcpy(&(cpKeys[i]), mem, sizeof(int)); 
		mem = mem + sizeof(int);
	}
	/* link back to the schema pointer related field */
	schema->numAttr = length;
    schema->attrNames = cpNames;
    schema->dataTypes = cpDt;
    schema->typeLength = cpSizes;
    schema->keyAttrs = cpKeys;
    schema->keySize = keysize;

}


/************************************************************************
Function Name: getSchemaContentSize
Description:
	Function to get the data size of the schema structure
Parameters:
	schem :the schema pointer points to the schema structure
Return:
	void
Author:
	Miao Song
HISTORY:
	Date		Name		Content	
 2016-03-30	Miao Song	    Written code
 2016-04-02	Miao Song		Fixed improper schema datatype
************************************************************************/	
int getSchemaContentSize(Schema *schema){
	int numbAttr_size = sizeof(int);
	int attriname_size = 0, i;
	for (i=0; i<schema->numAttr; i++){
		attriname_size += strlen(schema->attrNames[i]);
	}
	int datatype_size = sizeof(DataType)*(schema->numAttr);
	int typelength_size = sizeof(int)*(schema->numAttr);
	int keyattri_size = sizeof(int)*(schema->keySize);
	int keySize_size = sizeof(int);
	return numbAttr_size+datatype_size+typelength_size+keyattri_size+attriname_size+keySize_size;

}

/************************************************************************
Function Name: getRecordSize
Description:
	Function to get the record size from schema
Parameters:
	Schema : Schema for which size has to be calculated
Return:
	Returns the size of the schema
Author:
	Jonathan Yang
HISTORY:
	Date		Name		Content	
 2016-04-01	Jonathan Yang	Written code
************************************************************************/
int getRecordSize(Schema *schema){
	int recordsize = 0;
	int i;
	for (i=0; i<schema->numAttr; i++){
		switch(schema->dataTypes[i]){
			case DT_INT:
				recordsize += sizeof(int);
				break;
			case DT_STRING:
				recordsize += schema->typeLength[i]; //for STRING type, it is defined in shema meta data
				break;
			case DT_FLOAT:
				recordsize +=  sizeof(float);
				break;
			case DT_BOOL:
				recordsize += sizeof(bool);
				break;
		}
	
	}
	return recordsize;
}


/************************************************************************
Function Name: freeSchema
Description:
	Function to free the schema from memory
Parameters:
	Schema : Schema to be free’d. 
Return:
	Returns RC_OK on success 
Author:
	Jonathan Yang
HISTORY:
	Date		Name		Content	
 2016-04-01	Jonathan Yang	Written code
************************************************************************/
RC freeSchema(Schema *schema) {
	int i;
	for (i=0; i<schema->numAttr ; i++){
		free(schema->attrNames[i]);
	}
	/* Free all the allocated memory attached to the schema realtd field pointer */
    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);
    free(schema);
    return RC_OK;
}

/************************************************************************
Function Name: createRecord
Description:
	Function to create the record from mentioned schema
Parameters:
	Record : Metadata for the record to be created
	Schema : Schema  using which record is to be created. 
Return:
	Returns RC_OK on success 
Author:
	Miao Song
HISTORY:
	Date		Name		Content	
 2016-03-30	Miao Song		Written code
************************************************************************/
RC createRecord(Record **record, Schema *schema){
	*record = (Record *)malloc(sizeof(Record));
	(*record)->data = (char *)malloc(sizeof(char)*getRecordSize(schema));
	return RC_OK;
}


/************************************************************************
Function Name: freeRecord
Description:
	Function to free the record
Parameters:
	Record : Metadata for the record to be created
	Schema : Schema  using which record is to be created. 
Return:
	Returns RC_OK on success 
Author:
	Miao Song
HISTORY:
	Date		Name		Content	
 2016-03-31	Miao Song		Written code
 2016-03-31	Miao Song		Added debugging info
************************************************************************/
RC freeRecord(Record *record){
	free(record->data);
	free(record);
	return RC_OK;
}


/************************************************************************
Function Name: getAttr
Description:
	Function to get an attribute’s value from the table
Parameters:
	Record : Record for which attribute/column has to be fetched.
	Schema : Schema for the record.
	attrNum : Attribute number.
	value : value of the fetched attribute
Return:
	Returns RC_OK on success 
Author:
	Miao Song
HISTORY:
	Date		Name		Content	
 2016-03-31	Miao Song		Written code
 2016-04-01	Chris Dani		Improved readability
************************************************************************/
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value){
	int i, tempint, length,offset = 0;
	float tempf; bool tempb;
	char * stringV;
	for (i=0; i<schema->numAttr; i++){
		if (i == attrNum){
			/* get all the values from the memory, the addrss at the memory is record->data */
			switch(schema->dataTypes[i]){
				case DT_STRING:
					length = schema->typeLength[i];
					stringV = (char *)malloc(length);
					memcpy(stringV, (record->data+offset), length);
					MAKE_STRING_VALUE(*value,stringV);
					break;
				case DT_INT:
					memcpy(&tempint,(record->data+offset),sizeof(int));
					MAKE_VALUE(*value, schema->dataTypes[i], tempint);
					break;	
				case DT_FLOAT:
					memcpy(&tempf,(record->data+offset),sizeof(float));
					MAKE_VALUE(*value, schema->dataTypes[i], tempf);
					break;
				case DT_BOOL:
					memcpy(&tempb,(record->data+offset),sizeof(bool));
					MAKE_VALUE(*value, schema->dataTypes[i], tempb);
					break;
					
			}
		} else {
			switch(schema->dataTypes[i]){
			case DT_INT:
				offset += sizeof(int);
				break;
			case DT_STRING:
				offset += schema->typeLength[i];
				break;
			case DT_FLOAT:
				offset +=  sizeof(float);
				break;
			case DT_BOOL:
				offset += sizeof(bool);
				break;
			}
		
		}
	
	}
	return RC_OK;
}

/************************************************************************
Function Name: setAttr
Description:
	Function to set an attribute’s value in a record
Parameters:
	Record : Record for which attribute/column has to be set.
	Schema : Schema for the record.
	attrNum : Attribute number.
	value : Attribute value that needs to be set. 
Return:
	Returns RC_OK on success 
Author:
	Miao Song
HISTORY:
	Date		Name		Content	
 2016-03-31	Miao Song		Written code
 2016-04-01	Nikhita Kataria	Fixed improper updation
************************************************************************/
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value){
	int i, offset = 0;
	int length;
	for (i=0; i<schema->numAttr; i++){
		if (i == attrNum){
			switch(schema->dataTypes[i]){
				/* store all the values into the memory, the address of memory is record->data */
				case DT_STRING:
					length = schema->typeLength[i];
					memcpy((record->data+offset),value->v.stringV, length);
					break;
				case DT_INT:
					memcpy((record->data+offset),&(value->v.intV), sizeof(int));
						break;
				case DT_FLOAT:
					memcpy((record->data+offset),&(value->v.floatV), sizeof(float));
					break;
				case DT_BOOL:
					memcpy((record->data+offset),&(value->v.boolV), sizeof(bool));
					break;
			}
		} else {
			switch(schema->dataTypes[i]){
			case DT_INT:
				offset += sizeof(int);
				break;
			case DT_STRING:
				offset += schema->typeLength[i];
				break;
			case DT_FLOAT:
				offset +=  sizeof(float);
				break;
			case DT_BOOL:
				offset += sizeof(bool);
				break;
			}
		
		}
	
	}
	return RC_OK;
}

/************************************************************************
Function Name: initRecordManager
Description:
	Initializes the Record Manager using mgmtData.
Parameters:
	mgmtData
Return:
	Returns RC_OK on success
Author:
	Jonathan Yang
HISTORY:
	Date		Name		Content	
 2016-03-28	Jonathan Yang	Written code
************************************************************************/
RC initRecordManager(void *mgmtData){
	return RC_OK;
}

/************************************************************************
Function Name: shutdownRecordManager
Description:
	Shutdown a record manager
Parameters:
	(none)
Return:
	Returns RC_OK on success
Author:
	Chris Dani
HISTORY:
	Date		Name		Content	
 2016-03-28	Chris Dani		Written code
 ************************************************************************/
RC shutdownRecordManager(){
	return RC_OK;
}


/************************************************************************
Function Name: startScan
Description:
	Start scanning the records satisfying a particular expression
Parameters:
	RM_TableData : Metadata for the table (name, schema and management information)
	RM_ScanHandle : Scan Handle to iterate over the table data (RM_TableData)
	Expr : Expression stating the condition on which scan should occur. 
Return:
	Returns RC_OK on success
Author:
	Nikhita Kataria
HISTORY:
	Date		Name		Content	
 2016-03-31	Nikhita Kataria	Written code 
 2016-04-01	Nikhita Kataria	Improved scanning
************************************************************************/
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
	
	scanmgmt *scan_mgmt = (scanmgmt *)malloc(sizeof(scanmgmt));
	scan_mgmt->cur_page_index = 1;
	scan_mgmt->cur_slot_index = 0;
	scan_mgmt->cond = cond;
	scan->rel = rel;
	scan->mgmtData= scan_mgmt;
	
	return RC_OK;
	
}

/************************************************************************
Function Name: next
Description:
	Get next record from the scan
Parameters:
	RM_ScanHandle : Scan Handle to iterate over the table data (RM_TableData)
	Record : Memory to populate the information about the fetched record. 
Return:
	Returns RC_OK on success
Author:
	Nikhita Kataria
HISTORY:
	Date		Name		Content	
 2016-04-01	Nikhita Kataria	Written code 
 2016-04-01	Miao Song		Added debugging information
************************************************************************/
RC next(RM_ScanHandle *scan, Record *record){
	Value *value; 
	BM_PageHandle * page;
	int usedslots;
	
	
	tablemgmt *mgmt = (tablemgmt *)scan->rel->mgmtData;
	scanmgmt *scan_mgmt = (scanmgmt *)scan->mgmtData;
	
	record->id.page = scan_mgmt->cur_page_index;
	record->id.slot = scan_mgmt->cur_slot_index;
	
	/* grasp the record info from the table */
	getRecord(scan->rel, record->id, record);
	/* evaluate the record info matches the search condition */
	evalExpr(record, scan->rel->schema, scan_mgmt->cond, &value);
	/* get used slots number info for position trace */
	page = &(mgmt->pagelist[scan_mgmt->cur_page_index]);
	memcpy(&usedslots, page->data+sizeof(int), sizeof(int));
	
	if(usedslots == record->id.slot)
	{
		if (scan_mgmt->cur_page_index == (mgmt->totalpages-1)){
			return RC_RM_NO_MORE_TUPLES;	// running to the end
		}else {	
		scan_mgmt->cur_page_index++;
		scan_mgmt->cur_slot_index = 0; // switch to the next page
		}
	} else {
		scan_mgmt->cur_slot_index++;
	}
	
	if (value->v.boolV != 1){
		/* the result is not what we want, continue */
		return next(scan,record);
	} else {
		
		return RC_OK;
	}

}


/************************************************************************
Function Name: closeScan
Description:
	Close the scanHandle
Parameters:
	RM_ScanHandle : Scan Handle to be closed. 
Return:
	Returns RC_OK on success
Author:
	Nikhita Kataria
HISTORY:
	Date		Name		Content	
 2016-04-01	Nikhita Kataria	Written code 
************************************************************************/                    
RC closeScan (RM_ScanHandle *scan){
	
	scanmgmt *scan_mgmt = (scanmgmt *)scan->mgmtData;
	free(scan_mgmt);
	scan->mgmtData = NULL;
	return RC_OK;
}


/************************************************************************
Function Name: createTable
Description:
	Create the table and the page file
Parameters:
	name : Name of the table
	schema : Schema Metadata (Attributes, Datatypes, Size of Attributes)
Return:
	Returns RC_OK on success
Author:
	Miao Song
HISTORY:
	Date		Name		Content	
 2016-03-30	Miao Song		Written code
 2016-03-31	Nikhita Kataria	Added debugging information
************************************************************************/
RC createTable(char *name, Schema *schema){
	int table_page_size = 0, i;
    SM_FileHandle fh;
	SM_PageHandle ph;
    createPageFile(name);
    openPageFile(name, &fh);
	ensureCapacity(32, &fh); // ensure the table can be fit in the memory
    ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
	storeSchemaContent(ph,schema);
	writeBlock(0, &fh, ph);
	closePageFile(&fh);
}


/************************************************************************
Function Name: openTable
Description:
	Open the specified table
Parameters:
	name : Name of the table to be opened
	RM_TableData : Metadata for the table (name, schema and management information)
Return:
	Returns RC_OK on success
Author:
	Nikhita Kataria
HISTORY:
	Date		Name		Content	
 2016-03-28	Nikhita Kataria	Written code
 2016-03-30	Nikhita Kataria	Added debugging information
************************************************************************/
RC openTable(RM_TableData *rel, char *name){
	/* Initate the data structure to store the future infomraiton */
	BM_BufferPool *bm = MAKE_POOL();
    initBufferPool(bm, name, 35, RS_FIFO, NULL);
	tablemgmt *mgmt = MAKE_tablemgmt();
	mgmt->record_start_page_index = -1;
	mgmt->record_end_page_index = -1;
	mgmt->totalpages = 0;
	mgmt->bm = bm;
	rel->mgmtData = mgmt;
	rel->schema = (Schema *)malloc(sizeof(Schema));
	rel->name = name;
	
	SM_FileHandle fh;
	openPageFile(name, &fh);
	mgmt->totalpages = fh.totalNumPages;
	closePageFile(&fh);
	int pageindex, maxslots, usedslots=0, slotsize;
	BM_PageHandle *pagelist = (BM_PageHandle *) malloc(sizeof(BM_PageHandle) * mgmt->totalpages);
	mgmt->pagelist= pagelist;
	if (mgmt->totalpages > 1){
		mgmt->record_start_page_index = 1; // the page number to store schema data is 0
	}
	
	 
	BM_PageHandle * page;
	
	for (pageindex=0; pageindex< mgmt->totalpages; pageindex++){

		page = &(mgmt->pagelist[pageindex]);
		pinPage(mgmt->bm, page, pageindex);
		if(pageindex == 0){
			/* fill in the schema information from the page memory, the address of each page memory in buffer pool is page->data */
			retriveSchemaContent(page->data, rel->schema);
	
		} else {
			int pageindex, maxslots, usedslots=0, slotsize;
			slotsize = getRecordSize(rel->schema);
			memcpy(&maxslots,page->data, sizeof(int));
			if(maxslots == 0){
				/* this is the newly created page with no page header information filled */
				maxslots=(PAGE_SIZE-sizeof(pageheader))/slotsize;
				memcpy(page->data, &maxslots, sizeof(int));
				memcpy(page->data+sizeof(int), &usedslots, sizeof(int)); // write back
			} 
		}
	}
	
	return RC_OK;
}


/************************************************************************
Function Name: destroy
Description:
	Destroy the tablemgmt information, release memory
Parameters:
	*mgmt: the pointer of the to-be-deleted tablemgmt structure
Return:
	void
Author:
	Miao Song
HISTORY:
	Date		Name		Content	
 2016-03-31	Miao Song	    Written code
 2016-03-30	Miao Song		Added debugging information
************************************************************************/
void destroy(tablemgmt *mgmt){
	int i;
	for (i=0; i<mgmt->totalpages; i++){
		/* write the memory data into disk */
		unpinPage(mgmt->bm, &(mgmt->pagelist[i]));
		forcePage(mgmt->bm, &(mgmt->pagelist[i]));
	}
	shutdownBufferPool(mgmt->bm);
	free(mgmt->bm); // free the allocation space
	free(mgmt->pagelist); // free the allocation space
	mgmt->bm = NULL;
	mgmt->pagelist = NULL;
	
}


/************************************************************************
Function Name: closeTable
Description:
	Closes the table with the name RM_TableData->name
Parameters:
	RM_TableData : Metadata for the table (name, schema and management information) to be closed
Return:
	Returns RC_OK on success
Author:
	Miao Song
HISTORY:
	Date		Name		Content	
 2016-03-28	Miao Song	    Written code
 2016-03-30	Miao Song		Added debugging information
************************************************************************/
RC closeTable(RM_TableData *rel){
	int i;
	tablemgmt *mgmt = (tablemgmt *)rel->mgmtData;
	destroy(mgmt);
	free(rel->mgmtData);
	rel->mgmtData = NULL;
    freeSchema(rel->schema); // remember to free schema allocation space
	rel->schema = NULL;
    return RC_OK;
	
}

/************************************************************************
Function Name: deleteTable
Description:
	Deletes the table specified in name parameter from the memory
Parameters:
	name : Name of the table to be deleted
Return:
	Returns RC_OK on success
Author:
	Miao Song
HISTORY:
	Date		Name		Content	
 2016-03-29	Miao Song		Written code
 2016-03-30	Miao Song		Fixed pointer assignment
************************************************************************/
RC deleteTable (char *name){
	return destroyPageFile(name);	
}


/************************************************************************
Function Name: getNumTuples
Description:
	Deletes the table specified in name parameter from the memory
Parameters:
	RM_TableData : Metadata for the table (name, schema and management information)
Return:
	Returns the number of tuples in the table with name RM_TableData->name
Author:
	Miao Song
HISTORY:
	Date		Name		Content	
 2016-03-30	Miao Song		Written code
 2016-03-30	Miao Song		Fixed Seg fault
 2016-03-31	Chris Dani		Added debugging information
************************************************************************/
int getNumTuples (RM_TableData *rel){
	tablemgmt *mgmt = (tablemgmt *)rel->mgmtData;
	BM_PageHandle * page;
	int i, numbertuples = 0;
	/* go through all the loaded blocks to iterate the inserted tuples number*/
	for (i=1; i<mgmt->totalpages; i++){
		int tupleperpage;
		page = &(mgmt->pagelist[i]);
		memcpy(&tupleperpage, page->data+sizeof(int), sizeof(int));
		numbertuples += tupleperpage;
	}
	return numbertuples;
}


/************************************************************************
Function Name: insertRecord
Description:
	Inserts a record into the table
Parameters:
	RM_TableData : Metadata for the table (name, schema and management information)
	Record : Information about the record to be inserted 
Return:
	Returns RC_OK on success
Author:
	Chris Dani
HISTORY:
	Date		Name		Content	
 2016-03-31	Chris Dani		Written code
 2016-03-31	Jonathan Yang	Fixed error handling for insertion failure
************************************************************************/
RC insertRecord (RM_TableData *rel, Record *record){
	
	tablemgmt *mgmt = (tablemgmt *)rel->mgmtData;
	int slot_size = getRecordSize(rel->schema);
	int i;
	int maxslots = (PAGE_SIZE-sizeof(pageheader))/getRecordSize(rel->schema);
	int usedslots;
	 
	BM_PageHandle * page;
	for (i=1; i<mgmt->totalpages; i++){
		/* go through all the loaded blocks to iterate the inserted tuples, the start memory addres of each page frame is page->data */
		page = &(mgmt->pagelist[i]);
	    memcpy(&usedslots,page->data+sizeof(int), sizeof(int));
		if((maxslots-usedslots)> 0){
			memcpy(page->data+sizeof(pageheader)+usedslots*getRecordSize(rel->schema), record->data, getRecordSize(rel->schema));
			record->id.page = page->pageNum;
			record->id.slot = usedslots;
			usedslots ++;	
        	memcpy(page->data+sizeof(int), &usedslots, sizeof(int));		   
			return RC_OK;
		} 
			
	}

}


/************************************************************************
Function Name: deleteRecord
Description:
	Deletes a record from the table
Parameters:
	RM_TableData : Metadata for the table (name, schema and management information)
	RID : ID of the record to be deleted. 
Return:
	Returns RC_OK on success
Author:
	Chris Dani
HISTORY:
	Date		Name		Content	
 2016-03-31	Chris Dani		Written code
 2016-03-31	Chris Dani		Added debugging information
************************************************************************/
RC deleteRecord (RM_TableData *rel, RID id){
	tablemgmt *mgmt = (tablemgmt *)rel->mgmtData;
	int i; BM_PageHandle * page;
	for (i=0; i<mgmt->totalpages; i++){
		/* go through all the loaded blocks to iterate the inserted tuples, the start memory addres of each page frame is page->data */
		page = &(mgmt->pagelist[i]);
		if(page->pageNum == id.page){
			memset(page->data+sizeof(pageheader)+id.slot*getRecordSize(rel->schema), '\0', getRecordSize(rel->schema));
		}
	}
	
	return RC_OK;
	
}


/************************************************************************
Function Name: updateRecord
Description:
	Update a record in the table
Parameters:
	RM_TableData : Metadata for the table (name, schema and management information)
	Record : Information about the record to be updated 
Return:
	Returns RC_OK on success
Author:
	Chris Dani
HISTORY:
	Date		Name		Content	
 2016-03-30	Chris Dani		Written code
 2016-03-31	Miao Song		Added debugging information
************************************************************************/
RC updateRecord (RM_TableData *rel, Record *record){
	tablemgmt *mgmt = (tablemgmt *)rel->mgmtData;
	RID id = record->id;
	int i;
	BM_PageHandle * page;
	for (i=0; i<mgmt->totalpages; i++){
		/* go through all the loaded blocks to iterate the inserted tuples, the start memory addres of each page frame is page->data */
		page = &(mgmt->pagelist[i]);
		if(page->pageNum == id.page){
			memcpy(page->data+sizeof(pageheader)+id.slot*getRecordSize(rel->schema), record->data, getRecordSize(rel->schema));
		}
	}
	return RC_OK;
		
	
}


/************************************************************************
Function Name: getRecord
Description:
	Get a record from the table
Parameters:
	RM_TableData : Metadata for the table (name, schema and management information)
	RID : ID of the record to be fetched. 
	Record : Information about the record updated once record information is 
		 fetched using RID.
Return:
	Returns RC_OK on success
Author:
	Chris Dani
HISTORY:
	Date		Name		Content	
 2016-03-31	Chris Dani		Written code
 2016-03-31	Chris Dani		Fixed minor bug and updated the debugging info
************************************************************************/
RC getRecord (RM_TableData *rel, RID id, Record *record){
	tablemgmt *mgmt = (tablemgmt *)rel->mgmtData;
	record->id = id;
	BM_PageHandle * page; int i;
	for (i=0; i<mgmt->totalpages; i++){
		/* go through all the loaded blocks to iterate the inserted tuples, the start memory addres of each page frame is page->data */
		page = &(mgmt->pagelist[i]);
		if(page->pageNum == id.page){
			memcpy(record->data,page->data+sizeof(pageheader)+id.slot*getRecordSize(rel->schema),getRecordSize(rel->schema));
			
		}
		
	}
	return RC_OK;

}



