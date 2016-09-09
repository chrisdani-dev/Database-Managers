all : make_my_files

make_my_files : cleanup test_assign 

cleanup:
	rm -rf storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o record_mgr.o test_assign 

test_assign : test_assign3_1.c
	gcc -c record_mgr.c -o record_mgr.o
	gcc -c buffer_mgr.c -o buffer_mgr.o
	gcc -c buffer_mgr_stat.c -o buffer_mgr_stat.o
	gcc -c dberror.c -o dberror.o
	gcc -c storage_mgr.c -o storage_mgr.o
	gcc -c expr.c -o expr.o
	gcc -c rm_serializer.c -o rm_serializer.o 
	gcc test_assign3_1.c record_mgr.o storage_mgr.o dberror.o buffer_mgr_stat.o buffer_mgr.o expr.o rm_serializer.o -o test_assign
	
