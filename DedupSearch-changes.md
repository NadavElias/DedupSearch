Changes made for DedupSearch on Destor:

DedupSearch-master/src/jcr.h:  
  Add global variables

DedupSearch-master/src/jcr.c:  
  Add initialization for new global variables
  
DedupSearch-master/src/search.h:  
  New header file for the main logic of DedupSearch

DedupSearch-master/src/destor.h:  
  Add defines for naive and DedupSearch commands
  
DedupSearch-master/src/storage/containerstore.c:  
  Fix a small bug of Destor
  
DedupSearch-master/src/storage/containerstore.h:  
  Add some fundations for a function that scans a whole container

DedupSearch-master/src/restore.h:  
  Change the queues to be external for access from search.c

DedupSearch-master/src/backup.h:  
  Change the queues to be external for access from search.c

DedupSearch-master/src/utils/sds.h:  
  Change sds typedef to unsigned char for handling non-ASCII characters

DedupSearch-master/src/aho_corasick.hpp:  
  Add the Aho-Corasick code

DedupSearch-master/src/do_search.c:  
  New code file for the main logic of DedupSearch
