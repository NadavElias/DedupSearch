/*
 * search.h
 *
 *  Created on: Sep 21, 2020
 *      Author: Nadav Elias
 */

#ifndef SEARCH_H_
#define SEARCH_H_

#include "utils/sync_queue.h"
#include <db.h>

extern SyncQueue *restore_chunk_queue;
extern SyncQueue *restore_recipe_queue;
extern SyncQueue *restore_container_queue; // DedupSearch
extern SyncQueue *chunk_search_results_queue; // DedupSearch

void* assembly_restore_thread(void *arg);
void* optimal_restore_thread(void *arg);

#endif /* SEARCH_H_ */
