/*
* Copyright (C) 2021 Nadav Elias.
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*/

#include "destor.h"
#include "jcr.h"
#include "recipe/recipestore.h"
#include "storage/containerstore.h"
#include "utils/lru_cache.h"
#include "search.h"
#include "keyword_search.hpp"
#include <iostream>
#include <linux/limits.h>

#define MAX_UINT_8 255
#define FIND 1
#define AHO_CORASICK 2
#define SEARCH_TYPE 2 // 1 for string::find; 2 for aho-corasick
#define TINY // remove to use without tiny
#define TINY_KEYWORDS_PER_BYTE 4

typedef u_int16_t location_t;
typedef u_int16_t keyword_pos_t;

// The per-chunk record
struct chunkSearchResult{
	u_int8_t keyword;
	keyword_pos_t prefix;
	keyword_pos_t suffix;
	u_int8_t exact_num;
	location_t first_exact;
};

struct csrQueueObj{
	struct chunkSearchResult* csr;
	u_int16_t prev_prefix; // 1B for suf/pref + 1B for start/end file signal
	u_int8_t prev_exact;
};

struct csrQueueObjWrap{
	int flag;
	unsigned char* file_path;
	u_int64_t offset;
	fingerprint* fp;
	#ifdef TINY
	u_int64_t size;
	fingerprint* prev_fp;
	fingerprint* next_fp;
	#endif
	GArray* csr_qobj_list;
};

static int keywords_num;
static const sds *keywords;
static int tiny_value_size;
static keyword_pos_t* keywords_len;
static struct tries search_tries;
// Databases:
static GHashTable *search_results;
static GHashTable *locations_lists;
static GArray*** concatenation_table;
#ifdef TINY
static DB *tiny_dbp; /* DB structure handle */
#endif

// variables for log
static int exceed_exact_chunk = 0; // counts the number of chunks that contains more than 254 exact results.
static u_int64_t exact = 0;
static u_int64_t concat_match = 0;
static u_int64_t pre_suf = 0;
static u_int64_t prefix_1 = 0;
static u_int64_t prefix_2 = 0;
static u_int64_t suffix_1 = 0;
static u_int64_t suffix_2 = 0;
static u_int64_t prefix_1_only = 0;
static u_int64_t prefix_2_only = 0;
static u_int64_t suffix_1_only = 0;
static u_int64_t suffix_2_only = 0;
static u_int64_t concat_try = 0;
static u_int64_t prefix_1_access = 0;
static u_int64_t prefix_2_access = 0;
static u_int64_t suffix_1_access = 0;
static u_int64_t suffix_2_access = 0;
static u_int64_t tiny_objects = 0;
static u_int64_t tiny_keyword_objects = 0;
static u_int64_t chunk_keyword_res_num = 0;
static u_int64_t tiny_access = 0;
static u_int64_t tiny_fetch = 0;
static u_int64_t tiny_matches = 0;
static u_int64_t calculated_tiny_size = 0;
static u_int64_t actual_tiny_size = 0;
static int counter = 0;

SyncQueue *restore_container_queue;
SyncQueue *chunk_search_results_queue;

const char log_path_end[] = "/destor-search/log.csv";
char log_path[PATH_MAX + 1] = { 0 };

// add search statistics to log file
static void save_results(char* search_type, u_int64_t search_results_count, int locations_lists_size, int locations_count){
	FILE *fp = fopen(log_path, "a");
	int len_sum = 3 * keywords_num + 1;
	for (int i = 0; i < keywords_num; i++)
		len_sum += keywords_len[i];
	char* keywords_print = malloc(sizeof(char) * len_sum);
	char* keywords_print_idx = keywords_print;
	for (int i = 0; i < keywords_num; i++){
		sprintf(keywords_print_idx, "%s $ ", keywords[i]);
		keywords_print_idx += keywords_len[i] + 3;
	}

	// output csv titles
	/* 
	 * dev-version,
	 * search type,
	 * keywords number,
	 * keywords,
	 * number of search results,
	 * job id,
	 * chunk num,
	 * data size,
	 * actually read container number,
	 * speed factor,
	 * throughput (MB/s),
	 * 
	 * total time (s),
	 * read recipe time (s),
	 * read recipe throughput (MB/s),
	 * read chunk time (s),
	 * read chunk throughput (MB/s),
	 * search chunk time (s),
	 * search chunk throughput (MB/s),
	 * fetch results time (s),
	 * fetch results throughput (MB/s),
	 * search file time (s),
	 * search file throughput (MB/s),
	 * 
	 * init search time,
	 * physical_phase_time,
	 * logical_phase_time,
	 * pre_suf_search_time,
	 * 
	 * search_results_count,
	 * search_results_size,
	 * chunk_keyword_res_num,
	 * locations_lists lists #,
	 * locations_lists locations #,
	 * locations_lists average # locations for fp,
	 * exceed_exact_chunk,
	 * 
	 * exact count,
	 * concat_match,
	 * concat_try,
	 * pre_suf count,
	 * prefix_1 count,
	 * prefix_2 count,
	 * suffix_1 count,
	 * suffix_2 count,
	 * prefix_1_only,
	 * prefix_2_only,
	 * suffix_1_only,
	 * suffix_2_only,
	 * prefix_1_access,
	 * prefix_2_access,
	 * suffix_1_access,
	 * suffix_2_access,
	 * tiny_objects,
	 * tiny_keyword_objects,
	 * calculated_tiny_size,
	 * actual_tiny_size,
	 * tiny_fetch, // when two keywords have a tiny result in the same chunk, the object is fetched only once
	 * tiny_access,
	 * tiny_matches
	 * 
	 */
	if (fp != NULL) {
		fprintf(fp, "%s, %s, %d, %" PRId32 ", %" PRId32 ", %" PRId32 ", %" PRId64 ", %" PRId32 ", %.4f, %.4f, \
				%.3f, %.3f, %.2f, %.3f, %.2f, %.3f, %.2f, %.3f, %.2f, %.3f, %.2f, \
				%.3f, %.3f, %.3f, %.3f, \
				%lu, %lu, %lu, %d, %d, %.2f, %d, \
				%lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu, %lu",
				#if (SEARCH_TYPE == AHO_CORASICK)
					#ifdef TINY
					"aho_corasick",
					#else
					"aho_corasick-noTiny",
					#endif
				#else
					#ifdef TINY
					"find",
					#else
					"find-noTiny",
					#endif
				#endif 
				search_type,
				keywords_num,
				jcr.search_results_num,
				jcr.id,
				jcr.chunk_num,
				jcr.data_size,
				jcr.read_container_num,
				jcr.data_size / (1024.0 * 1024 * jcr.read_container_num),
				jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time),

				jcr.total_time / 1000000,
				jcr.read_recipe_time / 1000000,
				jcr.data_size * 1000000 / jcr.read_recipe_time / 1024 / 1024,
				jcr.read_chunk_time / 1000000,
				jcr.data_size * 1000000 / jcr.read_chunk_time / 1024 / 1024,
				jcr.search_chunk_time / 1000000,
				jcr.data_size * 1000000 / jcr.search_chunk_time / 1024 / 1024,
				jcr.fetch_results_time / 1000000,
				jcr.data_size * 1000000 / jcr.fetch_results_time / 1024 / 1024,
				jcr.search_file_time / 1000000,
				jcr.data_size * 1000000 / jcr.search_file_time / 1024 / 1024,

				jcr.init_search_time / 1000000,
				jcr.physical_phase_time / 1000000, 
				jcr.logical_phase_time / 1000000,
				jcr.pre_suf_search_time / 1000000,

				search_results_count,
				search_results_count * sizeof(fingerprint) + (chunk_keyword_res_num * sizeof(struct chunkSearchResult)),
				chunk_keyword_res_num,
				locations_lists_size,
				locations_count,
				locations_count/(float)locations_lists_size,
				exceed_exact_chunk,

				exact,
				concat_match,
				concat_try,
				pre_suf,
				prefix_1,
				prefix_2,
				suffix_1,
				suffix_2,
				prefix_1_only,
				prefix_2_only,
				suffix_1_only,
				suffix_2_only,
				prefix_1_access,
				prefix_2_access,
				suffix_1_access,
				suffix_2_access,
				tiny_objects,
				tiny_keyword_objects,
				calculated_tiny_size,
				actual_tiny_size,
				tiny_fetch,
				tiny_access,
				tiny_matches
				);

		fprintf(fp, "\n");
		printf("saved results successfully!\n");

		free(keywords_print);
		fclose(fp);
	}
	else {
		puts("Could not open log file! Results will not be saved.\n The path tried to open: ");
		puts(log_path);
		puts("\n");
	}
}

// emit a keyword match in a file
static void print_match(FILE *fp, int keyword_idx, u_int64_t offset){
	fprintf(fp, "    %.*s: %lu\n", keywords_len[keyword_idx], keywords[keyword_idx], offset);
}

// initiate structures before searching: Aho-Corasick tries, Partial-match table
static void* init_search() {
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	
	strncpy(log_path, getenv("HOME"), PATH_MAX);
	strncat(log_path, log_path_end, PATH_MAX);

	// init keywords length array
	keywords_len = malloc(sizeof(keyword_pos_t) * keywords_num);
	for (int i = 0; i < keywords_num; i++){
		keywords_len[i] = sdslen(keywords[i]);
	}

	#if (SEARCH_TYPE == AHO_CORASICK)
	std::vector<u_string> keywords_vec;
	for (int i = 0; i < keywords_num; i++){
		keywords_vec.push_back(u_string(keywords[i], keywords[i] + sdslen(keywords[i])));
	}
	// init Aho-Corasick tries and Partial-match table
	init(keywords_vec, &search_tries);

	#else
	
	GArray* locations;
	location_t loc;
	size_t pos;
	u_string concat, suffix;

	// Partial-match table
	concatenation_table = malloc(keywords_num * sizeof(GArray**));
	for (int i = 0; i < keywords_num; i++){
		concatenation_table[i] = malloc((keywords_len[i]-1)*(keywords_len[i]-1)*sizeof(GArray*));
		u_string keyword(keywords[i]);
		for (keyword_pos_t suf = 1; suf < keywords_len[i]; suf++){
		suffix = keyword.substr(keywords_len[i] - suf);
		for (keyword_pos_t pre = 1; pre < keywords_len[i]; pre++){
			concat = keyword.substr(0, pre).append(suffix);
			pos = -1;
			locations = NULL;
			while ((pos = concat.find(keyword, pos + 1)) != u_string::npos){
				if (locations == NULL){
					locations = g_array_new(FALSE, FALSE, sizeof(location_t));
				}
				loc = pos;
				g_array_append_val(locations, loc);
			}
			concatenation_table[i][(pre-1)*(keywords_len[i]-1) + (suf-1)] = locations;
		}
	}
	}

	#endif

	TIMER_END(1, jcr.init_search_time);
	return NULL;
}

// The second thread of Naïve search
// Destor's restore fetch chunks thread:
// pops the recipes from the recipe queue,
// fetches the corresponding chunks by reading their containers,
// and inserts the chunks in order of their appearance in the
// file to the chunk queue.
// Some of the chunks might be fetched from the operating
// system cache rather than the disk.
static void* lru_restore_thread(void *arg) {
	struct lruCache *cache;
	if (destor.simulation_level >= SIMULATION_RESTORE)
		cache = new_lru_cache(destor.restore_cache[1], free_container_meta,
				lookup_fingerprint_in_container_meta);
	else
		cache = new_lru_cache(destor.restore_cache[1], free_container,
				lookup_fingerprint_in_container);

	struct chunk* c;
	while ((c = sync_queue_pop(restore_recipe_queue))) {

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			sync_queue_push(restore_chunk_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		if (destor.simulation_level >= SIMULATION_RESTORE) {
			struct containerMeta *cm = lru_cache_lookup(cache, &c->fp);
			if (!cm) {
				VERBOSE("Restore cache: container %lld is missed", c->id);
				cm = retrieve_container_meta_by_id(c->id);
				assert(lookup_fingerprint_in_container_meta(cm, &c->fp));
				lru_cache_insert(cache, cm, NULL, NULL);
				jcr.read_container_num++;
			}

			TIMER_END(1, jcr.read_chunk_time);
		} else {
			struct container *con = lru_cache_lookup(cache, &c->fp);
			if (!con) {
				VERBOSE("Restore cache: container %lld is missed", c->id);
				con = retrieve_container_by_id(c->id);
				lru_cache_insert(cache, con, NULL, NULL);
				jcr.read_container_num++;
			}
			// The following chunk fetch might use the
			// operating system cache rather than the disk.
			struct chunk *rc = get_chunk_in_container(con, &c->fp);
			assert(rc);
			TIMER_END(1, jcr.read_chunk_time);
			sync_queue_push(restore_chunk_queue, rc);
		}

		jcr.data_size += c->size;
		jcr.chunk_num++;
		free_chunk(c);
	}

	sync_queue_term(restore_chunk_queue);

	free_lru_cache(cache);

	return NULL;
}

// The first thread of Naïve search & first thread of logical phase:
// Destore's restore read recipes thread:
// reads the file recipes and inserts them into the recipe queue
static void* read_recipe_thread(void *arg) {

	int i, j, k;
	for (i = 0; i < jcr.bv->number_of_files; i++) {
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		struct fileRecipeMeta *r = read_next_file_recipe_meta(jcr.bv);

		struct chunk *c = new_chunk(sdslen(r->filename) + 1);
		strcpy(c->data, r->filename);
		SET_CHUNK(c, CHUNK_FILE_START);

		TIMER_END(1, jcr.read_recipe_time);

		sync_queue_push(restore_recipe_queue, c);

		for (j = 0; j < r->chunknum; j++) {
			TIMER_DECLARE(1);
			TIMER_BEGIN(1);

			struct chunkPointer* cp = read_next_n_chunk_pointers(jcr.bv, 1, &k);

			struct chunk* c = new_chunk(0);
			memcpy(&c->fp, &cp->fp, sizeof(fingerprint));
			c->size = cp->size;
			c->id = cp->id;

			TIMER_END(1, jcr.read_recipe_time);

			sync_queue_push(restore_recipe_queue, c);
			free(cp);
		}

		c = new_chunk(0);
		SET_CHUNK(c, CHUNK_FILE_END);
		sync_queue_push(restore_recipe_queue, c);

		free_file_recipe_meta(r);
	}

	sync_queue_term(restore_recipe_queue);
	return NULL;
}

// The third thread of Naïve search
// based on the last thread of Destore’s restore mechanism:
// instead of writing the chunk’s data, it is processed with
// the Aho-Corasick trie of the input keywords.
// To identify keywords that are split between chunks, the last n−1 characters
// (where n is the length of the longest keyword) of the previous
// chunk are concatenated to the beginning of the current chunk.
void* search_restore_data(void *arg) {
	// file for search results output
	FILE *fp = fopen("search_results.out", "w");
	struct chunk *c = NULL;
	sds filepath = NULL;
	unsigned char* sub_str;
	u_int64_t offset;
	int first;
	u_string data;
	
	int max_keyword_len = keywords_len[0];
	for (int i = 1; i < keywords_num; i++){
		max_keyword_len = MAX(max_keyword_len, keywords_len[i]);
	}

	while ((c = sync_queue_pop(restore_chunk_queue))) {

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		if (CHECK_CHUNK(c, CHUNK_FILE_START)) {
			VERBOSE("Searching: %s", c->data);

			filepath = sdsdup(c->data);
			offset = 0;
			first = TRUE;
			data = u_string();
		} else if (CHECK_CHUNK(c, CHUNK_FILE_END)) {
		    jcr.file_num++;

			if (filepath)
				sdsfree(filepath);
			filepath = NULL;
		} else {
			assert(destor.simulation_level == SIMULATION_NO);
			VERBOSE("Searching %d bytes", c->size);

			// the last n−1 character (where n is the length of the longest keyword) of
			// the previous chunk are concatenated to the beginning of the current chunk
			// to identify keywords that are split between chunks
			keyword_pos_t prev_len = data.size();
			data.append((unsigned char*)(c->data), c->size);

			#if (SEARCH_TYPE == AHO_CORASICK)

			std::vector<u_string> chunks_temp ({data});
			trie::emit_collection chunk_res = search_block(chunks_temp, true, &search_tries)[0];

			for (auto& match : chunk_res) { // over all keywords
				int adjust = MIN(prev_len, (max_keyword_len - keywords_len[match.first]));
				for (auto& pair : match.second) { // over all results: pair = (match_type, (length, locations))
					if (pair.first == aho_corasick::FULL)
					{
						if (first == TRUE){
							first = FALSE;
							fprintf(fp, "%s:\n", filepath);
						}
						std::vector<unsigned> locations = pair.second.second; // was list
						jcr.search_results_num += locations.size();
						for (int i = 0; i < locations.size(); i++){
							if (locations[i] >= adjust)
								print_match(fp, match.first, offset - prev_len + locations[i]);
							else
								jcr.search_results_num--;
						}
					}
				}
			}

			#elif (SEARCH_TYPE == FIND)

			for (int i = 0; i < keywords_num; i++){
				size_t loc = -1;
				int adjust = MIN(prev_len, (max_keyword_len - keywords_len[i]));
				while ((loc = data.find(keywords[i], adjust + loc + 1)) != u_string::npos){
					if (first == TRUE){
						first = FALSE;
						fprintf(fp, "%s:\n", filepath);
					}
					jcr.search_results_num++;
					print_match(fp, i, offset - prev_len + loc);
				}
			}
			#endif

			data = data.substr(MAX(0, (c->size + prev_len) - (max_keyword_len - 1)));
			offset += c->size;
		}

		free_chunk(c);

		TIMER_END(1, jcr.search_chunk_time);
	}

	fclose(fp);
    jcr.status = JCR_STATUS_DONE;
    return NULL;
}

// Main Naïve search function
void do_naive_search(int revision, sds *keywords_p, size_t keywords_num_p) {
	keywords_num = keywords_num_p;
	keywords = keywords_p;

	destor_log(DESTOR_NOTICE, "naive search");

	init_recipe_store();
	init_container_store();

	init_restore_jcr(revision, ".");

	destor_log(DESTOR_NOTICE, "job id: %d", jcr.id);
	destor_log(DESTOR_NOTICE, "backup path: %s", jcr.bv->path);
	destor_log(DESTOR_NOTICE, "%d strings to search", keywords_num);
	for (int i = 0; i < keywords_num; i++)
		printf("%s, ", keywords[i]);
	printf("\n");

	restore_chunk_queue = sync_queue_new(100);
	restore_recipe_queue = sync_queue_new(100);

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	
	// Naïve search starts
	puts("==== search begin ====");

    jcr.status = JCR_STATUS_RUNNING;
	pthread_t recipe_t, read_t, search_t;

	#if (SEARCH_TYPE == AHO_CORASICK)
	init_search();
	#else
	keywords_len = malloc(sizeof(keyword_pos_t) * keywords_num);
	for (int i = 0; i < keywords_num; i++)
		keywords_len[i] = sdslen(keywords[i]);
	#endif

	pthread_create(&recipe_t, NULL, read_recipe_thread, NULL);

	if (destor.restore_cache[0] == RESTORE_CACHE_LRU) {
		destor_log(DESTOR_NOTICE, "restore cache is LRU");
		pthread_create(&read_t, NULL, lru_restore_thread, NULL);
	} else if (destor.restore_cache[0] == RESTORE_CACHE_OPT) {
		destor_log(DESTOR_NOTICE, "restore cache is OPT");
		pthread_create(&read_t, NULL, optimal_restore_thread, NULL);
	} else if (destor.restore_cache[0] == RESTORE_CACHE_ASM) {
		destor_log(DESTOR_NOTICE, "restore cache is ASM");
		pthread_create(&read_t, NULL, assembly_restore_thread, NULL);
	} else {
		fprintf(stderr, "Invalid restore cache.\n");
		exit(1);
	}

	pthread_create(&search_t, NULL, search_restore_data, NULL);

    do{
        sleep(5);
        /*time_t now = time(NULL);*/
        fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed, %d search results, %.3f read chunk time\r", 
                jcr.data_size, jcr.chunk_num, jcr.file_num, jcr.search_results_num, jcr.read_chunk_time / 1000000);
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
    fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed, %d search results, %.3f read chunk time\n", 
        jcr.data_size, jcr.chunk_num, jcr.file_num, jcr.search_results_num, jcr.read_chunk_time / 1000000);

	assert(sync_queue_size(restore_chunk_queue) == 0);
	assert(sync_queue_size(restore_recipe_queue) == 0);

	free_backup_version(jcr.bv);

	TIMER_END(1, jcr.total_time);
	// Naïve search ends
	puts("==== search end ====");

	printf("job id: %" PRId32 "\n", jcr.id);
	printf("strings to search: ");
	for (int i = 0; i < keywords_num; i++)
		printf("%s, ", keywords[i]);
	printf("\n");
	printf("number of search results: %" PRId32 "\n", jcr.search_results_num);
	printf("number of files: %" PRId32 "\n", jcr.file_num);
	printf("number of chunks: %" PRId32"\n", jcr.chunk_num);
	printf("total size(B): %" PRId64 "\n", jcr.data_size);
	printf("total time(s): %.3f\n", jcr.total_time / 1000000);
	printf("throughput(MB/s): %.2f\n",
			jcr.data_size * 1000000 / (1024.0 * 1024 * jcr.total_time));
	printf("speed factor: %.2f\n",
			jcr.data_size / (1024.0 * 1024 * jcr.read_container_num));

	printf("read_recipe_time : %.3fs, %.2fMB/s\n",
			jcr.read_recipe_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_recipe_time / 1024 / 1024);
	printf("read_chunk_time : %.3fs, %.2fMB/s\n", jcr.read_chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_chunk_time / 1024 / 1024);
	printf("search_chunk_time : %.3fs, %.2fMB/s\n",
			jcr.search_chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.search_chunk_time / 1024 / 1024);
	printf("counter: %d\n", counter);
	

	save_results("naive", 0, 0, 0);

	close_container_store();
	close_recipe_store();
}

// ==================================== DedupSearch ====================================================

void g_array_free_wrap (GArray *array){
	g_array_free (array, TRUE);
}

void g_array_free_wrap_ext (GArray *array){
	for (u_int64_t i = 0; i < array->len; i++){
		GArray* arr = (g_array_index(array, GArray*, i));
		g_array_free (arr, TRUE);
	}
	g_array_free (array, FALSE);
}

static void free_concatenation_table(){
	GArray* arr;
	for (int i = 0; i < keywords_num; i++){
		for (keyword_pos_t suf = 1; suf < keywords_len[i]; suf++){
			for (keyword_pos_t pre = 1; pre < keywords_len[i]; pre++){
				arr = concatenation_table[i][(pre-1)*(keywords_len[i]-1) + (suf-1)];
				if (arr != NULL)
					g_array_free(arr, FALSE);
			}
		}
		free(concatenation_table[i]);
	}
	free(concatenation_table);
}

// Search keywords in a chunk using c++ string find function
static void search_chunk(u_string &data, unsigned char* keyword, size_t keyword_len, struct chunkSearchResult* &csr, GArray* &locations_list){
	csr = NULL;
	locations_list = NULL;
	u_int16_t exact_num = 0;
	size_t pos;
	location_t loc;
	
	pos = data.find(keyword);
	if (pos != u_string::npos){
		exact_num++;
		csr = (struct chunkSearchResult*)malloc(sizeof(struct chunkSearchResult));
		csr->prefix = 0;
		csr->suffix = 0;
		csr->first_exact = (location_t)pos;
		pos = data.find(keyword, pos + 1);
	}
	while (pos != u_string::npos){
		if (locations_list == NULL)
			locations_list = g_array_new(FALSE, FALSE, sizeof(location_t));
		exact_num++;
		loc = pos;
		g_array_append_val(locations_list, loc);
		pos = data.find(keyword, pos + 1);
	}

	// find prefixes and suffixes
	TIMER_DECLARE(0);
	TIMER_BEGIN(0);
	size_t data_len = data.size();
	keyword_pos_t len;
	pos = MIN(data_len, keyword_len - 1) - 1;
	while (pos >= 0 && (pos = data.rfind(keyword[keyword_len-1], pos)) != u_string::npos){
		len = pos + 1;
		if (data.compare(0, len, keyword + keyword_len - len) == 0){
			if (csr == NULL){
				csr = (struct chunkSearchResult*)malloc(sizeof(struct chunkSearchResult));
				csr->first_exact = 0;
				csr->exact_num = 0;
				csr->prefix = 0;
			}
			csr->suffix = len;
			break;
		}
		pos--;
	}
	pos = MAX(0, data_len - keyword_len + 1);
	while (pos <= data_len - 1 && (pos = data.find(keyword[0], pos)) != u_string::npos){
		len = data_len - pos;
		if (data.compare(data_len - len, len, keyword, len) == 0){
			if (csr == NULL){
				csr = (struct chunkSearchResult*)malloc(sizeof(struct chunkSearchResult));
				csr->first_exact = 0;
				csr->exact_num = 0;
				csr->suffix = 0;
			}
			csr->prefix = len;
			break;
		}
		pos++;
	}
	TIMER_END(0, jcr.pre_suf_search_time);
	if(csr != NULL){
		csr->exact_num = exact_num;
		if (exact_num >= MAX_UINT_8){
			g_array_prepend_val(locations_list, exact_num);
			exceed_exact_chunk++;
			csr->exact_num = MAX_UINT_8;
		}
	}
}

// - - - - - - - - - - - - - - - - - - Physical Phase - - - - - - - - - - - - - - - - - - - - - - - - -

// The first thread of the physical search:
// fetch all containers in the system and insert them to container queue
static void* read_container_thread(void *arg) {
	containerid container_id;
	containerid container_count = (containerid)get_container_count();
	// Go over all containers in the system
	for (container_id = 0; container_id < container_count; container_id++){
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		struct container* con = retrieve_container_by_id(container_id); // maybe read more than one container at a time
		
		TIMER_END(1, jcr.read_chunk_time);

		sync_queue_push(restore_container_queue, con);
		jcr.read_container_num++;
	}
	sync_queue_term(restore_container_queue);
	return NULL;
}

// The second and main thread of the physical phase:
// go over all chunks from the container queue and search in each chunk
// of data for the keywords, using Aho-Corasick or C++ string find.
// For each chunk, record the exact matches of the keyword, if it is found,
// as well as prefixes or suffixes of the keyword (partial matches)
// found at chunk boundaries. insert all chunk result records to
// the result databases.
void* search_containers_data(void *arg) {
	u_int16_t exact_num;
	size_t pos;
	location_t loc;

	struct metaEntry* me;
	struct chunkSearchResult* csr;
	struct container* con;

	GHashTableIter iter;
	gpointer key, value;
	GArray* locations_list;
	GArray* csr_list;
	GArray* locations_list_list;
	#ifdef TINY
	char* tiny_value = malloc(tiny_value_size);
	#endif

	// Go over all the container queue
	while ((con = sync_queue_pop(restore_container_queue))) {
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		g_hash_table_iter_init(&iter, con->meta.map);
		// Go over all chunks in a container
		while(g_hash_table_iter_next(&iter, &key, &value)){
			me = (struct metaEntry*)value;
			u_string data = u_string((unsigned char*)(con->data + me->off), me->len);

			csr_list = NULL;
			locations_list_list = NULL;
			#ifdef TINY
			memset(tiny_value, 0, tiny_value_size);
			bool has_tiny = false;
			#endif

			#if (SEARCH_TYPE == FIND)

			for (int i = 0; i < keywords_num; i++){
				search_chunk(data, keywords[i], keywords_len[i], csr, locations_list);

				if (csr != NULL){
					csr->keyword = i;

					if (csr->prefix == 1){
						prefix_1++;
						if (csr->exact_num == 0)
							prefix_1_only++;
					}
					else if (csr->prefix == 2){
						prefix_2++;
						if (csr->exact_num == 0)
							prefix_2_only++;
					}
					if (csr->suffix == 1){
						suffix_1++;
						if (csr->exact_num == 0)
							suffix_1_only++;
					}
					else if (csr->suffix == 2){
						suffix_2++;
						if (csr->exact_num == 0)
							suffix_2_only++;
					}

					#ifdef TINY
					if (csr->exact_num > 0 || csr->prefix > 1 || csr->suffix > 1 || (keywords_len[csr->keyword] == 2 && csr->prefix > 0)){
					#endif
						chunk_keyword_res_num++;
						if (csr->exact_num > 0)
							exact++;
						if (csr->prefix > 0 || csr->suffix > 0)
							pre_suf++;
						if (csr_list == NULL)
							csr_list = g_array_new(FALSE, FALSE, sizeof(struct chunkSearchResult));
						g_array_append_val(csr_list, *csr); // TODO: if copies by value - dont malloc csr
						if (locations_list != NULL){
							if (locations_list_list == NULL)
								locations_list_list = g_array_new(FALSE, FALSE, sizeof(GArray*));
							g_array_append_val(locations_list_list, locations_list);
						}
					#ifdef TINY
					}
					else{ // tiny
						tiny_keyword_objects++;
						has_tiny = true;
						int arr_idx = csr->keyword / TINY_KEYWORDS_PER_BYTE;
						int char_idx = csr->keyword % TINY_KEYWORDS_PER_BYTE;
						tiny_value[arr_idx] |= (1 << (1 + csr->prefix * char_idx * 2)) | (1 << (csr->suffix * char_idx * 2));
						free(csr);
					}
					#endif
				}
			}

			#else

			std::vector<u_string> chunks_temp ({data});
			trie::emit_collection chunk_res = search_block(chunks_temp, false, &search_tries)[0];
			for (auto& match : chunk_res) { // over all keywords
				locations_list = NULL;
				csr = NULL;
				exact_num = 0;
				for (auto& pair : match.second) { // over all results: pair = (match_type, (length, locations))
					switch (pair.first)
					{
					case aho_corasick::FULL:{
						if (csr == NULL){
							csr = (struct chunkSearchResult*)malloc(sizeof(struct chunkSearchResult));
							csr->keyword = match.first;
							csr->prefix = 0;
							csr->suffix = 0;
						}
						std::vector<unsigned> locations = pair.second.second;
						csr->first_exact = (location_t)(locations.front());
						exact_num = locations.size();

						if(locations.size() > 1){
							locations_list = g_array_new(FALSE, FALSE, sizeof(location_t));
							for (int i = 1; i < locations.size(); i++){
								g_array_append_val(locations_list, locations[i]);
							}
						}
						break;}
					case aho_corasick::PREFIX:{
						if (csr == NULL){
							csr = (struct chunkSearchResult*)malloc(sizeof(struct chunkSearchResult));
							csr->keyword = match.first;
							csr->first_exact = 0;
							csr->exact_num = 0;
							csr->suffix = 0;
						}
						csr->prefix = pair.second.first;
						break;}
					case aho_corasick::SUFFIX:{
						if (csr == NULL){
							csr = (struct chunkSearchResult*)malloc(sizeof(struct chunkSearchResult));
							csr->keyword = match.first;
							csr->first_exact = 0;
							csr->exact_num = 0;
							csr->prefix = 0;
						}
						csr->suffix = pair.second.first;
						break;}
					default:
						break;
					}
				}

				if(csr != NULL){
					csr->exact_num = exact_num;
					if (exact_num >= MAX_UINT_8){
						g_array_prepend_val(locations_list, exact_num);
						exceed_exact_chunk++;
						csr->exact_num = MAX_UINT_8;
					}

					
					if (csr->prefix == 1){
						prefix_1++;
						if (csr->exact_num == 0)
							prefix_1_only++;
					}
					else if (csr->prefix == 2){
						prefix_2++;
						if (csr->exact_num == 0)
							prefix_2_only++;
					}
					if (csr->suffix == 1){
						suffix_1++;
						if (csr->exact_num == 0)
							suffix_1_only++;
					}
					else if (csr->suffix == 2){
						suffix_2++;
						if (csr->exact_num == 0)
							suffix_2_only++;
					}

					#ifdef TINY
					if (csr->exact_num > 0 || csr->prefix > 1 || csr->suffix > 1 || (keywords_len[csr->keyword] == 2 && csr->prefix > 0)){
					#endif
						chunk_keyword_res_num++;
						if (csr->exact_num > 0)
							exact++;
						if (csr->prefix > 0 || csr->suffix > 0)
							pre_suf++;
						if (csr_list == NULL)
							csr_list = g_array_new(FALSE, FALSE, sizeof(struct chunkSearchResult));
						g_array_append_val(csr_list, *csr);
						if (locations_list != NULL){
							if (locations_list_list == NULL)
								locations_list_list = g_array_new(FALSE, FALSE, sizeof(GArray*));
							g_array_append_val(locations_list_list, locations_list);
						}
					#ifdef TINY
					}
					else{ // tiny
						tiny_keyword_objects++;
						has_tiny = true;
						int arr_idx = csr->keyword / TINY_KEYWORDS_PER_BYTE;
						int char_idx = csr->keyword % TINY_KEYWORDS_PER_BYTE;
						tiny_value[arr_idx] |= (1 << (1 + csr->prefix * char_idx * 2)) | (1 << (csr->suffix * char_idx * 2));
						free(csr);
					}
					#endif
				}
			}

			#endif

			if(csr_list != NULL){
				fingerprint* fp = (fingerprint*)malloc(sizeof(fingerprint));
				memcpy(*fp, me->fp, sizeof(fingerprint));
				g_hash_table_insert(search_results, fp, csr_list);
				if (locations_list_list != NULL){
					fingerprint* fp = (fingerprint*)malloc(sizeof(fingerprint)); // not sure that needed. change the hash table free if switching
					memcpy(*fp, me->fp, sizeof(fingerprint));
					g_hash_table_insert(locations_lists, fp, locations_list_list); // TODO check if need to malloc the fp also
				}
			}
			
			#ifdef TINY
			if (has_tiny){
				tiny_objects++;
				DBT tiny_key, tiny_data;
				/* Zero out the DBTs before using them. */
				memset(&tiny_key, 0, sizeof(DBT));
				memset(&tiny_data, 0, sizeof(DBT));
				tiny_key.data = me->fp;
				tiny_key.size = sizeof(fingerprint);
				tiny_data.data = tiny_value;
				tiny_data.size = tiny_value_size;
				tiny_dbp->put(tiny_dbp, NULL, &tiny_key, &tiny_data, 0);
			}
			#endif

			jcr.chunk_num++;
			jcr.data_size += me->len;
		}

		free_container(con);

		TIMER_END(1, jcr.search_chunk_time);
	}

    jcr.status = JCR_STATUS_DONE;
    return NULL;
}

// - - - - - - - - - - - - - - - - - - Logical Phase - - - - - - - - - - - - - - - - - - - - - - - - -

// The second thread of the logical phase:
// uses the fingerprints from the recipe queue to fetch chunk-result
// records and inserts them into the result queue with the required
// metadata.
static void* fp_to_search_results(void *arg) {
	struct chunk* c;
	struct chunkSearchResult *csr;
	struct csrQueueObj* csr_qobj;
	struct csrQueueObjWrap* csr_qobj_wrap;
	u_int64_t offset = 0;
	keyword_pos_t* prev_prefix = malloc(sizeof(keyword_pos_t) * keywords_num);
	u_int8_t* prev_exact = malloc(sizeof(u_int8_t) * keywords_num);
	GArray* csr_qobj_list;
	u_int8_t* seen_keywords = malloc(sizeof(u_int8_t) * keywords_num);
	#ifdef TINY
	fingerprint* prev_fp;
	bool first;
	#endif

	// Go over all recipes
	while ((c = sync_queue_pop(restore_recipe_queue))) {
		if (CHECK_CHUNK(c, CHUNK_FILE_START)) {
			for (int i = 0; i < keywords_num; i++){
				prev_prefix[i] = 0;
				prev_exact[i] = 0;
			}
			offset = 0;
			csr_qobj_wrap = (struct csrQueueObjWrap*)malloc(sizeof(struct csrQueueObjWrap));
			csr_qobj_wrap->flag = c->flag;
			csr_qobj_wrap->file_path = sdsdup(c->data);
			sync_queue_push(chunk_search_results_queue, csr_qobj_wrap);
			free_chunk(c);
			#ifdef TINY
			first = true;
			prev_fp = NULL;
			#endif
			continue;
		} else if (CHECK_CHUNK(c, CHUNK_FILE_END)) {
			#ifdef TINY
			if (prev_fp != NULL)
				free(prev_fp); // prev_fp isn't used in last chunk
			// enqueue the object from the *prev* iteration, edge case
			if(!first && csr_qobj_wrap != NULL){
				csr_qobj_wrap->next_fp = NULL;
				sync_queue_push(chunk_search_results_queue, csr_qobj_wrap);
			}
			#endif
			csr_qobj_wrap = (struct csrQueueObjWrap*)malloc(sizeof(struct csrQueueObjWrap));
			csr_qobj_wrap->flag = c->flag;
			sync_queue_push(chunk_search_results_queue, csr_qobj_wrap);
			free_chunk(c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		
		#ifdef TINY
		// enqueue the object from the *prev* iteration so we can know its next fingerprint
		if(!first && csr_qobj_wrap != NULL){ 
			csr_qobj_wrap->next_fp = (fingerprint*)malloc(sizeof(fingerprint));
			memcpy(*(csr_qobj_wrap->next_fp), c->fp, sizeof(fingerprint));
			sync_queue_push(chunk_search_results_queue, csr_qobj_wrap);
		}
		first = false;
		#endif

		// fetch a chunk-result record
		GArray* csr_list = g_hash_table_lookup(search_results, c->fp);

		if (csr_list == NULL){
			#ifdef TINY
			if (prev_fp != NULL)
				free(prev_fp); // prev_fp isn't used
			csr_qobj_wrap = NULL;
			#endif
			for (int i = 0; i < keywords_num; i++){
				prev_prefix[i] = 0; // can be updated once only and not for all?
				prev_exact[i] = 0;
			}
		} else{
			csr_qobj_wrap = (struct csrQueueObjWrap*)malloc(sizeof(struct csrQueueObjWrap));
			csr_qobj_wrap->flag = 0;
			csr_qobj_wrap->offset = offset;
			#ifdef TINY
			csr_qobj_wrap->size = c->size;
			csr_qobj_wrap->prev_fp = prev_fp;
			#endif
			csr_qobj_wrap->fp = (fingerprint*)malloc(sizeof(fingerprint));
			memcpy(*(csr_qobj_wrap->fp), c->fp, sizeof(fingerprint));
			csr_qobj_wrap->csr_qobj_list = g_array_new(FALSE, FALSE, sizeof(struct csrQueueObj*));

			for (int i = 0; i < keywords_num; i++)
				seen_keywords[i] = FALSE;

			for (int i = 0; i < csr_list->len; i++){
				csr = &(g_array_index(csr_list, struct chunkSearchResult, i));
				int k_i = csr->keyword;
			 	csr_qobj = (struct csrQueueObj*)malloc(sizeof(struct csrQueueObj));
				csr_qobj->prev_prefix = prev_prefix[k_i];
				csr_qobj->prev_exact = prev_exact[k_i];
				csr_qobj->csr = csr;
				g_array_append_val(csr_qobj_wrap->csr_qobj_list, csr_qobj);

				seen_keywords[k_i] = TRUE;
				prev_prefix[k_i] = csr->prefix;
				prev_exact[k_i] = csr->exact_num;
			}
			for (int i = 0; i < keywords_num; i++){
				if (seen_keywords[i] == FALSE){
					prev_prefix[i] = 0;
					prev_exact[i] = 0;
				}
			}
			#ifndef TINY
			sync_queue_push(chunk_search_results_queue, csr_qobj_wrap);
			#endif
		}

		offset += c->size;

		#ifdef TINY
		prev_fp = (fingerprint*)malloc(sizeof(fingerprint));
		memcpy(*prev_fp, c->fp, sizeof(fingerprint));
		#endif

		TIMER_END(1, jcr.fetch_results_time);
		free_chunk(c);
	}

	sync_queue_term(chunk_search_results_queue);

	free(seen_keywords);
	free(prev_prefix);
	free(prev_exact);

	return NULL;
}

// The third and last thread of the logical phase:
// pops the result records from the result queue, collects exact matches
// and combines partial matches, fetching tiny and location-list records if needed.
// finally, emits the respective full matches.
void* search_chunks_results(void *arg) {
	// file for search results output
	FILE *fp = fopen("search_results.out", "w+");
	struct csrQueueObj* csr_qobj = NULL;
	struct csrQueueObjWrap* csr_qobj_wrap;
	GArray* locations_list;
	GArray* locations_list_list;
	sds filepath = NULL;
	int first;
	int list_i;
	#ifdef TINY
	unsigned char* tiny_prev_value = malloc(tiny_value_size);
	unsigned char* tiny_next_value = malloc(tiny_value_size);
	#endif

	// Go over all chunk results that were fetched according to the recipes
	while ((csr_qobj_wrap = sync_queue_pop(chunk_search_results_queue))) {
		locations_list_list = NULL;
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		if (CHECK_CHUNK(csr_qobj_wrap, CHUNK_FILE_START)) {
			VERBOSE("Searching: %s", csr_qobj_wrap->file_path);
			filepath = csr_qobj_wrap->file_path;
			first = TRUE;
		} else if (CHECK_CHUNK(csr_qobj_wrap, CHUNK_FILE_END)) {
		    jcr.file_num++;

			if (filepath)
				sdsfree(filepath);
			filepath = NULL;
		} else {
			assert(destor.simulation_level == SIMULATION_NO);

			#ifdef TINY
			bool tiny_prev_fetched = false;
			bool tiny_next_fetched = false;
			#endif
			for (int csr_qobj_i = 0; csr_qobj_i < csr_qobj_wrap->csr_qobj_list->len; csr_qobj_i++){
				csr_qobj = g_array_index(csr_qobj_wrap->csr_qobj_list, struct csrQueueObj*, csr_qobj_i);

				//concatenation
				if (csr_qobj->prev_prefix > 0 && csr_qobj->csr->suffix > 0){
					concat_try++;

					#if(SEARCH_TYPE == AHO_CORASICK)
					std::vector<unsigned> locations = search_concatenation(csr_qobj->csr->keyword, csr_qobj->prev_prefix, csr_qobj->csr->suffix);
					if (locations.size() > 0){
						if (first == TRUE){
							first = FALSE;
							fprintf(fp, "%s:\n", filepath);
						}
						jcr.search_results_num += locations.size();
						for (auto& location : locations){
							print_match(fp, csr_qobj->csr->keyword, csr_qobj_wrap->offset - csr_qobj->prev_prefix + location);
							concat_match++;
						}
					}

					#else
					GArray* locations = concatenation_table[csr_qobj->csr->keyword][(csr_qobj->prev_prefix-1)*(keywords_len[csr_qobj->csr->keyword]-1) + (csr_qobj->csr->suffix-1)];
					if (locations != NULL){
						if (first == TRUE){
							first = FALSE;
							fprintf(fp, "%s:\n", filepath);
						}
						jcr.search_results_num += locations->len;
						for (int i = 0; i < locations->len; i++){
							concat_match++;
							location_t location = (g_array_index(locations, location_t, i));
							print_match(fp, csr_qobj->csr->keyword, csr_qobj_wrap->offset - csr_qobj->prev_prefix + location);
						}
					}
					#endif
				}

				#ifdef TINY
				if (csr_qobj->csr->suffix == keywords_len[csr_qobj->csr->keyword] - 1 && csr_qobj_wrap->prev_fp != NULL){
					tiny_access++;
					if (!tiny_prev_fetched){
						// fetch a tiny record from the tiny database
						tiny_fetch++;
						tiny_prev_fetched = true;
						DBT tiny_key, tiny_data;
						memset(&tiny_key, 0, sizeof(DBT));
						memset(&tiny_data, 0, sizeof(DBT));
						tiny_key.data = csr_qobj_wrap->prev_fp;
						tiny_key.size = sizeof(fingerprint);
						tiny_data.data = tiny_prev_value;
						tiny_data.ulen = tiny_value_size;
						tiny_data.flags = DB_DBT_USERMEM;
						int ret = tiny_dbp->get(tiny_dbp, NULL, &tiny_key, &tiny_data, 0);
						if (ret == DB_NOTFOUND)
							memset(tiny_prev_value, 0, tiny_value_size);
					}
					int arr_idx = csr_qobj->csr->keyword / TINY_KEYWORDS_PER_BYTE;
					int char_idx = csr_qobj->csr->keyword % TINY_KEYWORDS_PER_BYTE;
					bool prev_prefix = tiny_prev_value[arr_idx] & (1 << (1 + char_idx * 2));
					if (prev_prefix){
						if (first == TRUE){
							first = FALSE;
							fprintf(fp, "%s:\n", filepath);
						}
						jcr.search_results_num++;
						print_match(fp, csr_qobj->csr->keyword, csr_qobj_wrap->offset - 1);
						concat_match++;
						tiny_matches++;
					}
				}
				if (csr_qobj->csr->prefix == keywords_len[csr_qobj->csr->keyword] - 1 && csr_qobj_wrap->next_fp != NULL){
					tiny_access++;
					if (!tiny_next_fetched){
						// fetch a tiny record from the tiny database
						tiny_fetch++;
						tiny_next_fetched = true;
						DBT tiny_key, tiny_data;
						memset(&tiny_key, 0, sizeof(DBT));
						memset(&tiny_data, 0, sizeof(DBT));
						tiny_key.data = csr_qobj_wrap->next_fp;
						tiny_key.size = sizeof(fingerprint);
						tiny_data.data = tiny_next_value;
						tiny_data.ulen = tiny_value_size;
						tiny_data.flags = DB_DBT_USERMEM;
						int ret = tiny_dbp->get(tiny_dbp, NULL, &tiny_key, &tiny_data, 0);
						if (ret == DB_NOTFOUND)
							memset(tiny_next_value, 0, tiny_value_size);
					}
					int arr_idx = csr_qobj->csr->keyword / TINY_KEYWORDS_PER_BYTE;
					int char_idx = csr_qobj->csr->keyword % TINY_KEYWORDS_PER_BYTE;
					bool next_suffix = tiny_next_value[arr_idx] & (1 << (char_idx * 2));
					if (next_suffix){
						if (first == TRUE){
							first = FALSE;
							fprintf(fp, "%s:\n", filepath);
						}
						jcr.search_results_num++;
						print_match(fp, csr_qobj->csr->keyword, csr_qobj_wrap->offset + csr_qobj_wrap->size - (keywords_len[csr_qobj->csr->keyword] - 1));
						concat_match++;
						tiny_matches++;
					}
				}
				#endif

				if (csr_qobj->csr->exact_num > 0){
					if (first == TRUE){
						first = FALSE;
						fprintf(fp, "%s:\n", filepath);
					}
					jcr.search_results_num++;
					print_match(fp, csr_qobj->csr->keyword, csr_qobj_wrap->offset + csr_qobj->csr->first_exact);
					
					if(csr_qobj->csr->exact_num > 1){
						int j;
						if (locations_list_list == NULL){
							// fetch a location-list record
							locations_list_list = g_hash_table_lookup(locations_lists, csr_qobj_wrap->fp);
							list_i = 0;
						}
						locations_list = g_array_index(locations_list_list, GArray*, list_i);
						list_i++;
						if (csr_qobj->csr->exact_num == MAX_UINT_8){		
							j = 1;
							jcr.search_results_num += g_array_index(locations_list, location_t, 0) - 1;
						} else{
							j = 0;
							jcr.search_results_num += csr_qobj->csr->exact_num - 1;
						}
						for (; j < locations_list->len; j++){
							location_t location = (g_array_index(locations_list, location_t, j));
							print_match(fp, csr_qobj->csr->keyword, csr_qobj_wrap->offset + location); // list returns pointer, not value
						}
					}
				}
			}
			free(csr_qobj_wrap->fp);
			#ifdef TINY
			if (csr_qobj_wrap->prev_fp != NULL)
				free(csr_qobj_wrap->prev_fp);
			if (csr_qobj_wrap->next_fp != NULL)
				free(csr_qobj_wrap->next_fp);
			#endif
			g_array_free(csr_qobj_wrap->csr_qobj_list, TRUE);
		}

		TIMER_END(1, jcr.search_file_time);
		free(csr_qobj_wrap);
	}

	fclose(fp);
    jcr.status = JCR_STATUS_DONE;
    return NULL;
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

// Main DedupSearch function
void do_quick_search(int revision, sds *keywords_p, size_t keywords_num_p, sds tiny_path) {
	keywords_num = keywords_num_p;
	keywords = keywords_p;
	#ifdef TINY
	tiny_value_size = (keywords_num + TINY_KEYWORDS_PER_BYTE - 1) / TINY_KEYWORDS_PER_BYTE;
	#endif

	destor_log(DESTOR_NOTICE, "quick search");

	init_recipe_store();
	init_container_store();

	init_restore_jcr(revision, ".");

	destor_log(DESTOR_NOTICE, "job id: %d", jcr.id);
	destor_log(DESTOR_NOTICE, "backup path: %s", jcr.bv->path);
	destor_log(DESTOR_NOTICE, "%d strings to search", keywords_num);
	for (int i = 0; i < keywords_num; i++)
		printf("%s, ", keywords[i]);
	printf("\n");

	restore_container_queue = sync_queue_new(100);
	restore_recipe_queue = sync_queue_new(100);
	chunk_search_results_queue = sync_queue_new(100);

	TIMER_DECLARE(1);
	TIMER_BEGIN(1); // total_time

	// DedupSearchf start
	puts("==== search begin ====");
	
	// Exact matches database:
	// Chunk-result records initialization
	search_results = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, free, g_array_free_wrap);
	// Location-list records initialization
	locations_lists = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, free, g_array_free_wrap_ext);

	#ifdef TINY
	// Tiny substrings database:
	// tiny-result records initialization
    int ret = db_create(&tiny_dbp, NULL, 0);
    if (ret != 0) {
        printf("error creating db: %d - %s", errno, strerror(errno));
    }
    u_int32_t flags = DB_CREATE | DB_TRUNCATE;
    ret = tiny_dbp->open(tiny_dbp, NULL, tiny_path, NULL, DB_HASH, flags, 0);
    if (ret != 0) {
        printf("error opening db: %d - %s", errno, strerror(errno));
    }
	#endif

	puts("==== physical phase ====");
	TIMER_DECLARE(0);
	TIMER_BEGIN(0); // physical_phase_time

    jcr.status = JCR_STATUS_RUNNING;
	pthread_t init_t, read_t, search_t;
	
	init_search();
	
	pthread_create(&read_t, NULL, read_container_thread, NULL);
	pthread_create(&search_t, NULL, search_containers_data, NULL);


    do{
        sleep(5);
        fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed, %d search results, %.3f read chunk time\r", 
                jcr.data_size, jcr.chunk_num, jcr.file_num, jcr.search_results_num, jcr.read_chunk_time / 1000000);
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
    fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed, %d search results, %.3f read chunk time\n", 
        jcr.data_size, jcr.chunk_num, jcr.file_num, jcr.search_results_num, jcr.read_chunk_time / 1000000);

	assert(sync_queue_size(restore_container_queue) == 0);
	uint64_t search_results_count = g_hash_table_size(search_results);

	TIMER_END(0, jcr.physical_phase_time);
	puts("==== logical phase ====");
	TIMER_BEGIN(0);

    jcr.status = JCR_STATUS_RUNNING;
	pthread_t recipe_t, search_results_t, emit_t;
	pthread_create(&recipe_t, NULL, read_recipe_thread, NULL);
	pthread_create(&search_results_t, NULL, fp_to_search_results, NULL);
	pthread_create(&emit_t, NULL, search_chunks_results, NULL);

    do{
        sleep(5);
        fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed, %d search results\r", 
                jcr.data_size, jcr.chunk_num, jcr.file_num, jcr.search_results_num);
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
    fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed, %d search results\n", 
        jcr.data_size, jcr.chunk_num, jcr.file_num, jcr.search_results_num);

	assert(sync_queue_size(restore_recipe_queue) == 0);
	assert(sync_queue_size(chunk_search_results_queue) == 0);
	free_backup_version(jcr.bv);
	g_hash_table_destroy(search_results);

	#if (SEARCH_TYPE == FIND)
	free_concatenation_table();
	#endif
	
	TIMER_END(0, jcr.logical_phase_time);
	TIMER_END(1, jcr.total_time);
	
	int locations_lists_size = 0;
	GHashTableIter iter;
	gpointer key, value;
	GArray* locations_list;
	GArray* locations_list_list;
	int locations_count = 0;
	g_hash_table_iter_init(&iter, locations_lists);
	while(g_hash_table_iter_next(&iter, &key, &value)){
		locations_list_list = (GArray*)value;
		for (int i = 0; i < locations_list_list->len; i++){
			locations_list = g_array_index(locations_list_list, GArray*, i);
			locations_count += locations_list->len;
			locations_lists_size++;
		}
	}
	g_hash_table_destroy(locations_lists);
	
	#ifdef TINY
	if (tiny_dbp != NULL)
        tiny_dbp->close(tiny_dbp, 0);
	FILE *fp = fopen(tiny_path, "r");
	fseek(fp, 0L, SEEK_END);
   	actual_tiny_size = ftell(fp);
   	fseek(fp, 0L, SEEK_SET);
	fclose(fp);
    remove(tiny_path);
	calculated_tiny_size = tiny_objects * (sizeof(fingerprint) + tiny_value_size);
	#endif

	// DedupSearchf end
	puts("==== search end ====");

	printf("job id: %" PRId32 "\n", jcr.id);
	printf("strings to search: ");
	for (int i = 0; i < keywords_num; i++)
		printf("%s, ", keywords[i]);
	printf("\n");
	printf("number of search results: %" PRId32 "\n", jcr.search_results_num);
	printf("number of files: %" PRId32 "\n", jcr.file_num);
	printf("number of chunks: %" PRId32"\n", jcr.chunk_num);
	printf("total size(B): %" PRId64 "\n", jcr.data_size);
	printf("search results count: %" PRId64 "\n", search_results_count);
	printf("total time(s): %.3f\n", jcr.total_time / 1000000);
	printf("throughput(MB/s): %.2f\n",
			jcr.data_size * 1000000 / (1024.0 * 1024 * jcr.total_time));
	printf("speed factor: %.2f\n",
			jcr.data_size / (1024.0 * 1024 * jcr.read_container_num));

	printf("read_chunk_time : %.3fs, %.2fMB/s\n",
			jcr.read_chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_chunk_time / 1024 / 1024);
	printf("search_chunk_time : %.3fs, %.2fMB/s\n",
			jcr.search_chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.search_chunk_time / 1024 / 1024);
	printf("read_recipe_time : %.3fs, %.2fMB/s\n",
			jcr.read_recipe_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_recipe_time / 1024 / 1024);
	printf("fetch_results_time : %.3fs, %.2fMB/s\n",
			jcr.fetch_results_time / 1000000,
			jcr.data_size * 1000000 / jcr.fetch_results_time / 1024 / 1024);
	printf("search_file_time : %.3fs, %.2fMB/s\n",
			jcr.search_file_time / 1000000,
			jcr.data_size * 1000000 / jcr.search_file_time / 1024 / 1024);
	printf("locations_lists size: %d lists, %d locations, average %.2f locations in list\n",
			locations_lists_size, locations_count, locations_count/(float)locations_lists_size);
	printf("init_search_time: %.3fs\n",
			jcr.init_search_time / 1000000);
	printf("pre_suf_search_time: %.3fs\n",
			jcr.pre_suf_search_time / 1000000);
	printf("phyisical phase time: %.3fs, %.2fMB/s\n",
			jcr.physical_phase_time / 1000000,
			jcr.data_size * 1000000 / jcr.physical_phase_time / 1024 / 1024);
	printf("logical phase time: %.3fs, %.2fMB/s\n",
			jcr.logical_phase_time / 1000000,
			jcr.data_size * 1000000 / jcr.logical_phase_time / 1024 / 1024);
	printf("pre_suf: %d, prefix_1: %d, prefix_2: %d, suffix_1: %d, suffix_2: %d\n",
			pre_suf, prefix_1, prefix_2, suffix_1, suffix_2);
	printf("counter: %d\n", counter);
	printf("concatenation results: %d\n", concat_match);
	#ifdef TINY
	printf("actual tiny size (B): %lu\n", actual_tiny_size);
	printf("calculated tiny size (B): %lu\n", calculated_tiny_size);
	#endif
	
	save_results("quick", search_results_count, locations_lists_size, locations_count);

	close_container_store();
	close_recipe_store();
}
